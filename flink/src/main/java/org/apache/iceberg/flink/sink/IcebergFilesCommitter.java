/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergFilesCommitter extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<WriteResult, Void>, BoundedOneInput {

  private static final long serialVersionUID = 1L;
  private static final long INITIAL_CHECKPOINT_ID = -1L;
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];

  private static final Logger LOG = LoggerFactory.getLogger(IcebergFilesCommitter.class);
  private static final String FLINK_JOB_ID = "flink.job-id";

  // The max checkpoint id we've committed to iceberg table. As the flink's checkpoint is always increasing, so we could
  // correctly commit all the data files whose checkpoint id is greater than the max committed one to iceberg table, for
  // avoiding committing the same data files twice. This id will be attached to iceberg's meta when committing the
  // iceberg transaction.
  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";
  static final String MAX_CONTINUOUS_EMPTY_COMMITS = "flink.max-continuous-empty-commits";

  // TableLoader to load iceberg table lazily.
  private final TableLoader tableLoader;
  private final boolean replacePartitions;

  // A sorted map to maintain the completed data files for each pending checkpointId (which have not been committed
  // to iceberg table). We need a sorted map here because there's possible that few checkpoints snapshot failed, for
  // example: the 1st checkpoint have 2 data files <1, <file0, file1>>, the 2st checkpoint have 1 data files
  // <2, <file3>>. Snapshot for checkpoint#1 interrupted because of network/disk failure etc, while we don't expect
  // any data loss in iceberg table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit
  // iceberg table when the next checkpoint happen.

  // 使用treemap作为存储是为了利用其有序性，因为可能存在ck时写sn失败的情况，这样summary中就不会有MAX_COMMITTED_CHECKPOINT_ID的更新
  // 等待下一次cp的时候，再把cp之间差值的增量同步更新上去，把max刷到最新；如果flink cp失败重试，那也不会重复写入，因为需要判断是否大于目前元数据中的max
  // 其中存储的key是cpid，value是序列化后二进制的data数据
  private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will be flushed to the
  // 'dataFilesPerCheckpoint'.
  // 存储当前cpid对应结果的缓存，一旦对齐完成，将会把数据写入dataFilesPerCheckpoint中
  private final List<WriteResult> writeResultsOfCurrentCkpt = Lists.newArrayList();

  // It will have an unique identifier for one job.
  private transient String flinkJobId;
  private transient Table table;
  private transient ManifestOutputFileFactory manifestOutputFileFactory;
  private transient long maxCommittedCheckpointId;
  // 对连续cp时候totalFiles为0的数据进行记录+1
  private transient int continuousEmptyCheckpoints;
  // 允许最大的continuousEmptyCheckpoints次数，即达到该次数后，都要提交一次进行快照
  // 本质上是为了减少无效快照的生成 e.g 如只是max是10，那么即使连续9次都是empty，iceberg的sn也不会增加
  private transient int maxContinuousEmptyCommits;
  // There're two cases that we restore from flink checkpoints: the first case is restoring from snapshot created by the
  // same flink job; another case is restoring from snapshot created by another different job. For the second case, we
  // need to maintain the old flink job's id in flink state backend to find the max-committed-checkpoint-id when
  // traversing iceberg table's snapshots.

  // 两种方式进行从cp恢复； 一种是从原先的flink任务中恢复，另一种是从新的flink任务中重启，此时jobid是新的；所以需要获取到以前任务jobid下的最大消费的cpid
  private static final ListStateDescriptor<String> JOB_ID_DESCRIPTOR = new ListStateDescriptor<>(
      "iceberg-flink-job-id", BasicTypeInfo.STRING_TYPE_INFO);
  private transient ListState<String> jobIdState;
  // All pending checkpoints states for this function.
  private static final ListStateDescriptor<SortedMap<Long, byte[]>> STATE_DESCRIPTOR = buildStateDescriptor();
  private transient ListState<SortedMap<Long, byte[]>> checkpointsState;

  IcebergFilesCommitter(TableLoader tableLoader, boolean replacePartitions) {
    this.tableLoader = tableLoader;
    this.replacePartitions = replacePartitions;
  }

  /**
   * 一些初始化操作，主要是获取调用该committer的flink相关的信息，
   * 若有数据需要恢复消费情况，则需要将未提交的数据进行提交，注意，这里只提交一次，快照只有一次，但可能提交了多个cp的数据
   * 取决于数据落后的情况
   *
   * */
  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();

    // Open the table loader and load the table.
    // 加载catalog数据，有hivecatalog和hadoopcatalog，还有自定义的
    this.tableLoader.open();
    // 加载iceberg表
    this.table = tableLoader.loadTable();

    // 设置flink连续提交empty何时触发sn
    maxContinuousEmptyCommits = PropertyUtil.propertyAsInt(table.properties(), MAX_CONTINUOUS_EMPTY_COMMITS, 10);
    Preconditions.checkArgument(maxContinuousEmptyCommits > 0,
        MAX_CONTINUOUS_EMPTY_COMMITS + " must be positive");

    // 类似于spark，创建tmp目录，然后最后提交再把tmp目录中的数据移到正式目录并删除临时目录文件夹
    int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    int attemptId = getRuntimeContext().getAttemptNumber();
    this.manifestOutputFileFactory = FlinkManifestUtil.createOutputFileFactory(table, flinkJobId, subTaskId, attemptId);
    this.maxCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    // cp状态存储，jobid状态存储(考虑到可能换任务用新jobid)
    this.checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
    this.jobIdState = context.getOperatorStateStore().getListState(JOB_ID_DESCRIPTOR);

    // 如果任务是有状态的(有中间计算结果)，那么需要搞定状态恢复的问题
    if (context.isRestored()) {
      // 轮询获取jobid
      String restoredFlinkJobId = jobIdState.get().iterator().next();
      Preconditions.checkState(!Strings.isNullOrEmpty(restoredFlinkJobId),
          "Flink job id parsed from checkpoint snapshot shouldn't be null or empty");

      // Since flink's checkpoint id will start from the max-committed-checkpoint-id + 1 in the new flink job even if
      // it's restored from a snapshot created by another different flink job, so it's safe to assign the max committed
      // checkpoint id from restored flink job to the current flink job.
      // 从有状态的一个flink任务恢复到另一个flink任务(jobid不一样), 需要先获取以前那个flink任务下最大的cpid
      // 因为iceberg会根据cpid进行判断，只有大于cpid的才会进行snapshot
      this.maxCommittedCheckpointId = getMaxCommittedCheckpointId(table, restoredFlinkJobId);

      // 恢复时拿到不小于maxCommittedCheckpointId值的一系列数据 e.g <cp1, <data1,data2>>, <cp2, <data3>>
      // 还未提交的片段(flink cp作为iceberg sn，cp之间的一段即为片段)
      NavigableMap<Long, byte[]> uncommittedDataFiles = Maps
          .newTreeMap(checkpointsState.get().iterator().next())
          .tailMap(maxCommittedCheckpointId, false);
      // 如果有未提交的片段，那么初始化时候先提交一波
      if (!uncommittedDataFiles.isEmpty()) {
        // Committed all uncommitted data files from the old flink job to iceberg table.
        // 拿到最大的未提交的cpid当做记录在summary中的flink.max-committed-checkpoint-id
        // 因为可能存在很多未提交的数据，有很多cpid及对应的数据，
        // 在初始化过程中对于未提交的数据进行恢复时，将通过一次快照把这些数据都写到iceberg中
        long maxUncommittedCheckpointId = uncommittedDataFiles.lastKey();
        commitUpToCheckpoint(uncommittedDataFiles, restoredFlinkJobId, maxUncommittedCheckpointId);
      }
    }
  }

  /**
   *  将iceberg快照的状态写入状态后端
   *
   * */
  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    long checkpointId = context.getCheckpointId();

    LOG.info("Start to flush snapshot state to state backend, table: {}, checkpointId: {}", table, checkpointId);

    // Update the checkpoint state.
    dataFilesPerCheckpoint.put(checkpointId, writeToManifest(checkpointId));
    // Reset the snapshot state to the latest state.
    checkpointsState.clear();
    checkpointsState.add(dataFilesPerCheckpoint);

    jobIdState.clear();
    jobIdState.add(flinkJobId);

    // Clear the local buffer for current checkpoint.
    writeResultsOfCurrentCkpt.clear();
  }

  /**
   * 核心
   * checkpoint完成并提交时候调用，相当于flink做cp时候，iceberg做sn
   *
   * */
  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // 将cpid写到状态后端
    super.notifyCheckpointComplete(checkpointId);
    // It's possible that we have the following events:
    //   1. snapshotState(ckpId);
    //   2. snapshotState(ckpId+1);
    //   3. notifyCheckpointComplete(ckpId+1);
    //   4. notifyCheckpointComplete(ckpId);
    // For step#4, we don't need to commit iceberg table again because in step#3 we've committed all the files,
    // Besides, we need to maintain the max-committed-checkpoint-id to be increasing.

    // 如果发现新的cpid > summary中存储的cpid，则进行提交，否则不提交，这样就保证了提交不重复
    if (checkpointId > maxCommittedCheckpointId) {
      commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, checkpointId);
      this.maxCommittedCheckpointId = checkpointId;
    }
  }

  /**
   * 提交部分核心方法
   * 由checkpoint触发的commit提交
   *
   * 1. 组装需要提交的元数据
   * 3. 根据是否覆盖分区进行不同方式提交，本质上最终调用commitOperation
   * 4. 删除flink manifest存放的临时路径
   *
   */
  private void commitUpToCheckpoint(NavigableMap<Long, byte[]> deltaManifestsMap,
                                    String newFlinkJobId,
                                    long checkpointId) throws IOException {
    NavigableMap<Long, byte[]> pendingMap = deltaManifestsMap.headMap(checkpointId, true);
    // manifests其实本质上就是snap-id代表的文件里的内容,内容是n个ManifestFile对象构成的
    List<ManifestFile> manifests = Lists.newArrayList();
    // WriteResult实际存储了metadata中.avro的文件内容，包含了实际存储数据中的一些统计信息，包括分区统计等
    NavigableMap<Long, WriteResult> pendingResults = Maps.newTreeMap();
    for (Map.Entry<Long, byte[]> e : pendingMap.entrySet()) {
      if (Arrays.equals(EMPTY_MANIFEST_DATA, e.getValue())) {
        // Skip the empty flink manifest.
        continue;
      }

      DeltaManifests deltaManifests = SimpleVersionedSerialization
          .readVersionAndDeSerialize(DeltaManifestsSerializer.INSTANCE, e.getValue());
      // 存储该cpid下，存储数据相关的一些信息，
      pendingResults.put(e.getKey(), FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io()));
      manifests.addAll(deltaManifests.manifests());
    }

    // 开始提交
    int totalFiles = pendingResults.values().stream()
        .mapToInt(r -> r.dataFiles().length + r.deleteFiles().length).sum();
    continuousEmptyCheckpoints = totalFiles == 0 ? continuousEmptyCheckpoints + 1 : 0;
    // 提交的条件： 符合非空 或者 达到连续空积累到最大连续空提交条件。
    // 即被整除，如连续空为9次，但最大连续空commit限定是10，则仍然不提交
    if (totalFiles != 0 || continuousEmptyCheckpoints % maxContinuousEmptyCommits == 0) {
      if (replacePartitions) {
        // 先删除分区，然后再提交，所谓覆盖
        replacePartitions(pendingResults, newFlinkJobId, checkpointId);
      } else {
        // 一般方式提交
        commitDeltaTxn(pendingResults, newFlinkJobId, checkpointId);
      }
      continuousEmptyCheckpoints = 0;
    }
    pendingMap.clear();

    // Delete the committed manifests.
    // 将临时的flink manifest文件删除
    for (ManifestFile manifest : manifests) {
      try {
        table.io().deleteFile(manifest.path());
      } catch (Exception e) {
        // The flink manifests cleaning failure shouldn't abort the completed checkpoint.
        String details = MoreObjects.toStringHelper(this)
            .add("flinkJobId", newFlinkJobId)
            .add("checkpointId", checkpointId)
            .add("manifestPath", manifest.path())
            .toString();
        LOG.warn("The iceberg transaction has been committed, but we failed to clean the temporary flink manifests: {}",
            details, e);
      }
    }
  }

  private void replacePartitions(NavigableMap<Long, WriteResult> pendingResults, String newFlinkJobId,
                                 long checkpointId) {
    // Partition overwrite does not support delete files.
    int deleteFilesNum = pendingResults.values().stream().mapToInt(r -> r.deleteFiles().length).sum();
    Preconditions.checkState(deleteFilesNum == 0, "Cannot overwrite partitions with delete files.");

    // Commit the overwrite transaction.
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions();

    int numFiles = 0;
    for (WriteResult result : pendingResults.values()) {
      Preconditions.checkState(result.referencedDataFiles().length == 0, "Should have no referenced data files.");

      numFiles += result.dataFiles().length;
      Arrays.stream(result.dataFiles()).forEach(dynamicOverwrite::addFile);
    }

    commitOperation(dynamicOverwrite, numFiles, 0, "dynamic partition overwrite", newFlinkJobId, checkpointId);
  }

  // 提交元数据
  private void commitDeltaTxn(NavigableMap<Long, WriteResult> pendingResults, String newFlinkJobId, long checkpointId) {
    int deleteFilesNum = pendingResults.values().stream().mapToInt(r -> r.deleteFiles().length).sum();

    if (deleteFilesNum == 0) {
      // To be compatible with iceberg format V1.
      // 一系列datafile结构的数据
      AppendFiles appendFiles = table.newAppend();

      int numFiles = 0;
      for (WriteResult result : pendingResults.values()) {
        Preconditions.checkState(result.referencedDataFiles().length == 0, "Should have no referenced data files.");

        numFiles += result.dataFiles().length;
        Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
      }
      // 提交文件内容
      commitOperation(appendFiles, numFiles, 0, "append", newFlinkJobId, checkpointId);
    } else {
      // To be compatible with iceberg format V2.
      for (Map.Entry<Long, WriteResult> e : pendingResults.entrySet()) {
        // We don't commit the merged result into a single transaction because for the sequential transaction txn1 and
        // txn2, the equality-delete files of txn2 are required to be applied to data files from txn1. Committing the
        // merged one will lead to the incorrect delete semantic.
        WriteResult result = e.getValue();

        // Row delta validations are not needed for streaming changes that write equality deletes. Equality deletes
        // are applied to data in all previous sequence numbers, so retries may push deletes further in the future,
        // but do not affect correctness. Position deletes committed to the table in this path are used only to delete
        // rows from data files that are being added in this commit. There is no way for data files added along with
        // the delete files to be concurrently removed, so there is no need to validate the files referenced by the
        // position delete files that are being committed.
        RowDelta rowDelta = table.newRowDelta();

        int numDataFiles = result.dataFiles().length;
        Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);

        int numDeleteFiles = result.deleteFiles().length;
        Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);

        commitOperation(rowDelta, numDataFiles, numDeleteFiles, "rowDelta", newFlinkJobId, e.getKey());
      }
    }
  }

  /**
   * 核心
   * 提交元数据操作
   * */
  private void commitOperation(SnapshotUpdate<?> operation, int numDataFiles, int numDeleteFiles, String description,
                               String newFlinkJobId, long checkpointId) {
    LOG.info("Committing {} with {} data files and {} delete files to table {}", description, numDataFiles,
        numDeleteFiles, table);
    // 将 flink.max-committed-checkpoint-id，flink.job-id 写入 summary
    operation.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
    operation.set(FLINK_JOB_ID, newFlinkJobId);

    long start = System.currentTimeMillis();
    operation.commit(); // abort is automatically called if this fails.
    long duration = System.currentTimeMillis() - start;
    LOG.info("Committed in {} ms", duration);
  }

  /**
   * todo 不断向缓存中写对应的数据，如cp1已过，而未到cp2期间，就会把数据缓存起来？
   * */
  @Override
  public void processElement(StreamRecord<WriteResult> element) {
    this.writeResultsOfCurrentCkpt.add(element.getValue());
  }

  /**
   * 后续没有数据再会到达时处理
   * summary中flink.max-committed-checkpoint-id直接拉到顶
   *
   */
  @Override
  public void endInput() throws IOException {
    // Flush the buffered data files into 'dataFilesPerCheckpoint' firstly.
    long currentCheckpointId = Long.MAX_VALUE;
    dataFilesPerCheckpoint.put(currentCheckpointId, writeToManifest(currentCheckpointId));
    writeResultsOfCurrentCkpt.clear();

    commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, currentCheckpointId);
  }

  /**
   * Write all the complete data files to a newly created manifest file and return the manifest's avro serialized bytes.
   */
  private byte[] writeToManifest(long checkpointId) throws IOException {
    if (writeResultsOfCurrentCkpt.isEmpty()) {
      return EMPTY_MANIFEST_DATA;
    }

    WriteResult result = WriteResult.builder().addAll(writeResultsOfCurrentCkpt).build();
    DeltaManifests deltaManifests = FlinkManifestUtil.writeCompletedFiles(result,
        () -> manifestOutputFileFactory.create(checkpointId), table.spec());

    return SimpleVersionedSerialization.writeVersionAndSerialize(DeltaManifestsSerializer.INSTANCE, deltaManifests);
  }

  @Override
  public void dispose() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }
  }

  private static ListStateDescriptor<SortedMap<Long, byte[]>> buildStateDescriptor() {
    Comparator<Long> longComparator = Comparators.forType(Types.LongType.get());
    // Construct a SortedMapTypeInfo.
    SortedMapTypeInfo<Long, byte[]> sortedMapTypeInfo = new SortedMapTypeInfo<>(
        BasicTypeInfo.LONG_TYPE_INFO, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, longComparator
    );
    return new ListStateDescriptor<>("iceberg-files-committer-state", sortedMapTypeInfo);
  }

  static long getMaxCommittedCheckpointId(Table table, String flinkJobId) {
    Snapshot snapshot = table.currentSnapshot();
    long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String snapshotFlinkJobId = summary.get(FLINK_JOB_ID);
      if (flinkJobId.equals(snapshotFlinkJobId)) {
        String value = summary.get(MAX_COMMITTED_CHECKPOINT_ID);
        if (value != null) {
          lastCommittedCheckpointId = Long.parseLong(value);
          break;
        }
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }

    return lastCommittedCheckpointId;
  }
}
