/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * SplitEnumerator可以将分片分配到SourceReader从而响应各种事件，包括发现新的分片，新SourceReader的注册，SourceReader的失败处理等
 * SplitEnumerator组件在JobManager上以单并行度运行，负责对未分配的分片进行维护，并以均衡的方式将其分配给reader
 * The enumerator class for Kafka source.
 */
@Internal
public class KafkaSourceEnumerator
        implements SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceEnumerator.class);
    private final KafkaSubscriber subscriber;
    // 传入的是ReaderHandledOffsetsInitializer
    private final OffsetsInitializer startingOffsetInitializer;
    private final OffsetsInitializer stoppingOffsetInitializer;
    private final Properties properties;
    private final long partitionDiscoveryIntervalMs;
    private final SplitEnumeratorContext<KafkaPartitionSplit> context;
    private final Boundedness boundedness;

    /** Partitions that have been assigned to readers. */
    private final Set<TopicPartition> assignedPartitions;

    /**
     * The discovered and initialized partition splits that are waiting for owner reader to be ready.
     * <p>
     * 对已发现并初始化的分区按照并发度预分配，等待所属的SubTask上的读取器来完成分配
     */
    private final Map<Integer, Set<KafkaPartitionSplit>> pendingPartitionSplitAssignment;

    /** The consumer group id used for this KafkaSource. */
    private final String consumerGroupId;

    // Lazily instantiated or mutable fields.
    // 用于操作和管理Kafka集群的客户端工具，在调用start方法时被初始化
    private AdminClient adminClient;

    // This flag will be marked as true if periodically partition discovery is disabled AND the
    // initializing partition discovery has finished.
    private boolean noMoreNewPartitionSplits = false;

    public KafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            Boundedness boundedness) {
        // SplitEnumerator组件在JobManager上以单并行度运行，负责对未分配的分片进行维护，并以均衡的方式将其分配给reader
        this(
                subscriber,
                startingOffsetInitializer,
                stoppingOffsetInitializer,
                properties,
                context,
                boundedness,
                Collections.emptySet());
    }

    // SplitEnumerator组件在JobManager上以单并行度运行，负责对未分配的分片进行维护，并以均衡的方式将其分配给reader
    public KafkaSourceEnumerator(
            KafkaSubscriber subscriber,
            OffsetsInitializer startingOffsetInitializer,
            OffsetsInitializer stoppingOffsetInitializer,
            Properties properties,
            SplitEnumeratorContext<KafkaPartitionSplit> context,
            Boundedness boundedness,
            Set<TopicPartition> assignedPartitions) {
        // 默认传入的是TopicListSubscriber
        this.subscriber = subscriber;
        this.startingOffsetInitializer = startingOffsetInitializer;
        this.stoppingOffsetInitializer = stoppingOffsetInitializer;
        this.properties = properties;
        this.context = context;
        this.boundedness = boundedness;
        // 固定传入的是一个空集合
        this.assignedPartitions = new HashSet<>(assignedPartitions);
        this.pendingPartitionSplitAssignment = new HashMap<>();
        // 检查一次新分区的时间间隔
        this.partitionDiscoveryIntervalMs = KafkaSourceOptions.getOption(
                properties,
                KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS,
                Long::parseLong);
        // 消费者组ID
        this.consumerGroupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
    }

    /**
     * Start the enumerator.
     *
     * <p>Depending on {@link #partitionDiscoveryIntervalMs}, the enumerator will trigger a one-time
     * partition discovery, or schedule a callable for discover partitions periodically.
     *
     * <p>The invoking chain of partition discovery would be:
     *
     * <ol>
     *   <li>{@link #getSubscribedTopicPartitions} in worker thread
     *   <li>{@link #checkPartitionChanges} in coordinator thread
     *   <li>{@link #initializePartitionSplits} in worker thread
     *   <li>{@link #handlePartitionSplitChanges} in coordinator thread
     * </ol>
     */
    @Override
    public void start() {
        // SplitEnumerator组件在JobManager上以单并行度运行，负责对未分配的分片进行维护，并以均衡的方式将其分配给reader
        adminClient = getKafkaAdminClient();
        // 如果设置了partition.discovery.interval.ms参数，且大于0，则会周期性的执行
        if (partitionDiscoveryIntervalMs > 0) {
            LOG.info(
                    "Starting the KafkaSourceEnumerator for consumer group {} "
                            + "with partition discovery interval of {} ms.",
                    consumerGroupId, partitionDiscoveryIntervalMs);
            context.callAsync(
                    // 获取订阅的Topic分区列表，放到单独的线程中执行
                    this::getSubscribedTopicPartitions,
                    // 将上面getSubscribedTopicPartitions方法的结果作为参数传入到checkPartitionChanges方法中
                    this::checkPartitionChanges,
                    // 周期任务初始延迟
                    0,
                    // 周期任务间隔
                    partitionDiscoveryIntervalMs);
        } else {
            LOG.info(
                    "Starting the KafkaSourceEnumerator for consumer group {} without periodic partition discovery.",
                    consumerGroupId);
            context.callAsync(this::getSubscribedTopicPartitions, this::checkPartitionChanges);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the kafka source pushes splits eagerly, rather than act upon split requests
    }

    @Override
    public void addSplitsBack(List<KafkaPartitionSplit> splits, int subtaskId) {
        // SourceReader失败时会调用addSplitsBack()方法。SplitEnumerator应当收回已经被分配，
        // 但尚未被该SourceReader确认（acknowledged）的分片
        addPartitionSplitChangeToPendingAssignments(splits);

        // If the failed subtask has already restarted, we need to assign pending splits to it
        if (context.registeredReaders().containsKey(subtaskId)) {
            assignPendingPartitionSplits(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug(
                "Adding reader {} to KafkaSourceEnumerator for consumer group {}.",
                subtaskId, consumerGroupId);
        // SubTask启动的时候会调用具体的Opterator的Open方法，在open方法中会向JobMaster注册reader，最终会调用该方法
        assignPendingPartitionSplits(Collections.singleton(subtaskId));
    }

    @Override
    public KafkaSourceEnumState snapshotState(long checkpointId) throws Exception {
        return new KafkaSourceEnumState(assignedPartitions);
    }

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    // ----------------- private methods -------------------

    /**
     * List subscribed topic partitions on Kafka brokers.
     *
     * <p>NOTE: This method should only be invoked in the worker executor thread, because it
     * requires network I/O with Kafka brokers.
     *
     * @return Set of subscribed {@link TopicPartition}s
     */
    private Set<TopicPartition> getSubscribedTopicPartitions() {
        // subscriber默认是TopicListSubscriber，获取所有的TopicPartition
        return subscriber.getSubscribedTopicPartitions(adminClient);
    }

    /**
     * Check if there's any partition changes within subscribed topic partitions fetched by worker
     * thread, and invoke {@link KafkaSourceEnumerator#initializePartitionSplits(PartitionChange)}
     * in worker thread to initialize splits for new partitions.
     *
     * <p>NOTE: This method should only be invoked in the coordinator executor thread.
     *
     * @param fetchedPartitions Map from topic name to its description
     * @param t Exception in worker thread
     */
    private void checkPartitionChanges(Set<TopicPartition> fetchedPartitions, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException(
                    "Failed to list subscribed topic partitions due to ", t);
        }
        // 获取分区变化，其实就是与之前的分区列表做对比，得到新增的分区列表，和删除的分区列表
        final PartitionChange partitionChange = getPartitionChange(fetchedPartitions);
        // 如果没有分区变化，直接返回
        if (partitionChange.isEmpty()) {
            return;
        }
        // 调用SourceCoordinatorContext的callAsync
        context.callAsync(
                // 这里其实就是将每个新增的分区以及其起始消费offset和stopOffset封装到KafkaPartitionSplit中
                // 然后再将新增的KafkaPartitionSplit列表和移除的分区列表封装到PartitionSplitChange中
                () -> initializePartitionSplits(partitionChange),
                this::handlePartitionSplitChanges);
    }

    /**
     * Initialize splits for newly discovered partitions.
     *
     * <p>Enumerator will be responsible for fetching offsets when initializing splits if:
     *
     * <ul>
     *   <li>using timestamp for initializing offset
     *   <li>or using specified offset, but the offset is not provided for the newly discovered
     *       partitions
     * </ul>
     *
     * <p>Otherwise offsets will be initialized by readers.
     *
     * <p>NOTE: This method should only be invoked in the worker executor thread, because it
     * potentially requires network I/O with Kafka brokers for fetching offsets.
     *
     * @param partitionChange Newly discovered and removed partitions
     *
     * @return {@link KafkaPartitionSplit} of new partitions and {@link TopicPartition} of removed
     *         partitions
     */
    private PartitionSplitChange initializePartitionSplits(PartitionChange partitionChange) {
        // 获取新增的partition分区
        Set<TopicPartition> newPartitions =
                Collections.unmodifiableSet(partitionChange.getNewPartitions());
        // 这里其实就是new一个PartitionOffsetsRetrieverImpl
        OffsetsInitializer.PartitionOffsetsRetriever offsetsRetriever = getOffsetsRetriever();

        // 调用ReaderHandledOffsetsInitializer的getPartitionOffsets方法
        // 这里其实是为每个新增的分区设置起始offset
        Map<TopicPartition, Long> startingOffsets =
                startingOffsetInitializer.getPartitionOffsets(newPartitions, offsetsRetriever);
        // 一般是NoStoppingOffsetsInitializer，所以一般返回空列表
        Map<TopicPartition, Long> stoppingOffsets =
                stoppingOffsetInitializer.getPartitionOffsets(newPartitions, offsetsRetriever);

        // 这里其实就是将每一个新增的分区以及其起始消费offset和stopOffset封装到KafkaPartitionSplit中
        Set<KafkaPartitionSplit> partitionSplits = new HashSet<>(newPartitions.size());
        for (TopicPartition tp : newPartitions) {
            Long startingOffset = startingOffsets.get(tp);
            // 如果不存在返回Long.MIN_VALUE，流处理的情况一般是不存在的
            long stoppingOffset =
                    stoppingOffsets.getOrDefault(tp, KafkaPartitionSplit.NO_STOPPING_OFFSET);
            partitionSplits.add(new KafkaPartitionSplit(tp, startingOffset, stoppingOffset));
        }
        return new PartitionSplitChange(partitionSplits, partitionChange.getRemovedPartitions());
    }

    /**
     * Mark partition splits initialized by {@link
     * KafkaSourceEnumerator#initializePartitionSplits(PartitionChange)} as pending and try to
     * assign pending splits to registered readers.
     *
     * <p>NOTE: This method should only be invoked in the coordinator executor thread.
     *
     * @param partitionSplitChange Partition split changes
     * @param t Exception in worker thread
     */
    private void handlePartitionSplitChanges(
            PartitionSplitChange partitionSplitChange,
            Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to initialize partition splits due to ", t);
        }
        if (partitionDiscoveryIntervalMs <= 0) {
            LOG.debug("Partition discovery is disabled.");
            noMoreNewPartitionSplits = true;
        }
        // TODO: Handle removed partitions.
        // 根据Topic的hashcode计算了一个启始偏移，然后加上partitionIndex同numReaders取余数
        // 给每一个并发度index，分批一个partition列表，存储在pendingPartitionSplitAssignment中
        addPartitionSplitChangeToPendingAssignments(partitionSplitChange.newPartitionSplits);
        assignPendingPartitionSplits(context.registeredReaders().keySet());
    }

    // This method should only be invoked in the coordinator executor thread.
    private void addPartitionSplitChangeToPendingAssignments(Collection<KafkaPartitionSplit> newPartitionSplits) {
        // 获取当前并发度
        int numReaders = context.currentParallelism();
        for (KafkaPartitionSplit split : newPartitionSplits) {
            // 根据Topic的hashcode计算了一个启始偏移，然后加上partitionIndex同numReaders取余数
            int ownerReader = getSplitOwner(split.getTopicPartition(), numReaders);
            // 这里其实就是给每一个并发度index，分批一个partition列表
            pendingPartitionSplitAssignment.computeIfAbsent(
                    ownerReader, r -> new HashSet<>()).add(split);
        }
        LOG.debug(
                "Assigned {} to {} readers of consumer group {}.",
                newPartitionSplits,
                numReaders,
                consumerGroupId);
    }

    // This method should only be invoked in the coordinator executor thread.
    private void assignPendingPartitionSplits(Set<Integer> pendingReaders) {
        Map<Integer, List<KafkaPartitionSplit>> incrementalAssignment = new HashMap<>();

        // Check if there's any pending splits for given readers
        // 这里的pendingReader其实就是subtaskId
        for (int pendingReader : pendingReaders) {
            // 做一个校验，如果subtaskId不包含在SourceCoordinatorContext中的registeredReaders就抛出异常
            checkReaderRegistered(pendingReader);
            // Remove pending assignment for the reader
            // 从pendingPartitionSplitAssignment将当前的subtaskId移除
            final Set<KafkaPartitionSplit> pendingAssignmentForReader =
                    pendingPartitionSplitAssignment.remove(pendingReader);

            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                // Put pending assignment into incremental assignment
                incrementalAssignment
                        .computeIfAbsent(pendingReader, (ignored) -> new ArrayList<>())
                        .addAll(pendingAssignmentForReader);

                // Mark pending partitions as already assigned
                // 将待处理的分区标记为已分配
                pendingAssignmentForReader.forEach(
                        split -> assignedPartitions.add(split.getTopicPartition()));
            }
        }

        // Assign pending splits to readers
        if (!incrementalAssignment.isEmpty()) {
            LOG.info("Assigning splits to readers {}", incrementalAssignment);
            // 调用SourceCoordinatorContext的assignSplits方法
            // 调用具体的TaskExecutor的sendOperatorEventToTask方法，将AddSplitEvent发送给具体的StreamTask
            context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
        }

        // If periodically partition discovery is disabled and the initializing discovery has done,
        // signal NoMoreSplitsEvent to pending readers
        // 批处理逻辑
        if (noMoreNewPartitionSplits && boundedness == Boundedness.BOUNDED) {
            LOG.debug(
                    "No more KafkaPartitionSplits to assign. Sending NoMoreSplitsEvent to reader {}"
                            + " in consumer group {}.",
                    pendingReaders,
                    consumerGroupId);
            pendingReaders.forEach(context::signalNoMoreSplits);
        }
    }

    private void checkReaderRegistered(int readerId) {
        if (!context.registeredReaders().containsKey(readerId)) {
            throw new IllegalStateException(
                    String.format("Reader %d is not registered to source coordinator", readerId));
        }
    }

    @VisibleForTesting
    PartitionChange getPartitionChange(Set<TopicPartition> fetchedPartitions) {
        // 已经被移除的分区列表
        final Set<TopicPartition> removedPartitions = new HashSet<>();
        // fetchedPartitions是当前全量的的分区列表
        Consumer<TopicPartition> dedupOrMarkAsRemoved = (tp) -> {
            // 将传入的TopicPartition从最新拉取到的分区列表中删除，如果删除成功，表示分区未发生变化
            if (!fetchedPartitions.remove(tp)) {
                // 如果删除失败，说明新拉取到的分区不包含该分区，说明该分区已被删除
                removedPartitions.add(tp);
            }
        };
        // assignedPartitions表示已经被分配给Reader的分区，从fetchedPartitions中移除已经被分配的分区
        assignedPartitions.forEach(dedupOrMarkAsRemoved);
        // 从fetchedPartitions中移除等待被分配的分区，即移除pendingPartitionSplitAssignment中已经包含的分区
        pendingPartitionSplitAssignment.forEach((reader, splits) ->
                splits.forEach(split -> dedupOrMarkAsRemoved.accept(split.getTopicPartition())));

        // 如果fetchedPartitions中还有分区，说明是新发现的分区
        if (!fetchedPartitions.isEmpty()) {
            LOG.info("Discovered new partitions: {}", fetchedPartitions);
        }
        // 如果removedPartitions不为空，说明有分区被移除
        if (!removedPartitions.isEmpty()) {
            LOG.info("Discovered removed partitions: {}", removedPartitions);
        }

        return new PartitionChange(fetchedPartitions, removedPartitions);
    }

    private AdminClient getKafkaAdminClient() {
        Properties adminClientProps = new Properties();
        deepCopyProperties(properties, adminClientProps);
        // set client id prefix
        String clientIdPrefix =
                adminClientProps.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key());
        adminClientProps.setProperty(
                ConsumerConfig.CLIENT_ID_CONFIG, clientIdPrefix + "-enumerator-admin-client");
        return AdminClient.create(adminClientProps);
    }

    private OffsetsInitializer.PartitionOffsetsRetriever getOffsetsRetriever() {
        String groupId = properties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
        return new PartitionOffsetsRetrieverImpl(adminClient, groupId);
    }

    /**
     * Returns the index of the target subtask that a specific Kafka partition should be assigned
     * to.
     *
     * <p>The resulting distribution of partitions of a single topic has the following contract:
     *
     * <ul>
     *   <li>1. Uniformly distributed across subtasks
     *   <li>2. Partitions are round-robin distributed (strictly clockwise w.r.t. ascending subtask
     *       indices) by using the partition id as the offset from a starting index (i.e., the index
     *       of the subtask which partition 0 of the topic will be assigned to, determined using the
     *       topic name).
     * </ul>
     *
     * @param tp the Kafka partition to assign.
     * @param numReaders the total number of readers.
     *
     * @return the id of the subtask that owns the split.
     */
    @VisibleForTesting
    static int getSplitOwner(TopicPartition tp, int numReaders) {
        int startIndex = ((tp.topic().hashCode() * 31) & 0x7FFFFFFF) % numReaders;

        // here, the assumption is that the id of Kafka partitions are always ascending
        // starting from 0, and therefore can be used directly as the offset clockwise from the
        // start index
        return (startIndex + tp.partition()) % numReaders;
    }

    @VisibleForTesting
    static void deepCopyProperties(Properties from, Properties to) {
        for (String key : from.stringPropertyNames()) {
            to.setProperty(key, from.getProperty(key));
        }
    }

    // --------------- private class ---------------

    /** A container class to hold the newly added partitions and removed partitions. */
    @VisibleForTesting
    static class PartitionChange {
        // 新增的分区
        private final Set<TopicPartition> newPartitions;
        // 删除的分区
        private final Set<TopicPartition> removedPartitions;

        PartitionChange(Set<TopicPartition> newPartitions, Set<TopicPartition> removedPartitions) {
            this.newPartitions = newPartitions;
            this.removedPartitions = removedPartitions;
        }

        public Set<TopicPartition> getNewPartitions() {
            return newPartitions;
        }

        public Set<TopicPartition> getRemovedPartitions() {
            return removedPartitions;
        }

        public boolean isEmpty() {
            return newPartitions.isEmpty() && removedPartitions.isEmpty();
        }
    }

    private static class PartitionSplitChange {
        // 新增的分区列表
        private final Set<KafkaPartitionSplit> newPartitionSplits;
        // 删除的分区列表
        private final Set<TopicPartition> removedPartitions;

        private PartitionSplitChange(
                Set<KafkaPartitionSplit> newPartitionSplits,
                Set<TopicPartition> removedPartitions) {
            this.newPartitionSplits = Collections.unmodifiableSet(newPartitionSplits);
            this.removedPartitions = Collections.unmodifiableSet(removedPartitions);
        }
    }

    /** The implementation for offsets retriever with a consumer and an admin client. */
    @VisibleForTesting
    public static class PartitionOffsetsRetrieverImpl
            implements OffsetsInitializer.PartitionOffsetsRetriever, AutoCloseable {
        private final AdminClient adminClient;
        private final String groupId;

        public PartitionOffsetsRetrieverImpl(AdminClient adminClient, String groupId) {
            this.adminClient = adminClient;
            this.groupId = groupId;
        }

        @Override
        public Map<TopicPartition, Long> committedOffsets(Collection<TopicPartition> partitions) {
            ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions()
                    .topicPartitions(new ArrayList<>(partitions));
            try {
                return adminClient.listConsumerGroupOffsets(groupId, options)
                        .partitionsToOffsetAndMetadata()
                        .thenApply(result -> {
                            Map<TopicPartition, Long> offsets = new HashMap<>();
                            result.forEach((tp, oam) -> {
                                if (oam != null) {
                                    offsets.put(tp, oam.offset());
                                }
                            });
                            return offsets;
                        }).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException(
                        "Interrupted while listing offsets for consumer group " + groupId, e);
            } catch (ExecutionException e) {
                throw new FlinkRuntimeException(
                        "Failed to fetch committed offsets for consumer group " + groupId
                                + " due to", e);
            }
        }

        /**
         * List offsets for the specified partitions and OffsetSpec. This operation enables to find
         * the beginning offset, end offset as well as the offset matching a timestamp in
         * partitions.
         *
         * @param topicPartitionOffsets The mapping from partition to the OffsetSpec to look up.
         *
         * @return The list offsets result.
         *
         * @see KafkaAdminClient#listOffsets(Map)
         */
        private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listOffsets(
                Map<TopicPartition, OffsetSpec> topicPartitionOffsets) {
            try {
                return adminClient.listOffsets(topicPartitionOffsets).all().thenApply(result -> {
                            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
                                    new HashMap<>();
                            result.forEach(
                                    (tp, listOffsetsResultInfo) -> {
                                        if (listOffsetsResultInfo != null) {
                                            offsets.put(tp, listOffsetsResultInfo);
                                        }
                                    });
                            return offsets;
                        })
                        .get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FlinkRuntimeException(
                        "Interrupted while listing offsets for topic partitions: "
                                + topicPartitionOffsets, e);
            } catch (ExecutionException e) {
                throw new FlinkRuntimeException(
                        "Failed to list offsets for topic partitions: "
                                + topicPartitionOffsets + " due to", e);
            }
        }

        private Map<TopicPartition, Long> listOffsets(
                Collection<TopicPartition> partitions, OffsetSpec offsetSpec) {
            return listOffsets(partitions.stream().collect(Collectors.toMap(
                    partition -> partition, __ -> offsetSpec)))
                    .entrySet().stream().collect(Collectors.toMap(
                            Map.Entry::getKey, entry -> entry.getValue().offset()));
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
            return listOffsets(partitions, OffsetSpec.latest());
        }

        @Override
        public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
            return listOffsets(partitions, OffsetSpec.earliest());
        }

        @Override
        public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
                Map<TopicPartition, Long> timestampsToSearch) {
            return listOffsets(timestampsToSearch.entrySet().stream().collect(
                    Collectors.toMap(
                            Map.Entry::getKey, entry ->
                                    OffsetSpec.forTimestamp(entry.getValue()))))
                    .entrySet().stream()
                    // OffsetAndTimestamp cannot be initialized with a negative offset, which is
                    // possible if the timestamp does not correspond to an offset and the topic
                    // partition is empty
                    // 可能存在时间戳未对应到任何偏移量并且主题分区为空
                    .filter(entry -> entry.getValue().offset() >= 0)
                    .collect(Collectors.toMap(
                            Map.Entry::getKey, entry ->
                                    new OffsetAndTimestamp(
                                            entry.getValue().offset(),
                                            entry.getValue().timestamp(),
                                            entry.getValue().leaderEpoch())));
        }

        @Override
        public void close() throws Exception {
            adminClient.close(Duration.ZERO);
        }
    }
}
