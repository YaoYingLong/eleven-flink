/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.NoMoreSplitsEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.ReportedWatermarkEvent;
import org.apache.flink.runtime.source.event.RequestSplitEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.source.TimestampsAndWatermarks;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.MultipleFuturesAvailabilityHelper;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask.CanEmitBatchOfRecordsChecker;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.configuration.PipelineOptions.ALLOW_UNALIGNED_SOURCE_SPLITS;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base source operator only used for integrating the source reader which is proposed by FLIP-27. It
 * implements the interface of {@link PushingAsyncDataInput} which is naturally compatible with one
 * input processing in runtime stack.
 *
 * <p><b>Important Note on Serialization:</b> The SourceOperator inherits the {@link
 * java.io.Serializable} interface from the StreamOperator, but is in fact NOT serializable. The
 * operator must only be instantiated in the StreamTask from its factory.
 *
 * @param <OUT> The output type of the operator.
 */
@Internal
public class SourceOperator<OUT, SplitT extends SourceSplit> extends AbstractStreamOperator<OUT>
        implements OperatorEventHandler,
        PushingAsyncDataInput<OUT>,
        TimestampsAndWatermarks.WatermarkUpdateListener {
    private static final long serialVersionUID = 1405537676017904695L;

    // Package private for unit test.
    static final ListStateDescriptor<byte[]> SPLITS_STATE_DESC =
            new ListStateDescriptor<>("SourceReaderState", BytePrimitiveArraySerializer.INSTANCE);

    /**
     * The factory for the source reader. This is a workaround, because currently the SourceReader
     * must be lazily initialized, which is mainly because the metrics groups that the reader relies
     * on is lazily initialized.
     */
    private final FunctionWithException<SourceReaderContext, SourceReader<OUT, SplitT>, Exception>
            readerFactory;

    /**
     * The serializer for the splits, applied to the split types before storing them in the reader
     * state.
     */
    private final SimpleVersionedSerializer<SplitT> splitSerializer;

    /** The event gateway through which this operator talks to its coordinator. */
    private final OperatorEventGateway operatorEventGateway;

    /** The factory for timestamps and watermark generators. */
    private final WatermarkStrategy<OUT> watermarkStrategy;

    private final WatermarkAlignmentParams watermarkAlignmentParams;

    /** The Flink configuration. */
    private final Configuration configuration;

    /**
     * Host name of the machine where the operator runs, to support locality aware work assignment.
     */
    private final String localHostname;

    /** Whether to emit intermediate watermarks or only one final watermark at the end of input. */
    private final boolean emitProgressiveWatermarks;

    // ---- lazily initialized fields (these fields are the "hot" fields) ----

    /** The source reader that does most of the work. */
    private SourceReader<OUT, SplitT> sourceReader;

    private ReaderOutput<OUT> currentMainOutput;

    private DataOutput<OUT> lastInvokedOutput;

    private long latestWatermark = Watermark.UNINITIALIZED.getTimestamp();

    private boolean idle = false;

    /** The state that holds the currently assigned splits. */
    private ListState<SplitT> readerState;

    /**
     * The event time and watermarking logic. Ideally this would be eagerly passed into this
     * operator, but we currently need to instantiate this lazily, because the metric groups exist
     * only later.
     */
    private TimestampsAndWatermarks<OUT> eventTimeLogic;

    /** A mode to control the behaviour of the {@link #emitNext(DataOutput)} method. */
    private OperatingMode operatingMode;

    private final CompletableFuture<Void> finished = new CompletableFuture<>();
    private final SourceOperatorAvailabilityHelper availabilityHelper =
            new SourceOperatorAvailabilityHelper();

    private final List<SplitT> outputPendingSplits = new ArrayList<>();

    private int numSplits;
    // 在周期接收到协调发送过来的对齐水位时，在checkSplitWatermarkAlignment方法中被使用判断是否需要暂停partition
    private final Map<String, Long> splitCurrentWatermarks = new HashMap<>();
    private final Set<String> currentlyPausedSplits = new HashSet<>();

    private enum OperatingMode {
        READING,
        WAITING_FOR_ALIGNMENT,
        OUTPUT_NOT_INITIALIZED,
        SOURCE_DRAINED,
        SOURCE_STOPPED,
        DATA_FINISHED
    }

    private InternalSourceReaderMetricGroup sourceMetricGroup;
    // 最大的对齐水位线，如果某个partition的水位超过该水位线，则会被暂停
    private long currentMaxDesiredWatermark = Watermark.MAX_WATERMARK.getTimestamp();
    /** Can be not completed only in {@link OperatingMode#WAITING_FOR_ALIGNMENT} mode. */
    private CompletableFuture<Void> waitingForAlignmentFuture =
            CompletableFuture.completedFuture(null);

    private @Nullable LatencyMarkerEmitter<OUT> latencyMarkerEmitter;

    private final boolean allowUnalignedSourceSplits;

    private final CanEmitBatchOfRecordsChecker canEmitBatchOfRecords;

    public SourceOperator(
            FunctionWithException<SourceReaderContext, SourceReader<OUT, SplitT>, Exception> readerFactory,
            OperatorEventGateway operatorEventGateway,
            SimpleVersionedSerializer<SplitT> splitSerializer,
            WatermarkStrategy<OUT> watermarkStrategy,
            ProcessingTimeService timeService,
            Configuration configuration,
            String localHostname,
            boolean emitProgressiveWatermarks,
            CanEmitBatchOfRecordsChecker canEmitBatchOfRecords) {
        // readerFactory其实就是KafkaSource::createReader的函数表达式
        this.readerFactory = checkNotNull(readerFactory);
        this.operatorEventGateway = checkNotNull(operatorEventGateway);
        this.splitSerializer = checkNotNull(splitSerializer);
        // 传入的我们自定义的水位线策略
        this.watermarkStrategy = checkNotNull(watermarkStrategy);
        this.processingTimeService = timeService;
        this.configuration = checkNotNull(configuration);
        this.localHostname = checkNotNull(localHostname);
        // emitProgressiveWatermarks默认为true
        this.emitProgressiveWatermarks = emitProgressiveWatermarks;
        this.operatingMode = OperatingMode.OUTPUT_NOT_INITIALIZED;
        // 水位线对齐参数，我们通过withWatermarkAlignment("default", Duration.ofMinutes(2))自定义的
        this.watermarkAlignmentParams = watermarkStrategy.getAlignmentParameters();
        this.allowUnalignedSourceSplits = configuration.get(ALLOW_UNALIGNED_SOURCE_SPLITS);
        this.canEmitBatchOfRecords = checkNotNull(canEmitBatchOfRecords);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        // 调用了SourceOperator的构造方法后会立即调用setup方法，open方法是晚于setup方法被调用的
        // 这里是调用AbstractStreamOperator的setup方法，初始化了combinedWatermark，以及初始化处理延迟指标
        // output要么是ChainingOutput，要么就是RecordWriterOutput，最终都会被封装为CountingOutput
        super.setup(containingTask, config, output);
        // 初始化指标类
        initSourceMetricGroup();
    }

    @VisibleForTesting
    protected void initSourceMetricGroup() {
        sourceMetricGroup = InternalSourceReaderMetricGroup.wrap(getMetricGroup());
    }

    /**
     * Initializes the reader. The code from this method should ideally happen in the constructor or
     * in the operator factory even. It has to happen here at a slightly later stage, because of the
     * lazy metric initialization.
     *
     * <p>Calling this method explicitly is an optional way to have the reader initialization a bit
     * earlier than in open(), as needed by the {@link
     * org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask}
     *
     * <p>This code should move to the constructor once the metric groups are available at task
     * setup time.
     */
    public void initReader() throws Exception {
        if (sourceReader != null) {
            return;
        }
        final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        final SourceReaderContext context = new SourceReaderContext() {
            @Override
            public SourceReaderMetricGroup metricGroup() {
                return sourceMetricGroup;
            }

            @Override
            public Configuration getConfiguration() {
                return configuration;
            }

            @Override
            public String getLocalHostName() {
                return localHostname;
            }

            @Override
            public int getIndexOfSubtask() {
                return subtaskIndex;
            }

            @Override
            public void sendSplitRequest() {
                operatorEventGateway.sendEventToCoordinator(
                        new RequestSplitEvent(getLocalHostName()));
            }

            @Override
            public void sendSourceEventToCoordinator(SourceEvent event) {
                operatorEventGateway.sendEventToCoordinator(new SourceEventWrapper(event));
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
                return new UserCodeClassLoader() {
                    @Override
                    public ClassLoader asClassLoader() {
                        return getRuntimeContext().getUserCodeClassLoader();
                    }

                    @Override
                    public void registerReleaseHookIfAbsent(
                            String releaseHookName, Runnable releaseHook) {
                        getRuntimeContext().registerUserCodeClassLoaderReleaseHookIfAbsent(
                                releaseHookName, releaseHook);
                    }
                };
            }

            @Override
            public int currentParallelism() {
                return getRuntimeContext().getNumberOfParallelSubtasks();
            }
        };
        // readerFactory其实就是KafkaSource::createReader的函数表达式，在构造方法中被初始化
        // 如果是KafkaSource这里得到的是KafkaSourceReader
        sourceReader = readerFactory.apply(context);
    }

    public InternalSourceReaderMetricGroup getSourceMetricGroup() {
        return sourceMetricGroup;
    }

    @Override
    public void open() throws Exception {
        // 这里其实就是执行readerFactory函数表达式得到真正的SourceReader，如果是KafkaSource这里得到的是KafkaSourceReader
        initReader();

        // in the future when we this one is migrated to the "eager initialization" operator
        // (StreamOperatorV2), then we should evaluate this during operator construction.
        // emitProgressiveWatermarks默认为true
        if (emitProgressiveWatermarks) {
            // 创建ProgressiveTimestampsAndWatermarks
            eventTimeLogic = TimestampsAndWatermarks.createProgressiveEventTimeLogic(
                    // 传入的我们自定义的水位线策略
                    watermarkStrategy,
                    sourceMetricGroup,
                    // 默认为ProcessingTimeServiceImpl
                    getProcessingTimeService(),
                    // 默认是200ms，可通过pipeline.auto-watermark-interval配置
                    getExecutionConfig().getAutoWatermarkInterval());
        } else {
            eventTimeLogic = TimestampsAndWatermarks.createNoOpEventTimeLogic(
                    watermarkStrategy, sourceMetricGroup);
        }

        // restore the state if necessary.
        // readerState会在initializeState中被初始化，initializeState会先open方法被调用
        final List<SplitT> splits = CollectionUtil.iterableToList(readerState.get());
        if (!splits.isEmpty()) {
            LOG.info("Restoring state for {} split(s) to reader.", splits.size());
            // 如果从状态中恢复了分区，则将分区添加到sourceReader中
            sourceReader.addSplits(splits);
        }

        // Register the reader to the coordinator.
        /**
         * 关键代码：通过RPC向JobMaster协调者注册当前的SourceReader，然后在JobMaster的SplitEnumerator会进行分区的分配
         * 并给当前的subtask发送AddSplitEvent事件，最终其实是调用该类的handleOperatorEvent方法，完成分区的分配
         */
        registerReader();
        // 指标中将其标记为空闲，记录空闲开始时间
        sourceMetricGroup.idlingStarted();
        // Start the reader after registration, sending messages in start is allowed.
        // 对于KafkaSourceReader该方法是一个空实现
        sourceReader.start();
        // 每200ms执行一次执行currentPerSplitOutputs和currentMainOutput的emitPeriodicWatermark()方法
        eventTimeLogic.startPeriodicWatermarkEmits();
    }

    @Override
    public void finish() throws Exception {
        stopInternalServices();
        super.finish();

        finished.complete(null);
    }

    private void stopInternalServices() {
        if (eventTimeLogic != null) {
            eventTimeLogic.stopPeriodicWatermarkEmits();
        }
        if (latencyMarkerEmitter != null) {
            latencyMarkerEmitter.close();
        }
    }

    public CompletableFuture<Void> stop(StopMode mode) {
        switch (operatingMode) {
            case WAITING_FOR_ALIGNMENT:
            case OUTPUT_NOT_INITIALIZED:
            case READING:
                this.operatingMode = mode == StopMode.DRAIN
                        ? OperatingMode.SOURCE_DRAINED : OperatingMode.SOURCE_STOPPED;
                availabilityHelper.forceStop();
                if (this.operatingMode == OperatingMode.SOURCE_STOPPED) {
                    stopInternalServices();
                    finished.complete(null);
                    return finished;
                }
                break;
        }
        return finished;
    }

    @Override
    public void close() throws Exception {
        if (sourceReader != null) {
            sourceReader.close();
        }
        super.close();
    }

    @Override
    public DataInputStatus emitNext(DataOutput<OUT> output) throws Exception {
        // guarding an assumptions we currently make due to the fact that certain classes
        // assume a constant output, this assumption does not need to stand if we emitted all
        // records. In that case the output will change to FinishedDataOutput

        // 如果是KafkaSource这里的output是AsyncDataOutputToOutput
        // AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
        assert lastInvokedOutput == output
                || lastInvokedOutput == null
                || this.operatingMode == OperatingMode.DATA_FINISHED;

        // short circuit the hot path. Without this short circuit (READING handled in the
        // switch/case) InputBenchmark.mapSink was showing a performance regression.
        // TODO 需要确定一点是否是每次启动都一定会先走到emitNextNotReading
        if (operatingMode != OperatingMode.READING) {
            // 短路热路径，如果没有这个短路（在switch/case中处理READING），InputBenchmark.mapSink会出现性能回退
            return emitNextNotReading(output);
        }

        InputStatus status;
        do {
            // 这里的sourceReader为KafkaSourceReader，但这里是调用的超类SourceReaderBase的pollNext
            status = sourceReader.pollNext(currentMainOutput);
        } while (status == InputStatus.MORE_AVAILABLE
                // 这里的canEmitBatchOfRecords是StreamTask的getCanEmitBatchOfRecords方法的函数表达式
                // 如果mailboxProcessor中有其他任务需要退出循环，线执行mailboxProcessor中的其他任务
                && canEmitBatchOfRecords.check()
                // 判断如果最大的水位，小于最后的水位，返回true，需要退出数据处理循环，等待水位线
                && !shouldWaitForAlignment());
        return convertToInternalStatus(status);
    }

    private DataInputStatus emitNextNotReading(DataOutput<OUT> output) throws Exception {
        switch (operatingMode) {
            // 构造方法中operatingMode会初始化为OUTPUT_NOT_INITIALIZED
            case OUTPUT_NOT_INITIALIZED:
                // 如果通过withWatermarkAlignment("default", Duration.ofMinutes(2))自定义了，这返回true
                if (watermarkAlignmentParams.isEnabled()) {
                    // Only wrap the output when watermark alignment is enabled, as otherwise this
                    // introduces a small performance regression (probably because of an extra virtual call)
                    /**
                     * RPC上报水位线，如果是空闲的则上报Long.MAX_VALUE作为水位线，否则上报latestWatermark作为最新的水位线
                     * 这里其实就是将ReportedWatermarkEvent事件上报到，SourceCoordinator的handleEventFromOperator方法中处理
                     */
                    processingTimeService.scheduleWithFixedDelay(
                            time -> emitLatestWatermark(),
                            // 更新周期&上报周期，不设置默认是1s
                            watermarkAlignmentParams.getUpdateInterval(),
                            // 更新周期&上报周期，不设置默认是1s
                            watermarkAlignmentParams.getUpdateInterval());
                }
                // 如果是KafkaSource这里的output是AsyncDataOutputToOutput
                // AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
                initializeMainOutput(output);
                // 如果是KafkaSource这里的output是AsyncDataOutputToOutput
                // AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
                // 这里得currentMainOutput是StreamingReaderOutput其实本质上是SourceOutputWithWatermarks
                return convertToInternalStatus(sourceReader.pollNext(currentMainOutput));
            case SOURCE_STOPPED:
                this.operatingMode = OperatingMode.DATA_FINISHED;
                // 指标中将其标记为空闲，记录空闲开始时间
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.STOPPED;
            case SOURCE_DRAINED:
                this.operatingMode = OperatingMode.DATA_FINISHED;
                // 指标中将其标记为空闲，记录空闲开始时间
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.END_OF_DATA;
            case DATA_FINISHED:
                if (watermarkAlignmentParams.isEnabled()) {
                    latestWatermark = Watermark.MAX_WATERMARK.getTimestamp();
                    emitLatestWatermark();
                }
                // 指标中将其标记为空闲，记录空闲开始时间
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.END_OF_INPUT;
            case WAITING_FOR_ALIGNMENT:
                checkState(!waitingForAlignmentFuture.isDone());
                checkState(shouldWaitForAlignment());
                // 出现等待水位对齐，即水位线超过最大水位
                return convertToInternalStatus(InputStatus.NOTHING_AVAILABLE);
            case READING:
            default:
                throw new IllegalStateException("Unknown operating mode: " + operatingMode);
        }
    }

    private void initializeMainOutput(DataOutput<OUT> output) {
        // eventTimeLogic在open方法中被初始化，如果是KafkaSource这里的output是AsyncDataOutputToOutput
        // AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
        // 这里得到的是StreamingReaderOutput其实本质上是SourceOutputWithWatermarks
        currentMainOutput = eventTimeLogic.createMainOutput(output, this);
        // 延迟数据周期处理逻辑
        initializeLatencyMarkerEmitter(output);
        lastInvokedOutput = output;
        // Create per-split output for pending splits added before main output is initialized
        createOutputForSplits(outputPendingSplits);
        this.operatingMode = OperatingMode.READING;
    }

    private void initializeLatencyMarkerEmitter(DataOutput<OUT> output) {
        long latencyTrackingInterval = getExecutionConfig().isLatencyTrackingConfigured()
                ? getExecutionConfig().getLatencyTrackingInterval()
                : getContainingTask()
                .getEnvironment()
                .getTaskManagerInfo()
                .getConfiguration()
                .getLong(MetricOptions.LATENCY_INTERVAL);
        // latencyTrackingInterval默认是0，表示不跟踪延迟
        if (latencyTrackingInterval > 0) {
            // 如果需要跟踪延迟

            // 如果是KafkaSource这里的output是AsyncDataOutputToOutput
            // AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
            latencyMarkerEmitter = new LatencyMarkerEmitter<>(
                    getProcessingTimeService(),
                    output::emitLatencyMarker,
                    latencyTrackingInterval,
                    getOperatorID(),
                    getRuntimeContext().getIndexOfThisSubtask());
        }
    }

    private DataInputStatus convertToInternalStatus(InputStatus inputStatus) {
        switch (inputStatus) {
            case MORE_AVAILABLE:
                return DataInputStatus.MORE_AVAILABLE;
            case NOTHING_AVAILABLE:
                // 指标中将其标记为空闲，记录空闲开始时间
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.NOTHING_AVAILABLE;
            case END_OF_INPUT:
                this.operatingMode = OperatingMode.DATA_FINISHED;
                // 指标中将其标记为空闲，记录空闲开始时间
                sourceMetricGroup.idlingStarted();
                return DataInputStatus.END_OF_DATA;
            default:
                throw new IllegalArgumentException("Unknown input status: " + inputStatus);
        }
    }

    private void emitLatestWatermark() {
        checkState(currentMainOutput != null);
        if (latestWatermark == Watermark.UNINITIALIZED.getTimestamp()) {
            // 如果水位线还未初始化直接return
            return;
        }
        /**
         * RPC上报水位线，如果是空闲的则上报Long.MAX_VALUE作为水位线，否则上报latestWatermark作为最新的水位线
         * 这里其实就是将ReportedWatermarkEvent事件上报到，SourceCoordinator的handleEventFromOperator方法中处理
         */
        operatorEventGateway.sendEventToCoordinator(new ReportedWatermarkEvent(
                idle ? Watermark.MAX_WATERMARK.getTimestamp() : latestWatermark));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        // 在StreamOperatorStateHandler中的snapshotState方法中被调用
        long checkpointId = context.getCheckpointId();
        LOG.debug("Taking a snapshot for checkpoint {}", checkpointId);
        readerState.update(sourceReader.snapshotState(checkpointId));
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        switch (operatingMode) {
            case WAITING_FOR_ALIGNMENT:
                // 这里其实就是返回waitingForAlignmentFuture是否处于完成状态，即等待水位线结束
                return availabilityHelper.update(waitingForAlignmentFuture);
            case OUTPUT_NOT_INITIALIZED:
            case READING:
                // 只要有待处理的数据，就返回AVAILABLE
                return availabilityHelper.update(sourceReader.isAvailable());
            case SOURCE_STOPPED:
            case SOURCE_DRAINED:
            case DATA_FINISHED:
                return AvailabilityProvider.AVAILABLE;
            default:
                throw new IllegalStateException("Unknown operating mode: " + operatingMode);
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        final ListState<byte[]> rawState =
                context.getOperatorStateStore().getListState(SPLITS_STATE_DESC);
        readerState = new SimpleVersionedListState<>(rawState, splitSerializer);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        sourceReader.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
        sourceReader.notifyCheckpointAborted(checkpointId);
    }

    @SuppressWarnings("unchecked")
    public void handleOperatorEvent(OperatorEvent event) {
        if (event instanceof WatermarkAlignmentEvent) {
            // 水位线等待对齐事件
            // 将currentMaxDesiredWatermark更新为事件中传过来的水位线
            updateMaxDesiredWatermark((WatermarkAlignmentEvent) event);
            // 判断最大期待水位和当前水位做比较，以及判断状态，修改为等待或者恢复消费
            checkWatermarkAlignment();
            // 这里是遍历所有的分区的水位线，如果水位线超过最大期望的水位，需要暂停该分区的消费，如果小于且已经被暂停，需要恢复消费
            checkSplitWatermarkAlignment();
        } else if (event instanceof AddSplitEvent) {
            // 接收从JobMaster上发送来的AddSplitEvent事件，处理具体的分区
            handleAddSplitsEvent(((AddSplitEvent<SplitT>) event));
        } else if (event instanceof SourceEventWrapper) {
            sourceReader.handleSourceEvents(((SourceEventWrapper) event).getSourceEvent());
        } else if (event instanceof NoMoreSplitsEvent) {
            sourceReader.notifyNoMoreSplits();
        } else {
            throw new IllegalStateException("Received unexpected operator event " + event);
        }
    }

    /**
     * JobMaster启动时会启动，SourceCoordinator，SourceCoordinator会启动KafkaSourceEnumerator
     * KafkaSourceEnumerator会根据并行度，给每个并行度预先分配分区数据量，当SubTask启动后调用SourceOperator的open时
     * 会向JobMaster注册当前的reader，然后通过KafkaSourceEnumerator获取根据并行度预先分配分区列表，
     * 然后由JobMaster向SubTask发送AddSplitEvent事件，完成分区的分配
     */
    private void handleAddSplitsEvent(AddSplitEvent<SplitT> event) {
        try {
            // 反序列化
            List<SplitT> newSplits = event.splits(splitSerializer);
            // numSplits默认是0
            numSplits += newSplits.size();
            // 如果没有被初始化
            if (operatingMode == OperatingMode.OUTPUT_NOT_INITIALIZED) {
                // For splits arrived before the main output is initialized, store them into the
                // pending list. Outputs of these splits will be created once the main output is ready.
                outputPendingSplits.addAll(newSplits);
            } else {
                // Create output directly for new splits if the main output is already initialized.
                createOutputForSplits(newSplits);
            }
            // 调用SourceReaderBase的addSplits方法
            sourceReader.addSplits(newSplits);
        } catch (IOException e) {
            throw new FlinkRuntimeException("Failed to deserialize the splits.", e);
        }
    }

    private void createOutputForSplits(List<SplitT> newSplits) {
        for (SplitT split : newSplits) {
            // 调用StreamingReaderOutput的createOutputForSplit方法
            currentMainOutput.createOutputForSplit(split.splitId());
        }
    }

    private void updateMaxDesiredWatermark(WatermarkAlignmentEvent event) {
        // 当前最大的水位线
        currentMaxDesiredWatermark = event.getMaxWatermark();
        // 记录指标
        sourceMetricGroup.updateMaxDesiredWatermark(currentMaxDesiredWatermark);
    }

    @Override
    public void updateIdle(boolean isIdle) {
        // 设置当前操作符的空闲状态
        this.idle = isIdle;
    }

    @Override
    public void updateCurrentEffectiveWatermark(long watermark) {
        // 周期被调用，这里设置的是WatermarkOutputMultiplexer中所有分区中水位线最小的作为latestWatermark
        latestWatermark = watermark;
        // 检查水位线对齐，设置operatingMode为WAITING_FOR_ALIGNMENT或READING
        checkWatermarkAlignment();
    }

    @Override
    public void updateCurrentSplitWatermark(String splitId, long watermark) {
        // 通知关于每个分片的水位线的变化，同updateCurrentEffectiveWatermark方法一样被周期调用
        // 在周期接收到协调发送过来的对齐水位时，在checkSplitWatermarkAlignment方法中被使用判断是否需要暂停partition
        splitCurrentWatermarks.put(splitId, watermark);
        // 如果当前的分片数大于1，并且当前分片的水位线大于currentMaxDesiredWatermark，并且当前分片没有被暂停
        // currentMaxDesiredWatermark是通过SourceCoordinator中周期同步的最大允许的水位值
        if (numSplits > 1 && watermark > currentMaxDesiredWatermark
                && !currentlyPausedSplits.contains(splitId)) {
            // 其实就是异步调用KafkaPartitionSplitReader的pauseOrResumeSplits方法，暂停分区消费
            pauseOrResumeSplits(Collections.singletonList(splitId), Collections.emptyList());
            // 将当前分片添加到currentlyPausedSplits中，表示当前分片已经被暂停
            currentlyPausedSplits.add(splitId);
        }
    }

    /**
     * Finds the splits that are beyond the current max watermark and pauses them. At the same time,
     * splits that have been paused and where the global watermark caught up are resumed.
     *
     * <p>Note: This takes effect only if there are multiple splits, otherwise it does nothing.
     */
    private void checkSplitWatermarkAlignment() {
        if (numSplits <= 1) {
            // A single split can't overtake any other splits assigned to this operator instance.
            // It is sufficient for the source to stop processing.
            return;
        }
        Collection<String> splitsToPause = new ArrayList<>();
        Collection<String> splitsToResume = new ArrayList<>();
        // 这里是遍历所有的分区的水位线，如果水位线超过最大期望的水位，需要暂停该分区的消费，如果小于且已经被暂停，需要恢复消费
        splitCurrentWatermarks.forEach(
                (splitId, splitWatermark) -> {
                    if (splitWatermark > currentMaxDesiredWatermark) {
                        splitsToPause.add(splitId);
                    } else if (currentlyPausedSplits.contains(splitId)) {
                        splitsToResume.add(splitId);
                    }
                });
        // 将需要暂停的列表中，移除需要恢复的partitonh
        splitsToPause.removeAll(currentlyPausedSplits);
        if (!splitsToPause.isEmpty() || !splitsToResume.isEmpty()) {
            // 这里是调用KafkaConsumer的原生API取暂停或恢复分区消费
            pauseOrResumeSplits(splitsToPause, splitsToResume);
            currentlyPausedSplits.addAll(splitsToPause);
            splitsToResume.forEach(currentlyPausedSplits::remove);
        }
    }

    private void pauseOrResumeSplits(
            Collection<String> splitsToPause, Collection<String> splitsToResume) {
        try {
            // 调用KafkaSourceReader的pauseOrResumeSplits方法，暂停或恢复分片
            // 其实就是异步调用KafkaPartitionSplitReader的pauseOrResumeSplits方法
            sourceReader.pauseOrResumeSplits(splitsToPause, splitsToResume);
        } catch (UnsupportedOperationException e) {
            if (!allowUnalignedSourceSplits) {
                throw e;
            }
        }
    }

    private void checkWatermarkAlignment() {
        if (operatingMode == OperatingMode.READING) {
            // 如果isDone返回false，则抛出异常，这里是判断是否已经存在一个未完成的waitingForAlignmentFuture
            checkState(waitingForAlignmentFuture.isDone());
            if (shouldWaitForAlignment()) {
                // 如果currentMaxDesiredWatermark < latestWatermark，将状态设置为WAITING_FOR_ALIGNMENT等待对齐
                operatingMode = OperatingMode.WAITING_FOR_ALIGNMENT;
                // 这里会将waitingForAlignmentFuture设置为未完成状态
                waitingForAlignmentFuture = new CompletableFuture<>();
            }
        } else if (operatingMode == OperatingMode.WAITING_FOR_ALIGNMENT) {
            // 如果isDone返回false，则抛出异常
            checkState(!waitingForAlignmentFuture.isDone());
            // 如果已经处理水位对齐等待过程中了，如果比较水位发现，不需要再等待对齐了，则修改状态
            if (!shouldWaitForAlignment()) {
                // 如果currentMaxDesiredWatermark >= latestWatermark将状态设置为READING
                operatingMode = OperatingMode.READING;
                // 这里会将waitingForAlignmentFuture设置为已完成状态
                waitingForAlignmentFuture.complete(null);
            }
        }
    }

    private boolean shouldWaitForAlignment() {
        /**
         * 关键代码，返回是否需要等待水位对齐，currentMaxDesiredWatermark表示最大的期望的水位线
         * latestWatermark表示当前水位，如果当前水位都超过了最大期望的水位线，肯定是需要停止任务等待水位对齐的
         */
        return currentMaxDesiredWatermark < latestWatermark;
    }

    private void registerReader() {
        // 调用OperatorEventGatewayImpl的sendEventToCoordinator方法，向JobMaster上的SourceCoordinator注册当前的reader
        operatorEventGateway.sendEventToCoordinator(new ReaderRegistrationEvent(
                getRuntimeContext().getIndexOfThisSubtask(), localHostname));
    }

    // --------------- methods for unit tests ------------

    @VisibleForTesting
    public SourceReader<OUT, SplitT> getSourceReader() {
        return sourceReader;
    }

    @VisibleForTesting
    ListState<SplitT> getReaderState() {
        return readerState;
    }

    private static class SourceOperatorAvailabilityHelper {
        private final CompletableFuture<Void> forcedStopFuture = new CompletableFuture<>();
        private final MultipleFuturesAvailabilityHelper availabilityHelper;

        private SourceOperatorAvailabilityHelper() {
            availabilityHelper = new MultipleFuturesAvailabilityHelper(2);
            availabilityHelper.anyOf(0, forcedStopFuture);
        }

        public CompletableFuture<?> update(CompletableFuture<Void> sourceReaderFuture) {
            if (sourceReaderFuture == AvailabilityProvider.AVAILABLE
                    || sourceReaderFuture.isDone()) {
                return AvailabilityProvider.AVAILABLE;
            }
            availabilityHelper.resetToUnAvailable();
            availabilityHelper.anyOf(0, forcedStopFuture);
            availabilityHelper.anyOf(1, sourceReaderFuture);
            return availabilityHelper.getAvailableFuture();
        }

        public void forceStop() {
            forcedStopFuture.complete(null);
        }
    }
}
