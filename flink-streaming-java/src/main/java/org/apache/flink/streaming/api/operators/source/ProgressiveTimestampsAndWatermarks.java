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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of {@link TimestampsAndWatermarks} that does periodic watermark emission and
 * keeps track of watermarks on a per-split basis. This should be used in execution contexts where
 * watermarks are important for efficiency/correctness, for example in STREAMING execution mode.
 *
 * @param <T> The type of the emitted records.
 */
@Internal
public class ProgressiveTimestampsAndWatermarks<T> implements TimestampsAndWatermarks<T> {

    private final TimestampAssigner<T> timestampAssigner;

    private final WatermarkGeneratorSupplier<T> watermarksFactory;

    private final WatermarkGeneratorSupplier.Context watermarksContext;
    // 默认是ProcessingTimeServiceImpl
    private final ProcessingTimeService timeService;
    // 默认200ms
    private final long periodicWatermarkInterval;

    @Nullable
    private SplitLocalOutputs<T> currentPerSplitOutputs;

    @Nullable
    private StreamingReaderOutput<T> currentMainOutput;

    @Nullable
    private ScheduledFuture<?> periodicEmitHandle;

    public ProgressiveTimestampsAndWatermarks(
            TimestampAssigner<T> timestampAssigner,
            WatermarkGeneratorSupplier<T> watermarksFactory,
            WatermarkGeneratorSupplier.Context watermarksContext,
            ProcessingTimeService timeService,
            Duration periodicWatermarkInterval) {
        // 如果有定义，则一般为我们自定义的，用于从数据中提取时间戳的逻辑
        this.timestampAssigner = timestampAssigner;
        // 其实就是自定义的水位线策略WatermarkStrategy
        this.watermarksFactory = watermarksFactory;
        this.watermarksContext = watermarksContext;
        // 默认是ProcessingTimeServiceImpl
        this.timeService = timeService;

        long periodicWatermarkIntervalMillis;
        try {
            // 默认200ms
            periodicWatermarkIntervalMillis = periodicWatermarkInterval.toMillis();
        } catch (ArithmeticException ignored) {
            // long integer overflow
            periodicWatermarkIntervalMillis = Long.MAX_VALUE;
        }
        this.periodicWatermarkInterval = periodicWatermarkIntervalMillis;
    }

    // ------------------------------------------------------------------------

    // 当调用SourceOperator的initializeMainOutput方法时被调用，即真正开始处理数据时
    @Override
    public ReaderOutput<T> createMainOutput(
            PushingAsyncDataInput.DataOutput<T> output,
            WatermarkUpdateListener watermarkUpdateListener) {
        // 传入的watermarkUpdateListener其实就是SourceOperator，其实现了WatermarkUpdateListener接口

        // At the moment, we assume only one output is ever created!
        // This assumption is strict, currently, because many of the classes in this implementation
        // do not support re-assigning the underlying output
        checkState(
                currentMainOutput == null && currentPerSplitOutputs == null,
                "already created a main output");
        // 如果是KafkaSource这里的output是AsyncDataOutputToOutput
        // AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
        // 将output封装成WatermarkToDataOutput
        final WatermarkOutput watermarkOutput =
                new WatermarkToDataOutput(output, watermarkUpdateListener);
        IdlenessManager idlenessManager = new IdlenessManager(watermarkOutput);

        // 其实就是从函数表达式转换为，我们自定义的水位线WatermarkGenerator
        final WatermarkGenerator<T> watermarkGenerator =
                watermarksFactory.createWatermarkGenerator(watermarksContext);
        // 可以理解为SourceOutputWithWatermarks容器，每个partition都会通过currentPerSplitOutputs创建一个SourceOutputWithWatermarks
        currentPerSplitOutputs = new SplitLocalOutputs<>(
                // output是AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
                output,
                // 其实就是将watermarkOutput再次封装成IdlenessAwareWatermarkOutput
                idlenessManager.getSplitLocalOutput(),
                // 传入的watermarkUpdateListener其实就是SourceOperator
                watermarkUpdateListener,
                // 如果有定义，则一般为我们自定义的，用于从数据中提取时间戳的逻辑
                timestampAssigner,
                watermarksFactory,
                watermarksContext);
        // StreamingReaderOutput其实本质上是SourceOutputWithWatermarks
        currentMainOutput = new StreamingReaderOutput<>(
                // output是AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
                output,
                // 其实就是将watermarkOutput再次封装成IdlenessAwareWatermarkOutput
                idlenessManager.getMainOutput(),
                // 如果有定义，则一般为我们自定义的，用于从数据中提取时间戳的逻辑
                timestampAssigner,
                // 其实就是从函数表达式转换为，我们自定义的水位线WatermarkGenerator
                watermarkGenerator,
                // currentPerSplitOutputs是SplitLocalOutputs
                currentPerSplitOutputs);
        return currentMainOutput;
    }

    @Override
    public void startPeriodicWatermarkEmits() {
        checkState(periodicEmitHandle == null, "periodic emitter already started");

        // periodicWatermarkInterval默认是200ms
        if (periodicWatermarkInterval == 0) {
            // a value of zero means not activated
            return;
        }
        // 每200ms执行一次triggerPeriodicEmit方法
        // 其实就是执行currentPerSplitOutputs和currentMainOutput的emitPeriodicWatermark()方法
        periodicEmitHandle = timeService.scheduleWithFixedDelay(
                this::triggerPeriodicEmit,
                // 默认200ms
                periodicWatermarkInterval,
                // 默认200ms
                periodicWatermarkInterval);
    }

    @Override
    public void stopPeriodicWatermarkEmits() {
        if (periodicEmitHandle != null) {
            periodicEmitHandle.cancel(false);
            periodicEmitHandle = null;
        }
    }

    void triggerPeriodicEmit(@SuppressWarnings("unused") long wallClockTimestamp) {
        // 这里传入的wallClockTimestamp时间戳是下一次执行的时间
        // currentPerSplitOutputs和createMainOutput都是在createMainOutput方法中被初始化
        // 当调用SourceOperator的initializeMainOutput方法时被调用，即真正开始处理数据时
        if (currentPerSplitOutputs != null) {
            // 调用SplitLocalOutputs的emitPeriodicWatermark方法
            currentPerSplitOutputs.emitPeriodicWatermark();
        }
        if (currentMainOutput != null) {
            // 调用StreamingReaderOutput的超类SourceOutputWithWatermarks的emitPeriodicWatermark方法
            currentMainOutput.emitPeriodicWatermark();
        }
    }

    // ------------------------------------------------------------------------

    private static final class StreamingReaderOutput<T> extends SourceOutputWithWatermarks<T>
            implements ReaderOutput<T> {

        private final SplitLocalOutputs<T> splitLocalOutputs;

        StreamingReaderOutput(
                PushingAsyncDataInput.DataOutput<T> output,
                WatermarkOutput watermarkOutput,
                TimestampAssigner<T> timestampAssigner,
                WatermarkGenerator<T> watermarkGenerator,
                SplitLocalOutputs<T> splitLocalOutputs) {

            super(output, watermarkOutput, watermarkOutput, timestampAssigner, watermarkGenerator);
            this.splitLocalOutputs = splitLocalOutputs;
        }

        @Override
        public SourceOutput<T> createOutputForSplit(String splitId) {
            // 这里创建的是SourceOutputWithWatermarks
            return splitLocalOutputs.createOutputForSplit(splitId);
        }

        @Override
        public void releaseOutputForSplit(String splitId) {
            splitLocalOutputs.releaseOutputForSplit(splitId);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A holder and factory for split-local {@link SourceOutput}s. The split-local outputs maintain
     * local watermark generators with their own state, to facilitate per-split watermarking logic.
     *
     * @param <T> The type of the emitted records.
     */
    private static final class SplitLocalOutputs<T> {

        private final WatermarkOutputMultiplexer watermarkMultiplexer;
        private final Map<String, SourceOutputWithWatermarks<T>> localOutputs;
        private final PushingAsyncDataInput.DataOutput<T> recordOutput;
        private final TimestampAssigner<T> timestampAssigner;
        private final WatermarkGeneratorSupplier<T> watermarksFactory;
        private final WatermarkGeneratorSupplier.Context watermarkContext;
        private final WatermarkUpdateListener watermarkUpdateListener;

        private SplitLocalOutputs(
                PushingAsyncDataInput.DataOutput<T> recordOutput,
                WatermarkOutput watermarkOutput,
                WatermarkUpdateListener watermarkUpdateListener,
                TimestampAssigner<T> timestampAssigner,
                WatermarkGeneratorSupplier<T> watermarksFactory,
                WatermarkGeneratorSupplier.Context watermarkContext) {
            // recordOutput是AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
            this.recordOutput = recordOutput;
            // 如果有定义，则一般为我们自定义的，用于从数据中提取时间戳的逻辑
            this.timestampAssigner = timestampAssigner;
            // 其实就是自定义的水位线策略WatermarkStrategy，通过fromSource传入的
            this.watermarksFactory = watermarksFactory;
            this.watermarkContext = watermarkContext;
            // 这里的watermarkUpdateListener其实就是SourceOperator
            this.watermarkUpdateListener = watermarkUpdateListener;
            // 如果是KafkaSource这里的output是AsyncDataOutputToOutput
            // AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
            // 将output封装成WatermarkToDataOutput然后再在这里封装成IdlenessAwareWatermarkOutput
            // 又将watermarkOutput封装成了WatermarkOutputMultiplexer
            this.watermarkMultiplexer = new WatermarkOutputMultiplexer(watermarkOutput);
            // we use a LinkedHashMap because it iterates faster
            this.localOutputs = new LinkedHashMap<>();
        }

        SourceOutput<T> createOutputForSplit(String splitId) {
            // splitId是对应的分区的id
            final SourceOutputWithWatermarks<T> previous = localOutputs.get(splitId);
            if (previous != null) {
                // 如果splitId已经存在于localOutputs中，说明该split已经创建过SourceOutputWithWatermarks
                return previous;
            }
            // 这里的watermarkUpdateListener其实就是SourceOperator
            // 这里其实就是创建一个新的PartialWatermark添加到WatermarkOutputMultiplexer中的CombinedWatermarkStatus中
            watermarkMultiplexer.registerNewOutput(
                    splitId, watermark -> watermarkUpdateListener.updateCurrentSplitWatermark(
                            splitId, watermark));
            // 将分区splitId生成的对应的PartialWatermark封装成ImmediateOutput
            final WatermarkOutput onEventOutput = watermarkMultiplexer.getImmediateOutput(splitId);
            // 将分区splitId生成的对应的PartialWatermark封装成DeferredOutput
            final WatermarkOutput periodicOutput = watermarkMultiplexer.getDeferredOutput(splitId);
            // 其实就是从函数表达式转换为，我们自定义的水位线WatermarkGenerator
            final WatermarkGenerator<T> watermarks =
                    watermarksFactory.createWatermarkGenerator(watermarkContext);

            // recordOutput是AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
            // timestampAssigner如果有定义，则一般为我们自定义的，用于从数据中提取时间戳的逻辑
            // 这里其实就是new一个SourceOutputWithWatermarks
            final SourceOutputWithWatermarks<T> localOutput = SourceOutputWithWatermarks
                    .createWithSeparateOutputs(
                            recordOutput,
                            onEventOutput,
                            periodicOutput,
                            timestampAssigner,
                            watermarks);

            localOutputs.put(splitId, localOutput);
            // 返回SourceOutputWithWatermarks
            return localOutput;
        }

        void releaseOutputForSplit(String splitId) {
            localOutputs.remove(splitId);
            watermarkMultiplexer.unregisterOutput(splitId);
        }

        void emitPeriodicWatermark() {
            // The call in the loop only records the next watermark candidate for each local output.
            // The call to 'watermarkMultiplexer.onPeriodicEmit()' actually merges the watermarks.
            // That way, we save inefficient repeated merging of (partially outdated) watermarks
            // before all local generators have emitted their candidates.
            for (SourceOutputWithWatermarks<?> output : localOutputs.values()) {
                /**
                 * 调用SourceOutputWithWatermarks的emitPeriodicWatermark
                 * 最终调用WatermarkGenerator的onPeriodicEmit方法
                 * 这里的作用其实是判断当前分片的水位线大于currentMaxDesiredWatermark，并且当前分片没有被暂停
                 */
                output.emitPeriodicWatermark();
            }
            /**
             * subtask分配的每一个分区都会调用createOutputForSplit方法，为将每一个分区都注册到watermarkMultiplexer中
             * 这里调用WatermarkOutputMultiplexer的emitWatermark方法，遍历所有的分区的水位线，将所有分区最小的水位线作为
             * 组合后的水位线
             */
            watermarkMultiplexer.onPeriodicEmit();
        }
    }

    /**
     * A helper class for managing idleness status of the underlying output.
     *
     * <p>This class tracks the idleness status of main and split-local output, and only marks the
     * underlying output as idle if both main and per-split output are idle.
     *
     * <p>The reason of adding this manager is that the implementation of source reader might only
     * use one of main or split-local output for emitting records and watermarks, and we could avoid
     * watermark generator on the vacant output keep marking the underlying output as idle.
     */
    private static class IdlenessManager {
        private final WatermarkOutput underlyingOutput;
        private final IdlenessAwareWatermarkOutput splitLocalOutput;
        private final IdlenessAwareWatermarkOutput mainOutput;

        IdlenessManager(WatermarkOutput underlyingOutput) {
            this.underlyingOutput = underlyingOutput;
            this.splitLocalOutput = new IdlenessAwareWatermarkOutput(underlyingOutput);
            // 如果是KafkaSource这里的output是AsyncDataOutputToOutput
            // AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
            // 将output封装成WatermarkToDataOutput然后再在这里封装成IdlenessAwareWatermarkOutput
            this.mainOutput = new IdlenessAwareWatermarkOutput(underlyingOutput);
        }

        IdlenessAwareWatermarkOutput getSplitLocalOutput() {
            return splitLocalOutput;
        }

        IdlenessAwareWatermarkOutput getMainOutput() {
            return mainOutput;
        }

        void maybeMarkUnderlyingOutputAsIdle() {
            if (splitLocalOutput.isIdle && mainOutput.isIdle) {
                underlyingOutput.markIdle();
            }
        }

        private class IdlenessAwareWatermarkOutput implements WatermarkOutput {
            private final WatermarkOutput underlyingOutput;
            private boolean isIdle = true;

            private IdlenessAwareWatermarkOutput(WatermarkOutput underlyingOutput) {
                // 如果是KafkaSource这里的output是AsyncDataOutputToOutput其是对ChainingOutput或RecordWriterOutput
                // 进行了一次封装，最后将output封装成WatermarkToDataOutput
                this.underlyingOutput = underlyingOutput;
            }

            @Override
            public void emitWatermark(Watermark watermark) {
                /**
                 * 如果是KafkaSource这里的output是AsyncDataOutputToOutput
                 * AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
                 * 将output封装成WatermarkToDataOutput，这里调用WatermarkToDataOutput的emitWatermark
                 * 
                 * 这里的watermark是当前subtask的所有分配的分区的水位线最小的水位
                 */
                underlyingOutput.emitWatermark(watermark);
                isIdle = false;
            }

            @Override
            public void markIdle() {
                isIdle = true;
                maybeMarkUnderlyingOutputAsIdle();
            }

            @Override
            public void markActive() {
                isIdle = false;
                underlyingOutput.markActive();
            }
        }
    }
}
