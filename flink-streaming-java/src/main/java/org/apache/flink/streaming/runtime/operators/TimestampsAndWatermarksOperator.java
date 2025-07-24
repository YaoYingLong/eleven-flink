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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.eventtime.NoWatermarksGenerator;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import static org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A stream operator that may do one or both of the following: extract timestamps from events and
 * generate watermarks.
 *
 * <p>These two responsibilities run in the same operator rather than in two different ones, because
 * the implementation of the timestamp assigner and the watermark generator is frequently in the
 * same class (and should be run in the same instance), even though the separate interfaces support
 * the use of different classes.
 *
 * @param <T> The type of the input elements
 */
public class TimestampsAndWatermarksOperator<T> extends AbstractStreamOperator<T>
        implements OneInputStreamOperator<T, T>, ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;

    private final WatermarkStrategy<T> watermarkStrategy;

    /** The timestamp assigner. */
    private transient TimestampAssigner<T> timestampAssigner;

    /** The watermark generator, initialized during runtime. */
    private transient WatermarkGenerator<T> watermarkGenerator;

    /** The watermark output gateway, initialized during runtime. */
    private transient WatermarkOutput wmOutput;

    /** The interval (in milliseconds) for periodic watermark probes. Initialized during runtime. */
    private transient long watermarkInterval;

    /** Whether to emit intermediate watermarks or only one final watermark at the end of input. */
    private final boolean emitProgressiveWatermarks;

    public TimestampsAndWatermarksOperator(
            WatermarkStrategy<T> watermarkStrategy, boolean emitProgressiveWatermarks) {
        this.watermarkStrategy = checkNotNull(watermarkStrategy);
        // 默认为true
        this.emitProgressiveWatermarks = emitProgressiveWatermarks;
        // 表示总是尝试将当前算子与上游算子链在一起，Flink 会尽可能地将多个算子合并到一个任务中，以减少线程间的数据传输
        this.chainingStrategy = ChainingStrategy.DEFAULT_CHAINING_STRATEGY;
    }

    @Override
    public void open() throws Exception {
        super.open();

        timestampAssigner = watermarkStrategy.createTimestampAssigner(this::getMetricGroup);
        // emitProgressiveWatermarks默认为true
        watermarkGenerator = emitProgressiveWatermarks
                ? watermarkStrategy.createWatermarkGenerator(this::getMetricGroup)
                : new NoWatermarksGenerator<>();

        // 将output封装为WatermarkEmitter
        wmOutput = new WatermarkEmitter(output);

        // 默认值是200毫秒
        watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();
        // 如果更新周期大于0，且emitProgressiveWatermarks为true，则注册一个定时器
        if (watermarkInterval > 0 && emitProgressiveWatermarks) {
            // 这里其实就是获取当前系统时间
            final long now = getProcessingTimeService().getCurrentProcessingTime();
            // 注册一个定时器，定时器会在watermarkInterval毫秒后触发，调用当前类的onProcessingTime方法
            getProcessingTimeService().registerTimer(now + watermarkInterval, this);
        }
    }

    @Override
    public void processElement(final StreamRecord<T> element) throws Exception {
        // 每条数据的处理都会经过该方法
        final T event = element.getValue();
        final long previousTimestamp = element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE;
        // 调用我们自己自定义的timestampAssigner的extractTimestamp获取当前的水位时间
        final long newTimestamp = timestampAssigner.extractTimestamp(event, previousTimestamp);

        // 并将时间更新设置到StreamRecord中
        element.setTimestamp(newTimestamp);
        output.collect(element);
        // 更新水位线
        watermarkGenerator.onEvent(event, newTimestamp, wmOutput);
    }

    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        // 这里最终会调用wmOutput的emitWatermark方法，更新水位线
        watermarkGenerator.onPeriodicEmit(wmOutput);

        final long now = getProcessingTimeService().getCurrentProcessingTime();
        // 注册一个定时器，定时器会在watermarkInterval毫秒后触发，调用当前类的onProcessingTime方法
        getProcessingTimeService().registerTimer(now + watermarkInterval, this);
    }

    /**
     * Override the base implementation to completely ignore watermarks propagated from upstream,
     * except for the "end of time" watermark.
     */
    @Override
    public void processWatermark(org.apache.flink.streaming.api.watermark.Watermark mark) throws Exception {
        // if we receive a Long.MAX_VALUE watermark we forward it since it is used
        // to signal the end of input and to not block watermark progress downstream
        if (mark.getTimestamp() == Long.MAX_VALUE) {
            wmOutput.emitWatermark(Watermark.MAX_WATERMARK);
        }
    }

    /** Override the base implementation to completely ignore statuses propagated from upstream. */
    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        watermarkGenerator.onPeriodicEmit(wmOutput);
    }

    // ------------------------------------------------------------------------

    /**
     * Implementation of the {@code WatermarkEmitter}, based on the components that are available
     * inside a stream operator.
     */
    public static final class WatermarkEmitter implements WatermarkOutput {

        private final Output<?> output;

        private long currentWatermark;

        private boolean idle;

        public WatermarkEmitter(Output<?> output) {
            this.output = output;
            this.currentWatermark = Long.MIN_VALUE;
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            // 当前最新的水位
            final long ts = watermark.getTimestamp();

            // 如果传入的水位比当前水位还低直接退出
            if (ts <= currentWatermark) {
                return;
            }
            // 更新当前水位为传入的最新水位
            currentWatermark = ts;
            // 判断如果当前的idle为true，则将其设置false，并更新Output中的WatermarkStatus为ACTIVE
            markActive();

            output.emitWatermark(new org.apache.flink.streaming.api.watermark.Watermark(ts));
        }

        @Override
        public void markIdle() {
            if (!idle) {
                idle = true;
                output.emitWatermarkStatus(WatermarkStatus.IDLE);
            }
        }

        @Override
        public void markActive() {
            if (idle) {
                idle = false;
                output.emitWatermarkStatus(WatermarkStatus.ACTIVE);
            }
        }
    }
}
