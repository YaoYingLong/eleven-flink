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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An adapter that exposes a {@link WatermarkOutput} based on a {@link
 * PushingAsyncDataInput.DataOutput}.
 */
@Internal
public final class WatermarkToDataOutput implements WatermarkOutput {

    private final PushingAsyncDataInput.DataOutput<?> output;
    private final TimestampsAndWatermarks.WatermarkUpdateListener watermarkEmitted;
    private long maxWatermarkSoFar;
    private boolean isIdle;

    @VisibleForTesting
    public WatermarkToDataOutput(PushingAsyncDataInput.DataOutput<?> output) {
        this(
                output,
                new TimestampsAndWatermarks.WatermarkUpdateListener() {
                    @Override
                    public void updateIdle(boolean isIdle) {}

                    @Override
                    public void updateCurrentEffectiveWatermark(long watermark) {}

                    @Override
                    public void updateCurrentSplitWatermark(String splitId, long watermark) {}
                });
    }

    /** Creates a new WatermarkOutput against the given DataOutput. */
    public WatermarkToDataOutput(
            PushingAsyncDataInput.DataOutput<?> output,
            TimestampsAndWatermarks.WatermarkUpdateListener watermarkEmitted) {
        // 如果是KafkaSource这里的output是AsyncDataOutputToOutput
        // AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
        this.output = checkNotNull(output);
        // 传入的watermarkEmitted其实就是SourceOperator，其实现了WatermarkUpdateListener接口
        this.watermarkEmitted = checkNotNull(watermarkEmitted);
        this.maxWatermarkSoFar = Long.MIN_VALUE;
    }

    @Override
    public void emitWatermark(Watermark watermark) {
        final long newWatermark = watermark.getTimestamp();
        // maxWatermarkSoFar初始值为Long.MIN_VALUE
        if (newWatermark <= maxWatermarkSoFar) {
            return;
        }
        // 将当前水位线赋值给maxWatermarkSoFar
        maxWatermarkSoFar = newWatermark;
        // 调用SourceOperator的updateCurrentEffectiveWatermark方法，更新latestWatermark
        watermarkEmitted.updateCurrentEffectiveWatermark(maxWatermarkSoFar);

        try {
            // 更新算子和Subtask的空闲状态
            markActiveInternally();
            // 如果是KafkaSource这里的output是AsyncDataOutputToOutput
            // AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
            output.emitWatermark(new org.apache.flink.streaming.api.watermark.Watermark(newWatermark));
        } catch (ExceptionInChainedOperatorException e) {
            throw e;
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }

    @Override
    public void markIdle() {
        if (isIdle) {
            return;
        }

        try {
            output.emitWatermarkStatus(WatermarkStatus.IDLE);
            watermarkEmitted.updateIdle(true);
            isIdle = true;
        } catch (ExceptionInChainedOperatorException e) {
            throw e;
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }

    @Override
    public void markActive() {
        try {
            markActiveInternally();
        } catch (ExceptionInChainedOperatorException e) {
            throw e;
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }

    private boolean markActiveInternally() throws Exception {
        // 如果当前是非空闲状态，则不需要再次发出活动状态
        if (!isIdle) {
            return true;
        }
        // 发送活动状态
        // 如果是KafkaSource这里的output是AsyncDataOutputToOutput
        // AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
        output.emitWatermarkStatus(WatermarkStatus.ACTIVE);
        // 更新SourceOperator的状态
        watermarkEmitted.updateIdle(false);
        isIdle = false;
        return false;
    }
}
