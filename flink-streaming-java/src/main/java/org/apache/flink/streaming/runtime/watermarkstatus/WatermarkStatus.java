/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.watermarkstatus;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * A Watermark Status element informs stream tasks whether or not they should continue to expect
 * watermarks from the input stream that sent them. There are 2 kinds of status, namely {@link
 * WatermarkStatus#IDLE} and {@link WatermarkStatus#ACTIVE}. Watermark Status elements are generated
 * at the sources, and may be propagated through the tasks of the topology. They directly infer the
 * current status of the emitting task; a {@link SourceStreamTask} or {@link StreamTask} emits a
 * {@link WatermarkStatus#IDLE} if it will temporarily halt to emit any watermarks (i.e. is idle),
 * and emits a {@link WatermarkStatus#ACTIVE} once it resumes to do so (i.e. is active). Tasks are
 * responsible for propagating their status further downstream once they toggle between being idle
 * and active. The cases that source tasks and downstream tasks are considered either idle or active
 * is explained below:
 *
 * <ul>
 *   <li>Source tasks: A source task is considered to be idle if its head operator, i.e. a {@link
 *       StreamSource}, will not emit watermarks for an indefinite amount of time. This is the case,
 *       for example, for Flink's Kafka Consumer, where sources might initially have no assigned
 *       partitions to read from, or no records can be read from the assigned partitions. Once the
 *       head {@link StreamSource} operator detects that it will resume emitting data, the source
 *       task is considered to be active. {@link StreamSource}s are responsible for toggling the
 *       status of the containing source task and ensuring that no watermarks will be emitted while
 *       the task is idle. This guarantee should be enforced on sources through {@link
 *       org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext}
 *       implementations.
 *   <li>Downstream tasks: a downstream task is considered to be idle if all its input streams are
 *       idle, i.e. the last received Watermark Status element from all input streams is a {@link
 *       WatermarkStatus#IDLE}. As long as one of its input streams is active, i.e. the last
 *       received Watermark Status element from the input stream is {@link WatermarkStatus#ACTIVE},
 *       the task is active.
 * </ul>
 *
 * <p>Watermark Status elements received at downstream tasks also affect and control how their
 * operators process and advance their watermarks. The below describes the effects (the logic is
 * implemented as a {@link StatusWatermarkValve} which downstream tasks should use for such
 * purposes):
 *
 * <ul>
 *   <li>Since there may be watermark generators that might produce watermarks anywhere in the
 *       middle of topologies regardless of whether there are input data at the operator, the
 *       current status of the task must be checked before forwarding watermarks emitted from an
 *       operator. If the status is actually idle, the watermark must be blocked.
 *   <li>For downstream tasks with multiple input streams, the watermarks of input streams that are
 *       temporarily idle, or has resumed to be active but its watermark is behind the overall min
 *       watermark of the operator, should not be accounted for when deciding whether or not to
 *       advance the watermark and propagated through the operator chain.
 * </ul>
 *
 * <p>Note that to notify downstream tasks that a source task is permanently closed and will no
 * longer send any more elements, the source should still send a {@link Watermark#MAX_WATERMARK}
 * instead of {@link WatermarkStatus#IDLE}. Watermark Status elements only serve as markers for
 * temporary status.
 *
 * 水印状态元素用于通知流任务（stream tasks）是否应该继续期待来自输入流发送的水印。水印状态分为两种类型，
 * 分别是 {@link WatermarkStatus#IDLE} 和 {@link WatermarkStatus#ACTIVE}。水印状态元素在数据源处生成，
 * 并可以在拓扑中的任务间传播。它直接反映了当前任务的状态：如果一个 {@link SourceStreamTask} 或 {@link StreamTask}
 * 暂时停止发送任何水印（即处于空闲状态），它将发送 {@link WatermarkStatus#IDLE}；一旦任务恢复发送水印（即处于活跃状态），
 * 它将发送 {@link WatermarkStatus#ACTIVE}。任务在状态切换（空闲与活跃之间）时负责将其状态进一步传播到下游。
 *
 * 源任务被认为是空闲状态的条件：如果其头部操作符（即 {@link StreamSource}）在一段时间内不会发送任何水印。
 * 例如，对于 Flink 的 Kafka 消费者（Kafka Consumer），当源任务没有分配任何分区读取数据，或无法从分配的分区中读取记录时，
 * 任务会被认为是空闲状态。
 *
 * 当头部的 {@link StreamSource} 操作符检测到将恢复发送数据时，源任务会被认为是活跃状态
 *
 * {@link StreamSource} 负责切换包含的源任务的状态，并确保在任务处于空闲状态时不会发送任何水印。
 * 此保证应通过 {@link org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext}
 * 的实现在源任务中强制执行。
 *
 */
@Internal
public final class WatermarkStatus extends StreamElement {

    // 空闲状态
    public static final int IDLE_STATUS = -1;
    // 活跃状态
    public static final int ACTIVE_STATUS = 0;

    public static final WatermarkStatus IDLE = new WatermarkStatus(IDLE_STATUS);
    public static final WatermarkStatus ACTIVE = new WatermarkStatus(ACTIVE_STATUS);

    public final int status;

    public WatermarkStatus(int status) {
        if (status != IDLE_STATUS && status != ACTIVE_STATUS) {
            throw new IllegalArgumentException(
                    "Invalid status value for WatermarkStatus; "
                            + "allowed values are "
                            + ACTIVE_STATUS
                            + " (for ACTIVE) and "
                            + IDLE_STATUS
                            + " (for IDLE).");
        }

        this.status = status;
    }

    public boolean isIdle() {
        return this.status == IDLE_STATUS;
    }

    public boolean isActive() {
        return !isIdle();
    }

    public int getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        return this == o
                || o != null
                        && o.getClass() == WatermarkStatus.class
                        && ((WatermarkStatus) o).status == this.status;
    }

    @Override
    public int hashCode() {
        return status;
    }

    @Override
    public String toString() {
        String statusStr = (status == ACTIVE_STATUS) ? "ACTIVE" : "IDLE";
        return "WatermarkStatus(" + statusStr + ")";
    }
}
