/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.graph.StreamGraph;

/** The data exchange mode between operators during {@link StreamGraph} generation. */
@Internal
public enum StreamExchangeMode {
    /**
     * 生产者和消费者同时在线。 产生的数据将立即被消费者接收。
     *
     * Producer and consumer are online at the same time. Produced data is received by consumer
     * immediately.
     */
    PIPELINED,

    /**
     * 生产者首先产生其全部结果并完成。 之后，使用者将启动并可以使用数据。
     *
     * The producer first produces its entire result and finishes. After that, the consumer is
     * started and may consume the data.
     */
    BATCH,

    /**
     * 消费者可以在生产者开始生产数据后随时开始消费数据，这种交换模式是可重新消费的
     *
     * The consumer can start consuming data anytime as long as the producer has started producing.
     *
     * <p>This exchange mode is re-consumable.
     */
    HYBRID_FULL,

    /**
     * 消费者可以在生产者开始生产数据后随时开始消费数据，这种交换模式不可重新消费
     *
     * The consumer can start consuming data anytime as long as the producer has started producing.
     *
     * <p>This exchange mode is not re-consumable.
     */
    HYBRID_SELECTIVE,

    /**
     * shuffle mode 未定义。它留给框架来决定随机播放模式
     * 框架最后将选择{@link StreamExchangeMode＃BATCH}或{@link StreamExchangeMode＃PIPELINED}中的一个
     * 如果没决定，则由Flink框架自行决定使用 Batch 或者 Pipeline
     *
     * The exchange mode is undefined. It leaves it up to the framework to decide the exchange mode.
     * The framework will pick one of {@link StreamExchangeMode#BATCH} or {@link
     * StreamExchangeMode#PIPELINED} in the end.
     */
    UNDEFINED
}
