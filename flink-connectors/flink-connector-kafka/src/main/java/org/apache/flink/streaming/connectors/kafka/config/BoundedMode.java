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

package org.apache.flink.streaming.connectors.kafka.config;

import org.apache.flink.annotation.Internal;

/**
 * Kafka 消费者的结束模式
 * End modes for the Kafka Consumer.
 */
@Internal
public enum BoundedMode {

    /**
     * 不停止消费
     * Do not end consuming.
     */
    UNBOUNDED,

    /**
     *
     * 从特定消费者组在 Zookeeper（ZK）或 Kafka broker 中的已提交偏移量开始结束消费
     * 这一操作会在开始消费指定分区时进行评估。
     *
     * End from committed offsets in ZK / Kafka brokers of a specific consumer group. This is
     * evaluated at the start of consumption from a given partition.
     */
    GROUP_OFFSETS,

    /**
     * End from the latest offset. This is evaluated at the start of consumption from a given
     * partition.
     */
    LATEST,

    /** End from user-supplied timestamp for each partition. */
    TIMESTAMP,

    /**
     * End from user-supplied specific offsets for each partition. If an offset for a partition is
     * not provided it will not consume from that partition.
     */
    SPECIFIC_OFFSETS;
}
