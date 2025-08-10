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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;

/**
 * This mode decides the default {@link ResultPartitionType} of job edges. Note that this only
 * affects job edges which are {@link StreamExchangeMode#UNDEFINED}.
 */
@Internal
public enum GlobalStreamExchangeMode {
    /**
     * Set all job edges to be {@link ResultPartitionType#BLOCKING}.
     *
     * 所有的边都以阻塞的方式进行数据交换，下游算子会等待上游算子完成数据的发送后，才能开始处理数据
     */
    ALL_EDGES_BLOCKING,

    /**
     * Set job edges with {@link ForwardPartitioner} to be {@link
     * ResultPartitionType#PIPELINED_BOUNDED} and other edges to be {@link
     * ResultPartitionType#BLOCKING}.
     *
     * 所有的 Forward 边（即点对点数据传输）都以流水线方式（pipelined）进行数据交换
     * 下游算子可以立即开始处理来自上游算子的输出数据，而无需等待上游算子完成
     */
    FORWARD_EDGES_PIPELINED,

    /**
     * Set job edges with {@link ForwardPartitioner} or {@link RescalePartitioner} to be {@link
     * ResultPartitionType#PIPELINED_BOUNDED} and other edges to be {@link
     * ResultPartitionType#BLOCKING}.
     *
     * 所有的点对点边（pointwise edges）都以流水线方式进行数据交换
     * 点对点边表示上游算子中的每个分区与下游算子中的一个或多个分区直接对应
     */
    POINTWISE_EDGES_PIPELINED,

    /**
     * Set all job edges {@link ResultPartitionType#PIPELINED_BOUNDED}.
     *
     * 所有的边（edges）都以流水线方式进行数据交换，这是一种完全流式的执行模式
     */
    ALL_EDGES_PIPELINED,

    /**
     * Set all job edges {@link ResultPartitionType#PIPELINED_APPROXIMATE}.
     *
     * 所有的边都以近似流水线的方式进行数据交换
     * 是 ALL_EDGES_PIPELINED 的一种变体，可能允许一些优化或妥协（如部分缓冲）
     */
    ALL_EDGES_PIPELINED_APPROXIMATE,

    /**
     * Set all job edges {@link ResultPartitionType#HYBRID_FULL}.
     *
     * 所有的边都使用混合模式（hybrid mode），具体行为由 Flink 的调度器和运行时环境决定
     * Flink 会根据资源情况、数据规模等因素动态选择使用阻塞模式或流水线模式
     */
    ALL_EDGES_HYBRID_FULL,

    /**
     * Set all job edges {@link ResultPartitionType#HYBRID_SELECTIVE}.
     *
     * 与ALL_EDGES_HYBRID_FULL类似，但这种模式更倾向于在某些特定条件下使用混合模式，如只针对部分边使用流水线或阻塞
     * Flink 可能只对关键路径上的边使用流水线模式，而对其他边使用阻塞模式
     */
    ALL_EDGES_HYBRID_SELECTIVE
}
