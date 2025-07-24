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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Defines the chaining scheme for the operator. When an operator is chained to the predecessor, it
 * means that they run in the same thread. They become one operator consisting of multiple steps.
 *
 * <p>The default value used by the StreamOperator is {@link #HEAD}, which means that the operator
 * is not chained to its predecessor. Most operators override this with {@link #ALWAYS}, meaning
 * they will be chained to predecessors whenever possible.
 *
 * 用于定义 Flink 算子（Operator）之间的链式操作策略。链式操作（Chaining）是 Flink 中的一种优化技术
 * 通过将多个算子合并到同一个线程中运行，减少线程间的数据传输开销，从而提高性能
 *
 */
@PublicEvolving
public enum ChainingStrategy {

    /**
     * 表示总是尝试将当前算子与上游算子链在一起，Flink 会尽可能地将多个算子合并到一个任务中，以减少线程间的数据传输
     *
     * Operators will be eagerly chained whenever possible.
     *
     * <p>To optimize performance, it is generally a good practice to allow maximal chaining and
     * increase operator parallelism.
     */
    ALWAYS,

    /**
     * 表示当前算子永远不会与上游算子链在一起，当前算子会单独运行在自己的任务线程中
     *
     * The operator will not be chained to the preceding or succeeding operators.
     */
    NEVER,

    /**
     * 表示当前算子是一个链的起点（链的头部），当前算子不会与上游算子链在一起，但下游算子可以与它链在一起
     *
     * The operator will not be chained to the predecessor, but successors may chain to this
     * operator.
     */
    HEAD,

    /**
     * 这个算子将作为一个链的起点运行，但它还会尝试在可能的情况下将源（source）输入进行链式操作
     * 这使得多输入算子可以与多个源算子链在一起，合并为一个任务（task）运行
     *
     * This operator will run at the head of a chain (similar as in {@link #HEAD}, but it will
     * additionally try to chain source inputs if possible. This allows multi-input operators to be
     * chained with multiple sources into one task.
     */
    HEAD_WITH_SOURCES;

    public static final ChainingStrategy DEFAULT_CHAINING_STRATEGY = ALWAYS;
}
