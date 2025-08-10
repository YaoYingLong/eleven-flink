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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Implementation of the {@link TaskManagerGateway} for Flink's RPC system. */
public class RpcTaskManagerGateway implements TaskManagerGateway {

    private final TaskExecutorGateway taskExecutorGateway;

    private final JobMasterId jobMasterId;

    public RpcTaskManagerGateway(TaskExecutorGateway taskExecutorGateway, JobMasterId jobMasterId) {
        this.taskExecutorGateway = Preconditions.checkNotNull(taskExecutorGateway);
        this.jobMasterId = Preconditions.checkNotNull(jobMasterId);
    }

    @Override
    public String getAddress() {
        return taskExecutorGateway.getAddress();
    }

    @Override
    public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
        // 这里的taskExecutorGateway其实就是TaskExecutor，调用的是TaskExecutor的submitTask方法，
        // 提交到对应的slot所在节点的TaskExecutor中来执行该ExecutionVertex，其实已经变成：Task
        // 关于Client提交Job到最后变成分布式Task物理执行图的所有细节到此为止，结束了。
        // 从这以后，就是去到了TaskManager中的TaskExecutor中来执行Task了
        // Flink 每个Task 发布的时候，单独启动一个线程来执行
        return taskExecutorGateway.submitTask(tdd, jobMasterId, timeout);

    }

    @Override
    public CompletableFuture<Acknowledge> cancelTask(
            ExecutionAttemptID executionAttemptID, Time timeout) {
        return taskExecutorGateway.cancelTask(executionAttemptID, timeout);
    }

    @Override
    public CompletableFuture<Acknowledge> updatePartitions(
            ExecutionAttemptID executionAttemptID,
            Iterable<PartitionInfo> partitionInfos,
            Time timeout) {
        return taskExecutorGateway.updatePartitions(executionAttemptID, partitionInfos, timeout);
    }

    @Override
    public void releasePartitions(JobID jobId, Set<ResultPartitionID> partitionIds) {
        taskExecutorGateway.releasePartitions(jobId, partitionIds);
    }

    @Override
    public void notifyCheckpointOnComplete(
            ExecutionAttemptID executionAttemptID,
            JobID jobId,
            long completedCheckpointId,
            long completedTimestamp,
            long lastSubsumedCheckpointId) {
        taskExecutorGateway.confirmCheckpoint(
                executionAttemptID,
                completedCheckpointId,
                completedTimestamp,
                lastSubsumedCheckpointId);
    }

    @Override
    public void notifyCheckpointAborted(
            ExecutionAttemptID executionAttemptID,
            JobID jobId,
            long checkpointId,
            long latestCompletedCheckpointId,
            long timestamp) {
        taskExecutorGateway.abortCheckpoint(
                executionAttemptID, checkpointId, latestCompletedCheckpointId, timestamp);
    }

    @Override
    public CompletableFuture<Acknowledge> triggerCheckpoint(
            ExecutionAttemptID executionAttemptID,
            JobID jobId,
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions) {
        return taskExecutorGateway.triggerCheckpoint(
                executionAttemptID, checkpointId, timestamp, checkpointOptions);
    }

    @Override
    public CompletableFuture<Acknowledge> freeSlot(
            AllocationID allocationId, Throwable cause, Time timeout) {
        return taskExecutorGateway.freeSlot(allocationId, cause, timeout);
    }

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToTask(
            ExecutionAttemptID task, OperatorID operator, SerializedValue<OperatorEvent> evt) {
        return taskExecutorGateway.sendOperatorEventToTask(task, operator, evt);
    }
}
