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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.TaskEventHandler;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.util.event.EventListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The task event dispatcher dispatches events flowing backwards from a consuming task to the task
 * producing the consumed result.
 *
 * <p>Backwards events only work for tasks, which produce pipelined results, where both the
 * producing and consuming task are running at the same time.
 */
public class TaskEventDispatcher implements TaskEventPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(TaskEventDispatcher.class);

    private final Map<ResultPartitionID, TaskEventHandler> registeredHandlers = new HashMap<>();

    /**
     * Registers the given partition for incoming task events allowing calls to {@link
     * #subscribeToEvent(ResultPartitionID, EventListener, Class)}.
     *
     * @param partitionId the partition ID
     */
    public void registerPartition(ResultPartitionID partitionId) {
        checkNotNull(partitionId);
        /**
         * 负责在Task内部的不同组件之间分发事件，这些事件通常不是用户定义的业务事件，而是Flink运行时系统使用的事件，
         * 例如用于协调数据交换、状态迁移、检查点或其他控制逻辑
         *
         * 在任务执行过程中，任务之间的数据通过网络传输InputChannel和ResultPartition，TaskEventDispatcher可以用于协调这些数据通道
         * 例如当下游任务需要通知上游重新发送数据或重新配置时，可以通过TaskEventDispatcher分发这些事件。
         *
         * 一些运行时控制逻辑（如检查点、故障恢复、流暂停和恢复等）需要在任务的不同组件之间发送控制信号，这些信号通常是以TaskEvent的形式分发的
         *
         * 用户可以通过扩展TaskEvent，在运行时逻辑中添加自定义事件处理。例如在某些高级场景下，用户可能需要实现自己的事件流控制逻辑
         */
        synchronized (registeredHandlers) {
            LOG.debug("registering {}", partitionId);
            if (registeredHandlers.put(partitionId, new TaskEventHandler()) != null) {
                throw new IllegalStateException("Partition " + partitionId
                        + " already registered at task event dispatcher.");
            }
        }
    }

    /**
     * Removes the given partition from listening to incoming task events, thus forbidding calls to
     * {@link #subscribeToEvent(ResultPartitionID, EventListener, Class)}.
     *
     * @param partitionId the partition ID
     */
    public void unregisterPartition(ResultPartitionID partitionId) {
        checkNotNull(partitionId);

        synchronized (registeredHandlers) {
            LOG.debug("unregistering {}", partitionId);
            // NOTE: tolerate un-registration of non-registered task (unregister is always called
            //       in the cleanup phase of a task even if it never came to the registration - see
            //       Task.java)
            registeredHandlers.remove(partitionId);
        }
    }

    /**
     * Subscribes a listener to this dispatcher for events on a partition.
     *
     * @param partitionId ID of the partition to subscribe for (must be registered via {@link
     *         #registerPartition(ResultPartitionID)} first!)
     * @param eventListener the event listener to subscribe
     * @param eventType event type to subscribe to
     */
    public void subscribeToEvent(
            ResultPartitionID partitionId,
            EventListener<TaskEvent> eventListener,
            Class<? extends TaskEvent> eventType) {
        checkNotNull(partitionId);
        checkNotNull(eventListener);
        checkNotNull(eventType);

        TaskEventHandler taskEventHandler;
        synchronized (registeredHandlers) {
            taskEventHandler = registeredHandlers.get(partitionId);
        }
        if (taskEventHandler == null) {
            throw new IllegalStateException(
                    "Partition " + partitionId + " not registered at task event dispatcher.");
        }
        taskEventHandler.subscribe(eventListener, eventType);
    }

    /**
     * Publishes the event to the registered {@link EventListener} instances.
     *
     * <p>This method is either called directly from a {@link LocalInputChannel} or the network I/O
     * thread on behalf of a {@link RemoteInputChannel}.
     *
     * @return whether the event was published to a registered event handler (initiated via {@link
     *         #registerPartition(ResultPartitionID)}) or not
     */
    @Override
    public boolean publish(ResultPartitionID partitionId, TaskEvent event) {
        checkNotNull(partitionId);
        checkNotNull(event);

        TaskEventHandler taskEventHandler;
        synchronized (registeredHandlers) {
            taskEventHandler = registeredHandlers.get(partitionId);
        }

        if (taskEventHandler != null) {
            taskEventHandler.publish(event);
            return true;
        }

        return false;
    }

    /** Removes all registered event handlers. */
    public void clearAll() {
        synchronized (registeredHandlers) {
            registeredHandlers.clear();
        }
    }
}
