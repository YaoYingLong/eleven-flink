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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * @param <IN> The type of the record that can be read with this record reader.
 */
@Internal
public final class StreamOneInputProcessor<IN> implements StreamInputProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(StreamOneInputProcessor.class);

    private StreamTaskInput<IN> input;
    private DataOutput<IN> output;

    private final BoundedMultiInput endOfInputAware;

    public StreamOneInputProcessor(
            StreamTaskInput<IN> input, DataOutput<IN> output, BoundedMultiInput endOfInputAware) {
        // 这里的input是StreamTaskSourceInput
        this.input = checkNotNull(input);
        // 如果是KafkaSource这里的output是AsyncDataOutputToOutput
        // AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
        this.output = checkNotNull(output);
        // 这里的endOfInputAware是OperatorChain
        this.endOfInputAware = checkNotNull(endOfInputAware);
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        // 只要operator有待处理的数据或者没有等待水位对齐，就返回AVAILABLE
        return input.getAvailableFuture();
    }

    @Override
    public DataInputStatus processInput() throws Exception {

        /**
         * 如果是KafkaSource这里的input是StreamTaskSourceInput，output是AsyncDataOutputToOutput
         * AsyncDataOutputToOutput是对ChainingOutput或RecordWriterOutput进行了一次封装
         *
         * 如果是非KafkaSource这里的input一般是StreamTaskNetworkInput，output是StreamTaskNetworkOutput
         * 但是StreamTaskNetworkInput并没有实现emitNext方法，这里调用的是超类AbstractStreamTaskNetworkInput的emitNext方法
         */
        DataInputStatus status = input.emitNext(output);

        if (status == DataInputStatus.END_OF_DATA) {
            // 只有批处理才会执行相关的逻辑
            endOfInputAware.endInput(input.getInputIndex() + 1);
            output = new FinishedDataOutput<>();
        } else if (status == DataInputStatus.END_OF_RECOVERY) {
            if (input instanceof RecoverableStreamTaskInput) {
                input = ((RecoverableStreamTaskInput<IN>) input).finishRecovery();
            }
            return DataInputStatus.MORE_AVAILABLE;
        }

        return status;
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        return input.prepareSnapshot(channelStateWriter, checkpointId);
    }

    @Override
    public void close() throws IOException {
        input.close();
    }
}
