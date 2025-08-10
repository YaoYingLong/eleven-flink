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

package org.apache.flink.connector.kafka.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/** The {@link RecordEmitter} implementation for {@link KafkaSourceReader}. */
@Internal
public class KafkaRecordEmitter<T>
        implements RecordEmitter<ConsumerRecord<byte[], byte[]>, T, KafkaPartitionSplitState> {

    private final KafkaRecordDeserializationSchema<T> deserializationSchema;
    private final SourceOutputWrapper<T> sourceOutputWrapper = new SourceOutputWrapper<>();

    public KafkaRecordEmitter(KafkaRecordDeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void emitRecord(
            ConsumerRecord<byte[], byte[]> consumerRecord, SourceOutput<T> output,
            KafkaPartitionSplitState splitState) throws Exception {
        try {
            // 将SourceOutput设置到SourceOutputWrapper中，这里的output是SourceOutputWithWatermarks
            sourceOutputWrapper.setSourceOutput(output);
            // 将当前ConsumerRecord的时间戳设置到SourceOutputWrapper中
            sourceOutputWrapper.setTimestamp(consumerRecord.timestamp());
            // 使用反序列化Schema将ConsumerRecord反序列化为输出类型T，调用sourceOutputWrapper的collect方法
            deserializationSchema.deserialize(consumerRecord, sourceOutputWrapper);
            // 更新splitState的当前偏移量
            splitState.setCurrentOffset(consumerRecord.offset() + 1);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize consumer record due to", e);
        }
    }

    private static class SourceOutputWrapper<T> implements Collector<T> {

        private SourceOutput<T> sourceOutput;
        private long timestamp;

        @Override
        public void collect(T record) {
            // 将SourceOutput设置到SourceOutputWrapper中，这里的output是SourceOutputWithWatermarks
            sourceOutput.collect(record, timestamp);
        }

        @Override
        public void close() {
        }

        private void setSourceOutput(SourceOutput<T> sourceOutput) {
            this.sourceOutput = sourceOutput;
        }

        private void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
