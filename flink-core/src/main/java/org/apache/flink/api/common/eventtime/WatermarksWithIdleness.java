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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A WatermarkGenerator that adds idleness detection to another WatermarkGenerator. If no events
 * come within a certain time (timeout duration) then this generator marks the stream as idle, until
 * the next watermark is generated.
 */
@Public
public class WatermarksWithIdleness<T> implements WatermarkGenerator<T> {

    private final WatermarkGenerator<T> watermarks;

    private final IdlenessTimer idlenessTimer;

    private boolean isIdleNow = false;

    /**
     * Creates a new WatermarksWithIdleness generator to the given generator idleness detection with
     * the given timeout.
     *
     * @param watermarks The original watermark generator.
     * @param idleTimeout The timeout for the idleness detection.
     */
    public WatermarksWithIdleness(WatermarkGenerator<T> watermarks, Duration idleTimeout) {
        this(watermarks, idleTimeout, SystemClock.getInstance());
    }

    @VisibleForTesting
    WatermarksWithIdleness(WatermarkGenerator<T> watermarks, Duration idleTimeout, Clock clock) {
        checkNotNull(idleTimeout, "idleTimeout");
        checkArgument(
                !(idleTimeout.isZero() || idleTimeout.isNegative()),
                "idleTimeout must be greater than zero");
        this.watermarks = checkNotNull(watermarks, "watermarks");
        this.idlenessTimer = new IdlenessTimer(clock, idleTimeout);
    }

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        // output是将splitId生成的对应的PartialWatermark封装成ImmediateOutput
        // 将事件传递给原始的水位线生成器，用于提取时间戳和生成水位线
        watermarks.onEvent(event, eventTimestamp, output);
        // 当有数据触发时就将counter++，每条数据都会调用该方法
        idlenessTimer.activity();
        isIdleNow = false;
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 周期性调用该方法，检查是否有空闲，判断逻辑时如果在两次执行onPeriodicEmit中间执行了onEvent，就会返回false
        // 否则会在没有执行onEvent的第一次onPeriodicEmit时重置时间，当一直没有调用onEvent就会导致当前时间大于第一次onPeriodicEmit时间
        // 从而导致返回true，如果为true，就不再是更新水位线
        if (idlenessTimer.checkIfIdle()) {
            if (!isIdleNow) {
                // 将当前Output标记为idle状态，即空闲状态
                output.markIdle();
                isIdleNow = true;
            }
        } else {
            watermarks.onPeriodicEmit(output);
        }
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    static final class IdlenessTimer {

        /** The clock used to measure elapsed time. */
        private final Clock clock;

        /** Counter to detect change. No problem if it overflows. */
        private long counter;

        /** The value of the counter at the last activity check. */
        private long lastCounter;

        /**
         * The first time (relative to {@link Clock#relativeTimeNanos()}) when the activity check
         * found that no activity happened since the last check. Special value: 0 = no timer.
         */
        private long startOfInactivityNanos;

        /** The duration before the output is marked as idle. */
        private final long maxIdleTimeNanos;

        IdlenessTimer(Clock clock, Duration idleTimeout) {
            this.clock = clock;

            long idleNanos;
            try {
                // 转换为纳秒
                idleNanos = idleTimeout.toNanos();
            } catch (ArithmeticException ignored) {
                // long integer overflow
                idleNanos = Long.MAX_VALUE;
            }
            // 转换为纳秒，设置为maxIdleTimeNanos
            this.maxIdleTimeNanos = idleNanos;
        }

        public void activity() {
            counter++;
        }

        public boolean checkIfIdle() {
            if (counter != lastCounter) {
                // activity since the last check. we reset the timer
                lastCounter = counter;
                // 如果lastCounter与counter不相等，说明有活动发生，需要重置startOfInactivityNanos为0
                startOfInactivityNanos = 0L;
                return false;
            } else // timer started but has not yet reached idle timeout
            if (startOfInactivityNanos == 0L) {
                // first time that we see no activity since the last periodic probe begin the timer
                // 第一次会将startOfInactivityNanos设置为当前系统时间
                startOfInactivityNanos = clock.relativeTimeNanos();
                return false;
            } else {
                // 当前系统时间 - startOfInactivityNanos如果大于maxIdleTimeNanos返回ture
                return clock.relativeTimeNanos() - startOfInactivityNanos > maxIdleTimeNanos;
            }
        }
    }
}
