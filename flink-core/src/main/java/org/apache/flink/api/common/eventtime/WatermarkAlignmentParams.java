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

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/** Configuration parameters for watermark alignment. */
@PublicEvolving
public final class WatermarkAlignmentParams implements Serializable {
    // 默认是禁用水位线对齐
    public static final WatermarkAlignmentParams WATERMARK_ALIGNMENT_DISABLED =
            new WatermarkAlignmentParams(Long.MAX_VALUE, "", 0);
    // 最大允许的水位线的差值，即最小的水位线和最大的水位线的差
    private final long maxAllowedWatermarkDrift;
    // 更新周期&上报周期，不设置默认是1s
    private final long updateInterval;
    // 分组
    private final String watermarkGroup;

    public WatermarkAlignmentParams(
            long maxAllowedWatermarkDrift, String watermarkGroup, long updateInterval) {
        this.maxAllowedWatermarkDrift = maxAllowedWatermarkDrift;
        // 更新周期&上报周期，不设置默认是1s
        this.watermarkGroup = watermarkGroup;
        this.updateInterval = updateInterval;
    }

    public boolean isEnabled() {
        return maxAllowedWatermarkDrift < Long.MAX_VALUE;
    }

    public long getMaxAllowedWatermarkDrift() {
        return maxAllowedWatermarkDrift;
    }

    public String getWatermarkGroup() {
        return watermarkGroup;
    }

    public long getUpdateInterval() {
        return updateInterval;
    }
}
