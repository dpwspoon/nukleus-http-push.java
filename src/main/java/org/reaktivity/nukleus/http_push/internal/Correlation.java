/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.http_push.internal;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.reaktivity.nukleus.buffer.BufferPool;

public class Correlation {

    private final String connectSource;
    private final int slotIndex;
    private final int slotLimit;
    private long connectCorrelation;
    private final BufferPool bufferPool;

    public Correlation(
        String connectSource,
        BufferPool bufferPool,
        int slotIndex,
        int slotLimit,
        long connectCorrelation)
    {
        this.connectSource = requireNonNull(connectSource);
        this.slotIndex = slotIndex;
        this.slotLimit = slotLimit;
        this.connectCorrelation = connectCorrelation;
        this.bufferPool = bufferPool;
    }

    public String connectSource()
    {
        return connectSource;
    }

    public long connectCorrelation()
    {
        return connectCorrelation;
    }

    public int slotIndex() {
        return slotIndex;
    }

    public int slotLimit()
    {
        return slotLimit;
    }

    public BufferPool bufferPool() {
        return bufferPool;
    }

    @Override
    public int hashCode()
    {
        int result = Long.hashCode(connectCorrelation);
        result = 31 * connectSource.hashCode();
        result = 31 * slotIndex;
        result = 31 * slotLimit;
        result = 31 * bufferPool.hashCode();

        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof Correlation))
        {
            return false;
        }

        Correlation that = (Correlation) obj;
        return this.connectCorrelation == that.connectCorrelation &&
                this.slotIndex == that.slotIndex &&
                this.slotLimit == that.slotLimit &&
                Objects.equals(this.connectSource, that.connectSource) &&
                Objects.equals(this.bufferPool, that.bufferPool);
    }

    @Override
    public String toString()
    {
        return String.format("[connectCorrelation=%d, slotIndex=%d slotLimit=%d connectSource=\"%s\", bufferPool=%s]",
                connectCorrelation, slotIndex, slotLimit, connectSource, bufferPool);
    }
}
