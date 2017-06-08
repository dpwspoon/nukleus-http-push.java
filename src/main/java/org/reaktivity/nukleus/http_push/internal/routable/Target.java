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
package org.reaktivity.nukleus.http_push.internal.routable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.reaktivity.nukleus.http_push.internal.util.HttpHeadersUtil.INJECTED_DEFAULT_HEADER;
import static org.reaktivity.nukleus.http_push.internal.util.HttpHeadersUtil.INJECTED_HEADER_AND_NO_CACHE;
import static org.reaktivity.nukleus.http_push.internal.util.HttpHeadersUtil.INJECTED_HEADER_AND_NO_CACHE_VALUE;
import static org.reaktivity.nukleus.http_push.internal.util.HttpHeadersUtil.INJECTED_HEADER_DEFAULT_VALUE;
import static org.reaktivity.nukleus.http_push.internal.util.HttpHeadersUtil.INJECTED_HEADER_NAME;
import static org.reaktivity.nukleus.http_push.internal.util.HttpHeadersUtil.NO_CACHE_CACHE_CONTROL;

import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.http_push.internal.HttpPushNukleus;
import org.reaktivity.nukleus.http_push.internal.layouts.StreamsLayout;
import org.reaktivity.nukleus.http_push.internal.types.Flyweight;
import org.reaktivity.nukleus.http_push.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_push.internal.types.ListFW;
import org.reaktivity.nukleus.http_push.internal.types.ListFW.Builder;
import org.reaktivity.nukleus.http_push.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_push.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_push.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_push.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_push.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http_push.internal.types.stream.HttpBeginExFW;

public final class Target implements Nukleus
{
    private static final DirectBuffer SOURCE_NAME_BUFFER = new UnsafeBuffer(HttpPushNukleus.NAME.getBytes(UTF_8));

    private final FrameFW frameRO = new FrameFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();

    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    private final String name;
    private final StreamsLayout layout;
    private final AtomicBuffer writeBuffer;

    private final RingBuffer streamsBuffer;
    private final RingBuffer throttleBuffer;
    private final Long2ObjectHashMap<MessageHandler> throttles;

    public Target(
        String name,
        StreamsLayout layout,
        AtomicBuffer writeBuffer)
    {
        this.name = name;
        this.layout = layout;
        this.writeBuffer = writeBuffer;
        this.streamsBuffer = layout.streamsBuffer();
        this.throttleBuffer = layout.throttleBuffer();
        this.throttles = new Long2ObjectHashMap<>();
    }

    @Override
    public int process()
    {
        return throttleBuffer.read(this::handleRead);
    }

    @Override
    public void close() throws Exception
    {
        layout.close();
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public String toString()
    {
        return name;
    }

    public void addThrottle(
        long streamId,
        MessageHandler throttle)
    {
        throttles.put(streamId, throttle);
    }

    public void removeThrottle(
        long streamId)
    {
        throttles.remove(streamId);
    }

    private void handleRead(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        frameRO.wrap(buffer, index, index + length);

        final long streamId = frameRO.streamId();
        // TODO: use Long2ObjectHashMap.getOrDefault(long, T) instead

        final MessageHandler throttle = throttles.get(streamId);

        if (throttle != null)
        {
            throttle.onMessage(msgTypeId, buffer, index, length);
        }
    }

    public void doHttpBegin(
        long targetId,
        long targetRef,
        long correlationId,
        Consumer<OctetsFW.Builder> extensions)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .source(SOURCE_NAME_BUFFER, 0, SOURCE_NAME_BUFFER.capacity())
                .sourceRef(targetRef)
                .correlationId(correlationId)
                .extension(extensions)
                .build();
        streamsBuffer.write(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    public void doHttpBegin(
            long targetId,
            long targetRef,
            long correlationId,
            Flyweight.Builder.Visitor injectHeaders)
    {
        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .source(SOURCE_NAME_BUFFER, 0, SOURCE_NAME_BUFFER.capacity())
                .sourceRef(targetRef)
                .correlationId(correlationId)
                .extension(e -> e.set(injectHeaders))
                .build();
        streamsBuffer.write(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    public void doHttpData(
        long targetId,
        OctetsFW payload)
    {
        DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .payload(p -> p.set(payload.buffer(), payload.offset(), payload.sizeof()))
                .extension(e -> e.reset())
                .build();

        streamsBuffer.write(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    public void doHttpEnd(
        long targetId)
    {

        EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(targetId)
                .extension(e -> e.reset())
                .build();
        streamsBuffer.write(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    public void doH2PushPromise(
        long targetId,
        ListFW<HttpHeaderFW> headers,
        Consumer<ListFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {

        DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
            .streamId(targetId)
            .payload(e -> e.reset())
            .extension(e -> e.set(injectSyncHeaders(mutator, headers)))
            .build();

        streamsBuffer.write(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private Flyweight.Builder.Visitor injectSyncHeaders(
            Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator,
            ListFW<HttpHeaderFW> headers)
    {
        if(headers.anyMatch(INJECTED_DEFAULT_HEADER) || headers.anyMatch(INJECTED_HEADER_AND_NO_CACHE))
        {
            // Already injected, NOOP
        }
        else if(headers.anyMatch(NO_CACHE_CACHE_CONTROL))
        {
            // INJECT HEADER
            mutator = mutator.andThen(
                x ->  x.item(h -> h.representation((byte) 0).name(INJECTED_HEADER_NAME).value(INJECTED_HEADER_DEFAULT_VALUE))
            );
            mutator = mutator.andThen(
                x ->  x.item(h -> h.representation((byte) 0).name("x-http-cache-sync").value("always"))
            );
        }
        else
        {
            // INJECT HEADER AND NO-CACHE
            mutator = mutator.andThen(
                x ->  x.item(h -> h.representation((byte) 0).name(INJECTED_HEADER_NAME).value(INJECTED_HEADER_AND_NO_CACHE_VALUE))
            );
            mutator = mutator.andThen(
                    x ->  x.item(h -> h.representation((byte) 0).name("cache-control").value("no-cache"))
            );
            mutator = mutator.andThen(
                x ->  x.item(h -> h.representation((byte) 0).name("x-http-cache-sync").value("always"))
            );
        }
        return visitHttpBeginEx(mutator);

    }

    private Flyweight.Builder.Visitor visitHttpBeginEx(
            Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)

    {
        return (buffer, offset, limit) ->
        httpBeginExRW.wrap(buffer, offset, limit)
                     .headers(headers)
                     .build()
                     .sizeof();
    }

}
