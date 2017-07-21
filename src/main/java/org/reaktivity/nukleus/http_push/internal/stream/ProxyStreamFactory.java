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
package org.reaktivity.nukleus.http_push.internal.stream;

import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http_push.util.HttpHeadersUtil.CACHE_CONTROL;
import static org.reaktivity.nukleus.http_push.util.HttpHeadersUtil.CACHE_SYNC;
import static org.reaktivity.nukleus.http_push.util.HttpHeadersUtil.INJECTED_DEFAULT_HEADER;
import static org.reaktivity.nukleus.http_push.util.HttpHeadersUtil.INJECTED_HEADER_AND_NO_CACHE;
import static org.reaktivity.nukleus.http_push.util.HttpHeadersUtil.INJECTED_HEADER_AND_NO_CACHE_VALUE;
import static org.reaktivity.nukleus.http_push.util.HttpHeadersUtil.INJECTED_HEADER_DEFAULT_VALUE;
import static org.reaktivity.nukleus.http_push.util.HttpHeadersUtil.INJECTED_HEADER_NAME;
import static org.reaktivity.nukleus.http_push.util.HttpHeadersUtil.IS_INJECTED_HEADER;
import static org.reaktivity.nukleus.http_push.util.HttpHeadersUtil.IS_POLL_HEADER;
import static org.reaktivity.nukleus.http_push.util.HttpHeadersUtil.NO_CACHE_CACHE_CONTROL;
import static org.reaktivity.nukleus.http_push.util.HttpHeadersUtil.getHeader;

import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http_push.internal.Correlation;
import org.reaktivity.nukleus.http_push.internal.stream.util.Writer;
import org.reaktivity.nukleus.http_push.internal.types.Flyweight;
import org.reaktivity.nukleus.http_push.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http_push.internal.types.ListFW;
import org.reaktivity.nukleus.http_push.internal.types.ListFW.Builder;
import org.reaktivity.nukleus.http_push.internal.types.OctetsFW;
import org.reaktivity.nukleus.http_push.internal.types.String16FW;
import org.reaktivity.nukleus.http_push.internal.types.StringFW;
import org.reaktivity.nukleus.http_push.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http_push.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http_push.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http_push.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http_push.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http_push.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http_push.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http_push.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http_push.util.LongObjectBiConsumer;
import org.reaktivity.nukleus.route.RouteHandler;
import org.reaktivity.nukleus.stream.StreamFactory;

public class ProxyStreamFactory implements StreamFactory
{
    private static final String STALE_WHILE_REVALIDATE_31536000 = "stale-while-revalidate=31536000";

    // TODO, remove need for RW in simplification of inject headers
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    private final BeginFW beginRO = new BeginFW();
    private final HttpBeginExFW httpBeginExRO = new HttpBeginExFW();
    private final ListFW<HttpHeaderFW> headersRO = new HttpBeginExFW().headers();
    private final ListFW<HttpHeaderFW> headersRO2 = new HttpBeginExFW().headers();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final RouteFW routeRO = new RouteFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final RouteHandler router;

    private final LongObjectBiConsumer<Runnable> scheduler;
    private final LongSupplier supplyStreamId;
    private final BufferPool bufferPool;
    private final Long2ObjectHashMap<Correlation> correlations;
    private final Writer writer;
    private final LongSupplier supplyCorrelationId;

    public ProxyStreamFactory(
        RouteHandler router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<Correlation> correlations,
        LongObjectBiConsumer<Runnable> scheduler)
    {
        this.router = requireNonNull(router);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.bufferPool = requireNonNull(bufferPool);
        this.correlations = requireNonNull(correlations);
        this.scheduler = scheduler;
        this.supplyCorrelationId = requireNonNull(supplyCorrelationId);
        this.writer = new Writer(writeBuffer);
    }

    @Override
    public MessageConsumer newStream(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length,
            MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long sourceRef = begin.sourceRef();

        MessageConsumer newStream;

        if (sourceRef == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
            final BeginFW begin,
            final MessageConsumer acceptThrottle)
    {
        final long acceptRef = begin.sourceRef();
        final String acceptName = begin.source().asString();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            return acceptRef == route.sourceRef() &&
                   acceptName.equals(route.source().asString());
        };

        final RouteFW route = router.resolve(filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long networkId = begin.streamId();

            newStream = new ProxyAcceptStream(acceptThrottle, networkId)::handleStream;
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
            final BeginFW begin,
            final MessageConsumer throttle)
    {
        final long throttleId = begin.streamId();

        return new ProxyConnectReplyStream(throttle, throttleId)::handleStream;
    }

    private final class ProxyAcceptStream
    {
        private final MessageConsumer acceptThrottle;
        private final long clientStreamId;

        private MessageConsumer streamState;

        private int pollInterval = 0; // needed because effectively final forEach
        private MessageConsumer connect;
        private long connectStreamId;
        private long connectCorrelationId;

        private boolean aborted = false; // needed because no cancel in scheduler

        private ProxyAcceptStream(
                MessageConsumer clientThrottle,
                long clientStreamId)
        {
            this.acceptThrottle = clientThrottle;
            this.clientStreamId = clientStreamId;
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                writer.doReset(acceptThrottle, clientStreamId);
            }
        }

        private void handleBegin(
                BeginFW begin)
        {
            final long acceptRef = beginRO.sourceRef();
            final String acceptName = begin.source().asString();
            final RouteFW connectRoute = resolveTarget(acceptRef, acceptName);

            if (connectRoute == null)
            {
                // just reset
                long acceptStreamId = begin.streamId();
                writer.doReset(acceptThrottle, acceptStreamId);
            }
            else
            {
                final String connectName = connectRoute.target().asString();
                this.connect = router.supplyTarget(connectName);
                final long connectRef = connectRoute.targetRef();
                this.connectStreamId =  supplyStreamId.getAsLong();
                final long acceptCorrelationId = begin.correlationId();
                this.connectCorrelationId = supplyCorrelationId.getAsLong();

                int storedRequestSize = -1;

                final OctetsFW extension = beginRO.extension();
                extension.get(httpBeginExRO::wrap);
                final ListFW<HttpHeaderFW> headers = httpBeginExRO.headers();

                int slotIndex = bufferPool.acquire(connectStreamId);
                if (slotIndex != NO_SLOT)
                {
                    final MutableDirectBuffer store = bufferPool.buffer(slotIndex);
                    store.putBytes(0, headers.buffer(), headers.offset(), headers.sizeof());
                    storedRequestSize = headers.sizeof();

                    Correlation correlation =
                            new Correlation(acceptName, bufferPool, slotIndex, storedRequestSize, acceptCorrelationId);

                    if (headers.anyMatch(IS_POLL_HEADER) && headers.anyMatch(IS_INJECTED_HEADER))
                    {
                        schedulePoll(connect, connectStreamId, connectRef,
                                     connectCorrelationId, store, slotIndex, storedRequestSize);
                        this.streamState = this::afterScheduledPoll;
                        correlations.put(connectCorrelationId, correlation);
                    }
                    else
                    {
                        writer.doHttpBegin(connect, connectStreamId, connectRef,
                                           connectCorrelationId, e -> e.set(beginRO.extension()));

                        router.setThrottle(connectName, connectStreamId, this::handleThrottle);

                        this.streamState = this::afterBegin;
                        correlations.put(connectCorrelationId, correlation);
                    }
                }
                else
                {
                    final MessageConsumer acceptReply = router.supplyTarget(acceptName);
                    final long acceptReplyStreamId = supplyStreamId.getAsLong();
                    writer.doHttpFailedStatus(
                            acceptReply,
                            acceptReplyStreamId,
                            0L,
                            acceptCorrelationId);
                    writer.doAbort(acceptReply, acceptReplyStreamId);
                }
            }
        }

        private void schedulePoll(
                MessageConsumer connectTarget,
                long connectStreamId,
                long connectRef,
                long correlationId,
                MutableDirectBuffer store,
                int slotIndex,
                int storedRequestSize)
        {
            scheduler.accept(System.currentTimeMillis() + (pollInterval * 1000), () ->
            {
                if (!this.aborted)
                {
                    final ListFW<HttpHeaderFW> headers = httpBeginExRO.headers().wrap(store, 0, storedRequestSize);

                    boolean stripNoCacheValue = false;

                    Predicate<HttpHeaderFW> isInjected = h -> INJECTED_HEADER_NAME.equals(h.name().asString());
                    if (headers.anyMatch(INJECTED_HEADER_AND_NO_CACHE) && headers.anyMatch(NO_CACHE_CACHE_CONTROL))
                        {
                            isInjected = isInjected.or(h -> CACHE_CONTROL.equals(h.name().asString()));
                            if (headers.anyMatch(h -> CACHE_CONTROL.equals(h.name().asString())))
                            {
                                stripNoCacheValue = true;
                            }
                    }

                    final Predicate<HttpHeaderFW> toForward = isInjected.negate();
                    final boolean toStripNoCacheValue = stripNoCacheValue;

                    writer.doHttpBegin2(
                        connectTarget,
                        connectStreamId,
                        connectRef,
                        correlationId,
                        hs -> headers.forEach(h ->
                        {
                            final StringFW nameRO = h.name();
                            final String name = nameRO.asString();
                            final String16FW valueRO = h.value();
                            final String value = valueRO.asString();
                            if (toForward.test(h))
                            {
                                hs.item(b ->
                                    b.representation((byte) 0).name(nameRO).value(valueRO)
                                );
                            }
                            else if (toStripNoCacheValue && CACHE_CONTROL.equals(name))
                            {
                                hs.item(b ->
                                    {
                                        String replaceFirst = value.replaceFirst(",?\\s*no-cache", "");
                                        b.representation((byte) 0)
                                                .name(nameRO)
                                                .value(replaceFirst);
                                    }
                                );
                            }
                        }));

                    writer.doHttpEnd(connectTarget, connectStreamId);
                }
            });
        }

        private void handleData(
                DataFW data)
        {
            final OctetsFW payload = data.payload();
            writer.doHttpData(connect, connectStreamId, payload.buffer(), payload.offset(), payload.sizeof());
        }

        private void afterBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                handleData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
                break;
            default:
                correlations.remove(connectCorrelationId);
                writer.doReset(acceptThrottle, clientStreamId);
                break;
            }
        }

        private void afterScheduledPoll(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
                break;
            default:
                this.aborted = true;
                correlations.remove(connectCorrelationId);
                writer.doReset(acceptThrottle, clientStreamId);
                break;
            }
        }

        private void handleEnd(
                EndFW end)
        {
            writer.doHttpEnd(connect, connectStreamId);
        }

        private void handleThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleWindow(
            WindowFW window)
        {
            final int bytes = windowRO.update();
            final int frames = windowRO.frames();

            writer.doWindow(acceptThrottle, clientStreamId, bytes, frames);
        }

        private void handleReset(
            ResetFW reset)
        {
            writer.doReset(acceptThrottle, clientStreamId);
        }

        private void handleAbort(
            AbortFW abort)
        {
            correlations.remove(connectCorrelationId);
            this.aborted = true;
            writer.doAbort(connect, connectStreamId);
        }
    }

    private final class ProxyConnectReplyStream
    {

        private static final String CACHE_SYNC_ALWAYS = "always";

        private static final String IF_UNMODIFIED_SINCE = "if-unmodified-since";

        private static final String IF_MATCH = "if-match";

        private static final String NO_CACHE = "no-cache";

        private static final String IF_NONE_MATCH = "if-none-match";

        private static final String IF_MODIFIED_SINCE = "if-modified-since";

        private MessageConsumer streamState;

        private final MessageConsumer connectThrottle;
        private final long connectReplyStreamId;

        private MessageConsumer acceptReply;
        private long acceptReplyStreamId;

        private ProxyConnectReplyStream(
                MessageConsumer connectReplyThrottle,
                long connectReplyId)
        {
            this.connectThrottle = connectReplyThrottle;
            this.connectReplyStreamId = connectReplyId;
            this.streamState = this::beforeBegin;
        }

        private void handleStream(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            streamState.accept(msgTypeId, buffer, index, length);
        }

        private void beforeBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                writer.doReset(connectThrottle, connectReplyStreamId);
            }
        }

        private void afterBegin(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    handleData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    handleEnd(end);
                    break;
                case AbortFW.TYPE_ID:
                    final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                    handleAbort(abort);
                    break;
                default:
                    writer.doReset(connectThrottle, connectReplyStreamId);
                    break;
            }
        }

        private void handleBegin(
                BeginFW begin)
        {
            final long connectRef = begin.sourceRef();
            final long correlationId = begin.correlationId();
            final Correlation streamCorrelation = connectRef == 0L ? correlations.remove(correlationId) : null;

            if (streamCorrelation != null)
            {
                final String acceptReplyName = streamCorrelation.connectSource();
                this.acceptReply = router.supplyTarget(acceptReplyName);
                this.acceptReplyStreamId = supplyStreamId.getAsLong();
                final long acceptCorrelationId = streamCorrelation.connectCorrelation();
                final BufferPool bufferPool = streamCorrelation.bufferPool();
                final int slotIndex = streamCorrelation.slotIndex();

                if (slotIndex != NO_SLOT)
                {
                    final int slabSlotLimit = streamCorrelation.slotLimit();
                    final MutableDirectBuffer savedRequest = bufferPool.buffer(slotIndex);
                    final ListFW<HttpHeaderFW> requestHeaders = headersRO.wrap(savedRequest, 0, slabSlotLimit);

                    boolean sendUpdateOnChange = requestHeaders.anyMatch(IS_POLL_HEADER);
                    if (sendUpdateOnChange)
                    {
                        final OctetsFW responseExtensions = beginRO.extension();
                        responseExtensions.get(httpBeginExRO::wrap);
                        final ListFW<HttpHeaderFW> responseHeaders = headersRO2.wrap(responseExtensions.buffer(),
                                                                                     responseExtensions.offset(),
                                                                                     responseExtensions.limit());
                        if (responseHeaders.anyMatch(h -> CACHE_CONTROL.equals(h.name().asString())))
                        {
                            writer.doHttpBegin2(acceptReply, acceptReplyStreamId, 0L,
                                    acceptCorrelationId, appendStaleWhileRevalidate(responseHeaders));
                        }
                        else
                        {
                            writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L,
                                    acceptCorrelationId, injectStaleWhileRevalidate(headersToExtensions(responseHeaders)));
                        }

                        if (!requestHeaders.anyMatch(
                                h -> INJECTED_HEADER_NAME.equals(h.name().asString())))
                        {
                            writer.doH2PushPromise(
                                    acceptReply,
                                    acceptReplyStreamId,
                                    injectPushHeaders(requestHeaders, responseHeaders));
                        }
                    }
                    else
                    {
                        final OctetsFW responseExtensions = beginRO.extension();
                        responseExtensions.get(httpBeginExRO::wrap);
                        writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L,
                                acceptCorrelationId, e -> e.set(responseExtensions));
                    }

                    router.setThrottle(acceptReplyName, acceptReplyStreamId, this::handleThrottle);
                    bufferPool.release(slotIndex);
                }
                else
                {
                    final OctetsFW responseExtensions = beginRO.extension();
                    responseExtensions.get(httpBeginExRO::wrap);
                    writer.doHttpBegin(acceptReply, acceptReplyStreamId, 0L, acceptCorrelationId, e -> e.set(responseExtensions));
                }
                this.streamState = this::afterBegin;
            }
            else
            {
                writer.doReset(connectThrottle, connectReplyStreamId);
            }
        }

        private void handleThrottle(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
        {
            switch (msgTypeId)
            {
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    handleWindow(window);
                    break;
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    handleReset(reset);
                    break;
                default:
                    // ignore
                    break;
            }
        }

        private void handleWindow(
            WindowFW window)
        {
            final int bytes = windowRO.update();
            final int frames = windowRO.frames();

            writer.doWindow(connectThrottle, connectReplyStreamId, bytes, frames);
        }

        private void handleReset(
            ResetFW reset)
        {
            writer.doReset(connectThrottle, connectReplyStreamId);
        }

        private void handleAbort(
                AbortFW abort)
        {
            writer.doAbort(acceptReply, acceptReplyStreamId);
        }

        private Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headersToExtensions(
            ListFW<HttpHeaderFW> headersFW)
        {
            return x -> headersFW
            .forEach(h ->
                x.item(y -> y.representation((byte) 0).name(h.name()).value(h.value()))
            );
        }

        private Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> injectPushHeaders(
                ListFW<HttpHeaderFW> requestHeadersFW, ListFW<HttpHeaderFW> responseHeadersFW)
        {
            Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> result;

            if (requestHeadersFW.anyMatch(INJECTED_DEFAULT_HEADER) || requestHeadersFW.anyMatch(INJECTED_HEADER_AND_NO_CACHE))
            {
               result = x -> addRequestHeaders(requestHeadersFW, responseHeadersFW, x);
            }
            else if (requestHeadersFW.anyMatch(NO_CACHE_CACHE_CONTROL))
            {
                result = x ->
                {
                    addRequestHeaders(requestHeadersFW, responseHeadersFW, x);

                    x.item(y -> y.representation((byte) 0)
                            .name(INJECTED_HEADER_NAME)
                            .value(INJECTED_HEADER_DEFAULT_VALUE));

                    x.item(y -> y.representation((byte) 0)
                            .name(CACHE_SYNC)
                            .value(CACHE_SYNC_ALWAYS));
                };
            }
            else if (requestHeadersFW.anyMatch(h -> CACHE_CONTROL.equals(h.name().asString())))
            {
                result = x ->
                {
                    addRequestHeaders(requestHeadersFW, responseHeadersFW, x);

                    x.item(y -> y.representation((byte) 0)
                                .name(INJECTED_HEADER_NAME)
                                .value(INJECTED_HEADER_AND_NO_CACHE_VALUE));

                    x.item(y -> y.representation((byte) 0)
                                .name(CACHE_SYNC)
                                .value(CACHE_SYNC_ALWAYS));
                };
            }
            else
            {
                result = x ->
                {
                    addRequestHeaders(requestHeadersFW, responseHeadersFW, x);

                    x.item(y -> y.representation((byte) 0)
                                .name(INJECTED_HEADER_NAME)
                                .value(INJECTED_HEADER_AND_NO_CACHE_VALUE));

                    x.item(y -> y.representation((byte) 0)
                            .name(CACHE_CONTROL)
                            .value(NO_CACHE));

                    x.item(y -> y.representation((byte) 0)
                                .name(CACHE_SYNC)
                                .value(CACHE_SYNC_ALWAYS));
                };
            }
            return result;
        }

        private void addRequestHeaders(
                ListFW<HttpHeaderFW> requestHeadersFW,
                ListFW<HttpHeaderFW> responseHeadersFW,
                Builder<HttpHeaderFW.Builder, HttpHeaderFW> x)
        {
            requestHeadersFW
               .forEach(h ->
               {
                   final StringFW nameFW = h.name();
                   final String name = nameFW.asString();
                   final String16FW valueFW = h.value();
                   final String value = valueFW.asString();

                   switch(name)
                   {
                       case CACHE_CONTROL:
                           if (value.contains(NO_CACHE))
                           {
                               x.item(y -> y.representation((byte) 0)
                                                .name(nameFW)
                                                .value(valueFW));
                           }
                           else
                           {
                               x.item(y -> y.representation((byte) 0)
                                                .name(nameFW)
                                                .value(value + ", no-cache"));
                           }
                           break;
                        case IF_MODIFIED_SINCE:
                           if (responseHeadersFW.anyMatch(h2 -> "last-modified".equals(h2.name().asString())))
                           {
                               final String newValue = getHeader(responseHeadersFW, "last-modified");
                               x.item(y -> y.representation((byte) 0)
                                            .name(nameFW)
                                            .value(newValue));
                           }
                           break;
                        case IF_NONE_MATCH:
                           if (responseHeadersFW.anyMatch(h2 -> "etag".equals(h2.name().asString())))
                           {
                               final String newValue = getHeader(responseHeadersFW, "etag");
                               x.item(y -> y.representation((byte) 0)
                                            .name(nameFW)
                                            .value(newValue));
                           }
                           break;
                        case IF_MATCH:
                        case IF_UNMODIFIED_SINCE:
                            break;
                       default: x.item(y -> y.representation((byte) 0)
                                            .name(nameFW)
                                            .value(valueFW));
                   }
               });
        }

        private Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> appendStaleWhileRevalidate(
                ListFW<HttpHeaderFW> headersFW)
        {
            return x -> headersFW
                    .forEach(h ->
                        {
                            final StringFW nameRO = h.name();
                            final String16FW valueRO = h.value();
                            final String name = nameRO.asString();
                            final String value = valueRO.asString();
                            if (CACHE_CONTROL.equals(name) && !value.contains(STALE_WHILE_REVALIDATE_31536000))
                            {
                                x.item(y -> y.representation((byte) 0)
                                        .name(nameRO)
                                        .value(value + ", " + STALE_WHILE_REVALIDATE_31536000));
                            }
                            else
                            {
                                x.item(y -> y.representation((byte) 0).name(nameRO).value(h.value()));
                            }
                        }
                    );
        }

        private Flyweight.Builder.Visitor injectStaleWhileRevalidate(
            Consumer<Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
        {
            mutator = mutator.andThen(
                    x ->  x.item(h -> h.representation((byte) 0).name("cache-control").value(STALE_WHILE_REVALIDATE_31536000))
                );
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

        private void handleData(
                DataFW data)
        {
            final OctetsFW payload = data.payload();
            writer.doHttpData(acceptReply, acceptReplyStreamId, payload.buffer(), payload.offset(), payload.sizeof());
        }

        private void handleEnd(
            EndFW end)
        {
            writer.doHttpEnd(acceptReply, acceptReplyStreamId);
        }

    }

    RouteFW resolveTarget(
            long sourceRef,
            String sourceName)
    {
        MessagePredicate filter = (t, b, o, l) ->
        {
            RouteFW route = routeRO.wrap(b, o, l);
            return sourceRef == route.sourceRef() && sourceName.equals(route.source().asString());
        };

        return router.resolve(filter, this::wrapRoute);
    }

    private RouteFW wrapRoute(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

}
