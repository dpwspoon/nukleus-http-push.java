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
package org.reaktivity.nukleus.http_push.internal.streams.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.reaktor.internal.ReaktorConfiguration.ABORT_STREAM_FRAME_TYPE_ID;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.http_push.internal.types.stream.AbortFW;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ProxyIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/http_push/control/route")
        .addScriptRoot("streams", "org/reaktivity/specification/nukleus/http_push/streams/proxy/");

    private final TestRule timeout = new DisableOnDebug(new Timeout(15, SECONDS));

    private final ReaktorRule nukleus = new ReaktorRule()
            .directory("target/nukleus-itests")
            .commandBufferCapacity(1024)
            .responseBufferCapacity(1024)
            .counterValuesBufferCapacity(1024)
            .nukleus("http-push"::equals)
            .configure(ABORT_STREAM_FRAME_TYPE_ID, AbortFW.TYPE_ID)
            .clean();

    @Rule
    public final TestRule chain = outerRule(nukleus).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/opening/accept/client",
        "${streams}/opening/connect/server" })
    public void shouldProxyConnection() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/inject.push.promise/accept/client",
        "${streams}/inject.push.promise/connect/server" })
    public void shouldInjectPushPromise() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/strip.injected.headers/accept/client",
        "${streams}/strip.injected.headers/connect/server" })
    public void shouldStripInjectedHeaders() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/client.sent.abort/accept/client",
        "${streams}/client.sent.abort/connect/server" })
    public void shouldForwardAbortOnAcceptToConnect() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/client.sent.abort.on.scheduled.poll/accept/client" })
    public void shouldCancelScheduledPollOnAbort() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/server.sent.abort/accept/client",
        "${streams}/server.sent.abort/connect/server" })
    public void shouldForwardAbortOnConnectReplyToAcceptReply() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/server.sent.reset/accept/client",
        "${streams}/server.sent.reset/connect/server" })
    public void shouldForwardResetOnConnectToAccept() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/controller",
        "${streams}/client.sent.reset/accept/client",
        "${streams}/client.sent.reset/connect/server" })
    public void shouldForwardResetOnAcceptReplyToConnectReply() throws Exception
    {
        k3po.finish();
    }

}
