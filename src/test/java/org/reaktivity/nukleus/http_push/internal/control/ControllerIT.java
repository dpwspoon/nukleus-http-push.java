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
package org.reaktivity.nukleus.http_push.internal.control;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.http_push.HttpPushController;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ControllerIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("route", "org/reaktivity/specification/nukleus/http_push/control/route")
        .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/http_push/control/unroute");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .controller(HttpPushController.class::equals);

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(reaktor);

    @Test
    @Specification({
        "${route}/proxy/nukleus"
    })
    public void shouldRouteProxy() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        reaktor.controller(HttpPushController.class)
           .routeProxy("source", 0L, "target", targetRef)
        .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/proxy/nukleus",
        "${unroute}/proxy/nukleus"
    })
    public void shouldUnrouteProxy() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long sourceRef = reaktor.controller(HttpPushController.class)
            .routeProxy("source", 0L, "target", targetRef)
            .get();

        k3po.notifyBarrier("ROUTED_PROXY");

        reaktor.controller(HttpPushController.class)
               .unrouteProxy("source", sourceRef, "target", targetRef)
                  .get();

        k3po.finish();
    }
}
