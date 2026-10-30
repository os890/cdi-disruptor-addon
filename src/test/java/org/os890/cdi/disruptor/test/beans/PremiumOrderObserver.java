/*
 * Copyright 2026 os890
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.os890.cdi.disruptor.test.beans;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.os890.cdi.disruptor.DisruptorEvent;
import org.os890.cdi.disruptor.ObservesDisruptor;

@Dependent
public class PremiumOrderObserver {
    public static final String PREMIUM_ORDERS = "premium-orders";
    public static final String REQUEST_COUNTS = "request-counts";
    public static final String DESTROYED = "premium-observer-destroyed";

    @Inject
    private EventRecorder recorder;

    void onPremiumOrder(@ObservesDisruptor @Premium Order order,
                        RequestCounter requestCounter,
                        DisruptorEvent<CharSequence> textEvent) {
        recorder.record(REQUEST_COUNTS, requestCounter.increment());
        recorder.record(PREMIUM_ORDERS, order.id());
        textEvent.tryFire("premium:" + order.id());
    }

    @PreDestroy
    void onDestroy() {
        recorder.record(DESTROYED, Boolean.TRUE);
    }
}
