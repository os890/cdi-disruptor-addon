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
package org.os890.cdi.disruptor.lite.test;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.os890.cdi.disruptor.ObservesDisruptor;

@ApplicationScoped
public class RegionObserver {
    public static final String EU_ORDERS = "eu-orders";

    @Inject
    private EventRecorder recorder;

    // the non-binding description differs from the one of the fired events
    void onEuOrder(@ObservesDisruptor @Region(value = "eu", description = "observer") Order order) {
        recorder.record(EU_ORDERS, order.id());
    }
}
