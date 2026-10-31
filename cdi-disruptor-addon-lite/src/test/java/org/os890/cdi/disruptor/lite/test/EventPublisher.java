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
import org.os890.cdi.disruptor.CircuitBreaker;
import org.os890.cdi.disruptor.DisruptorEvent;
import org.os890.cdi.disruptor.RingBuffer;

@ApplicationScoped
public class EventPublisher {
    @Inject
    private DisruptorEvent<Order> orderEvent;

    @Inject
    @Premium
    private DisruptorEvent<Order> premiumOrderEvent;

    @Inject
    private DisruptorEvent<Object> anyEvent;

    @Inject
    @RingBuffer(name = "audit")
    private DisruptorEvent<Order> auditOrderEvent;

    // the settings of the circuit-breaker are configured via TestDisruptorConfig
    @Inject
    @RingBuffer(name = "guarded", size = 2)
    @CircuitBreaker
    private DisruptorEvent<BlockingTask> guardedEvent;

    @Inject
    @RingBuffer(name = "guarded")
    private DisruptorEvent<BlockingTask> unguardedEvent;

    // the settings of the circuit-breaker are configured via application.properties (MicroProfile Config)
    @Inject
    @RingBuffer(name = "flaky")
    private DisruptorEvent<FlakyTask> flakyEvent;

    public DisruptorEvent<Order> orderEvent() {
        return orderEvent;
    }

    public DisruptorEvent<Order> premiumOrderEvent() {
        return premiumOrderEvent;
    }

    public DisruptorEvent<Object> anyEvent() {
        return anyEvent;
    }

    public DisruptorEvent<Order> auditOrderEvent() {
        return auditOrderEvent;
    }

    public DisruptorEvent<BlockingTask> guardedEvent() {
        return guardedEvent;
    }

    public DisruptorEvent<BlockingTask> unguardedEvent() {
        return unguardedEvent;
    }

    public DisruptorEvent<FlakyTask> flakyEvent() {
        return flakyEvent;
    }
}
