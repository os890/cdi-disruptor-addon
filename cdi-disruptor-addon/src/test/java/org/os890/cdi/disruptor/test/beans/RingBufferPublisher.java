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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.os890.cdi.disruptor.DisruptorEvent;
import org.os890.cdi.disruptor.RingBuffer;

@ApplicationScoped
public class RingBufferPublisher {
    @Inject
    @RingBuffer(name = "audit")
    private DisruptorEvent<Order> auditOrderEvent;

    // configured via annotation only
    @Inject
    @RingBuffer(name = "small", size = 2)
    private DisruptorEvent<BlockingTask> smallEvent;

    // the size of the annotation gets overridden via MicroProfile Config (if available)
    @Inject
    @RingBuffer(name = "tiny", size = 8)
    private DisruptorEvent<BlockingTask> tinyEvent;

    // the same ring-buffer as tinyEvent (configured once, used with another qualifier)
    @Inject
    @RingBuffer(name = "tiny")
    @Premium
    private DisruptorEvent<BlockingTask> premiumTinyEvent;

    // configured via TestDisruptorConfig (overrides MicroProfile Config)
    @Inject
    @RingBuffer(name = "custom")
    private DisruptorEvent<BlockingTask> customEvent;

    public DisruptorEvent<Order> auditOrderEvent() {
        return auditOrderEvent;
    }

    public DisruptorEvent<BlockingTask> smallEvent() {
        return smallEvent;
    }

    public DisruptorEvent<BlockingTask> tinyEvent() {
        return tinyEvent;
    }

    public DisruptorEvent<BlockingTask> premiumTinyEvent() {
        return premiumTinyEvent;
    }

    public DisruptorEvent<BlockingTask> customEvent() {
        return customEvent;
    }
}
