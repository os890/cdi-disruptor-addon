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
package org.os890.cdi.disruptor.test.perf;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import org.os890.cdi.disruptor.DisruptorEvent;
import org.os890.cdi.disruptor.RingBuffer;

import java.util.function.Consumer;

@ApplicationScoped
public class BenchPublisher {
    @Inject
    private Event<BenchEvent> cdiEvent;

    // the configuration of the ring-buffer is specified by the observer of the variant
    @Inject
    @RingBuffer(name = "bench")
    private DisruptorEvent<BenchEvent> disruptorEvent;

    public Consumer<BenchEvent> cdiAsync() {
        return cdiEvent::fireAsync;
    }

    public Consumer<BenchEvent> disruptor() {
        return disruptorEvent::fire;
    }
}
