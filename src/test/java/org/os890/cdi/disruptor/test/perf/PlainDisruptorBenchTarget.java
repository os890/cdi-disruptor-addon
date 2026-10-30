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

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

/**
 * Plain Disruptor without CDI (same producer-type as the add-on).
 * <p>
 * With event objects the slots reference the fired events (same programming model as the add-on),
 * with preallocated slots the data of the event gets copied into the slot (idiomatic zero-allocation style).
 */
final class PlainDisruptorBenchTarget implements BenchTarget {
    private static final EventTranslatorOneArg<Slot, BenchEvent> STORE_REFERENCE =
        (slot, sequence, event) -> slot.event = event;
    private static final EventTranslatorOneArg<Slot, BenchEvent> COPY_DATA =
        (slot, sequence, event) -> slot.data = event.sequence();

    private final BenchStatistics statistics = new BenchStatistics();
    private final Disruptor<Slot> disruptor;
    private final RingBuffer<Slot> ringBuffer;
    private final EventTranslatorOneArg<Slot, BenchEvent> translator;

    PlainDisruptorBenchTarget(int size, WaitStrategy waitStrategy, boolean preallocated) {
        disruptor = new Disruptor<>(Slot::new, size, DaemonThreadFactory.INSTANCE, ProducerType.MULTI, waitStrategy);
        if (preallocated) {
            translator = COPY_DATA;
            disruptor.handleEventsWith((slot, sequence, endOfBatch) -> statistics.record(null));
        } else {
            translator = STORE_REFERENCE;
            disruptor.handleEventsWith((slot, sequence, endOfBatch) -> {
                BenchEvent event = slot.event;
                slot.event = null;
                statistics.record(event);
            });
        }
        ringBuffer = disruptor.start();
    }

    @Override
    public void fire(BenchEvent event) {
        ringBuffer.publishEvent(translator, event);
    }

    @Override
    public long received() {
        return statistics.received();
    }

    @Override
    public void close() {
        disruptor.shutdown();
    }

    private static final class Slot {
        private BenchEvent event;
        private long data;
    }
}
