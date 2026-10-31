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

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageCodec;

/**
 * Eclipse Vert.x without CDI - one verticle with a local event-bus consumer (event-loop thread),
 * the events get sent via {@code EventBus#send} (fire-and-forget, the event objects are passed by reference).
 */
final class VertxEventBusBenchTarget implements BenchTarget {
    private static final String ADDRESS = "bench";

    private final BenchStatistics statistics = new BenchStatistics();
    private final Vertx vertx = Vertx.vertx();
    private final EventBus eventBus = vertx.eventBus();

    VertxEventBusBenchTarget() {
        eventBus.registerDefaultCodec(BenchEvent.class, new LocalCodec());
        vertx.deployVerticle(new BenchVerticle(statistics)).toCompletionStage().toCompletableFuture().join();
    }

    @Override
    public void fire(BenchEvent event) {
        eventBus.send(ADDRESS, event);
    }

    @Override
    public long received() {
        return statistics.received();
    }

    @Override
    public void close() {
        vertx.close().toCompletionStage().toCompletableFuture().join();
    }

    static final class BenchVerticle extends AbstractVerticle {
        private final BenchStatistics statistics;

        BenchVerticle(BenchStatistics statistics) {
            this.statistics = statistics;
        }

        @Override
        public void start() {
            vertx.eventBus().<BenchEvent>localConsumer(ADDRESS, message -> statistics.record(message.body()));
        }
    }

    // local delivery only - the event objects aren't copied
    static final class LocalCodec implements MessageCodec<BenchEvent, BenchEvent> {
        @Override
        public void encodeToWire(Buffer buffer, BenchEvent event) {
            throw new UnsupportedOperationException("local delivery only");
        }

        @Override
        public BenchEvent decodeFromWire(int position, Buffer buffer) {
            throw new UnsupportedOperationException("local delivery only");
        }

        @Override
        public BenchEvent transform(BenchEvent event) {
            return event;
        }

        @Override
        public String name() {
            return "bench-event";
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
