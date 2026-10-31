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

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.os890.cdi.disruptor.DisruptorEvent;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Manual throughput check: {@code mvn test -Dtest=ThroughputTest -Dperf=true}.
 */
@EnabledIfSystemProperty(named = "perf", matches = "true")
class ThroughputTest {
    private static final int EVENTS = 20_000_000;
    private static final int BURST_SIZE = 60_000;

    @Test
    void measureFireThroughput() throws InterruptedException {
        try (SeContainer container = SeContainerInitializer.newInstance()
                .addBeanClasses(PerfObserver.class, PerfPublisher.class).initialize()) {
            DisruptorEvent<Long> tickEvent = container.select(PerfPublisher.class).get().tickEvent();
            PerfObserver observer = container.select(PerfObserver.class).get();

            for (int round = 1; round <= 3; round++) {
                long start = System.nanoTime();
                for (long i = 0; i < EVENTS; i++) {
                    tickEvent.fire(i);
                }
                long fired = System.nanoTime();
                long expected = (long) round * EVENTS;
                awaitReceived(observer, expected);
                long done = System.nanoTime();
                System.out.printf("PERF round %d: fire %.1f ns/event, end-to-end %.1f M events/s%n", round,
                    (fired - start) / (double) EVENTS, EVENTS / ((done - start) / 1000.0));
            }

            // producer only: bursts which fit into the (empty) ring-buffer
            long fireNanos = 0;
            long published = observer.received();
            for (int burst = 0; burst < 400; burst++) {
                long start = System.nanoTime();
                for (long i = 0; i < BURST_SIZE; i++) {
                    tickEvent.fire(i);
                }
                if (burst >= 100) {
                    fireNanos += System.nanoTime() - start;
                }
                published += BURST_SIZE;
                awaitReceived(observer, published);
            }
            System.out.printf("PERF producer only: fire %.1f ns/event%n", fireNanos / (300.0 * BURST_SIZE));
        }
    }

    private static void awaitReceived(PerfObserver observer, long expected) {
        long timeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (observer.received() < expected) {
            if (System.nanoTime() > timeout) {
                fail("received " + observer.received() + " of " + expected + " events");
            }
            Thread.onSpinWait();
        }
    }
}
