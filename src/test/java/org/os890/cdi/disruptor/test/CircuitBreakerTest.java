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
package org.os890.cdi.disruptor.test;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.os890.cdi.disruptor.DisruptorEvent;
import org.os890.cdi.disruptor.EventRejectedException;
import org.os890.cdi.disruptor.test.beans.BlockingTask;
import org.os890.cdi.disruptor.test.beans.CircuitBreakerPublisher;
import org.os890.cdi.disruptor.test.beans.EventRecorder;
import org.os890.cdi.disruptor.test.beans.FlakyObserver;
import org.os890.cdi.disruptor.test.beans.FlakyTask;
import org.os890.cdi.disruptor.test.beans.TestDisruptorConfig;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CircuitBreakerTest {
    private SeContainer container;
    private CircuitBreakerPublisher publisher;

    @BeforeEach
    void startContainer() {
        container = SeContainerInitializer.newInstance().initialize();
        publisher = container.select(CircuitBreakerPublisher.class).get();
    }

    @AfterEach
    void stopContainer() {
        container.close();
    }

    @Test
    void circuitBreakerProtectsTheProducer() throws InterruptedException {
        DisruptorEvent<BlockingTask> guardedEvent = publisher.guardedEvent();
        CountDownLatch release = new CountDownLatch(1);
        try {
            // closed: the ring-buffer (size 2) gets filled - the observer is stuck at the first event
            guardedEvent.fire(new BlockingTask(release));
            guardedEvent.fire(new BlockingTask(release));

            // closed: the ring-buffer is full - the events get rejected instead of blocking the producer
            EventRejectedException rejected =
                assertThrows(EventRejectedException.class, () -> guardedEvent.fire(new BlockingTask(release)));
            assertFalse(rejected.isCircuitBreakerOpen());
            // second consecutive rejection - opens the circuit-breaker (failure-threshold 2)
            assertFalse(guardedEvent.tryFire(new BlockingTask(release)));

            // the observer processes the pending events
            release.countDown();
            awaitFreeSlot(publisher.unguardedEvent());

            // open: events get rejected immediately, although there are free slots
            rejected = assertThrows(EventRejectedException.class, () -> guardedEvent.fire(new BlockingTask(release)));
            assertTrue(rejected.isCircuitBreakerOpen());
            assertFalse(guardedEvent.tryFire(new BlockingTask(release)));

            // half-open after the delay: the trial gets accepted and closes the circuit-breaker
            Thread.sleep(TestDisruptorConfig.GUARDED_DELAY.toMillis() + 100);
            guardedEvent.fire(new BlockingTask(release));
            assertTrue(guardedEvent.tryFire(new BlockingTask(release)));
        } finally {
            release.countDown();
        }
    }

    @Test
    void circuitBreakerSkipsFailingObserver() throws InterruptedException {
        DisruptorEvent<FlakyTask> flakyEvent = publisher.flakyEvent();
        EventRecorder recorder = container.select(EventRecorder.class).get();

        // closed: two consecutive failures open the circuit-breaker (failure-threshold 2)
        flakyEvent.fire(new FlakyTask("fail-1", true));
        flakyEvent.fire(new FlakyTask("fail-2", true));
        // open: the observer method doesn't get notified
        flakyEvent.fire(new FlakyTask("skipped", false));
        recorder.await(FlakyObserver.NOTIFICATIONS, 2);

        // half-open after the delay: the trial succeeds and closes the circuit-breaker
        Thread.sleep(TestDisruptorConfig.GUARDED_DELAY.toMillis() + 100);
        flakyEvent.fire(new FlakyTask("trial", false));
        flakyEvent.fire(new FlakyTask("closed", false));

        assertEquals(List.of("fail-1", "fail-2", "trial", "closed"), recorder.await(FlakyObserver.NOTIFICATIONS, 4));
    }

    // injection points without @CircuitBreaker aren't affected by an open circuit-breaker
    private static void awaitFreeSlot(DisruptorEvent<BlockingTask> unguardedEvent) throws InterruptedException {
        CountDownLatch released = new CountDownLatch(0);
        long timeout = System.currentTimeMillis() + 10_000;
        while (!unguardedEvent.tryFire(new BlockingTask(released))) {
            if (System.currentTimeMillis() > timeout) {
                throw new AssertionError("the observer didn't process the pending events");
            }
            Thread.sleep(5);
        }
    }
}
