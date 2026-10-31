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

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.os890.cdi.disruptor.DisruptorEvent;
import org.os890.cdi.disruptor.EventRejectedException;
import org.os890.cdi.disruptor.RingBuffer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The build-compatible extension with Quarkus.
 */
@QuarkusTest
class LiteDisruptorEventTest {
    @Inject
    private EventPublisher publisher;

    @Inject
    private EventRecorder recorder;

    @BeforeEach
    void clearRecordedEvents() {
        recorder.clear();
    }

    @Test
    void observerReceivesEventsInOrderOnDisruptorThread() throws InterruptedException {
        List<String> expectedIds = IntStream.range(0, 1_000).mapToObj(String::valueOf).collect(Collectors.toList());
        expectedIds.forEach(id -> publisher.orderEvent().fire(new Order(id)));

        assertEquals(expectedIds, recorder.await(OrderObserver.ORDERS, expectedIds.size()));
        assertTrue(recorder.await(OrderObserver.ORDER_THREADS, expectedIds.size()).stream()
            .allMatch(threadName -> threadName.toString().startsWith("cdi-disruptor-default-")));
    }

    @Test
    void qualifiedObserverReceivesOnlyQualifiedEvents() throws InterruptedException {
        publisher.orderEvent().fire(new Order("1"));
        publisher.premiumOrderEvent().fire(new Order("2"));
        publisher.orderEvent().select(Premium.Literal.INSTANCE).fire(new Order("3"));

        assertEquals(List.of("1", "2", "3"), recorder.await(OrderObserver.ORDERS, 3));
        assertEquals(List.of("2", "3"), recorder.await(PremiumOrderObserver.PREMIUM_ORDERS, 2));
    }

    @Test
    void observerMethodGetsInvokedLikeCdiObserver() throws InterruptedException {
        publisher.premiumOrderEvent().fire(new Order("1"));
        publisher.premiumOrderEvent().fire(new Order("2"));

        // new request-context per event (opt-in)
        assertEquals(List.of(1, 1), recorder.await(PremiumOrderObserver.REQUEST_COUNTS, 2));
        // the @Dependent observer instance gets destroyed after every notification
        assertEquals(2, recorder.await(PremiumOrderObserver.DESTROYED, 2).size());
        // DisruptorEvent as additional parameter of an observer method + static observer method
        assertEquals(List.of("premium:1", "premium:2"), recorder.await(TextObserver.TEXTS, 2));
    }

    @Test
    void requestContextIsOptIn() throws InterruptedException {
        publisher.orderEvent().fire(new Order("1"));

        assertEquals(List.of(false), recorder.await(OrderObserver.REQUEST_CONTEXT_ACTIVE, 1));
    }

    @Test
    void nonBindingMembersOfQualifiersGetIgnored() throws InterruptedException {
        publisher.orderEvent().select(Region.Literal.of("us", "producer")).fire(new Order("us-1"));
        publisher.orderEvent().select(Region.Literal.of("eu", "producer")).fire(new Order("eu-1"));

        assertEquals(List.of("eu-1"), recorder.await(RegionObserver.EU_ORDERS, 1));
    }

    @Test
    void observerOfSuperTypeReceivesSubTypes() throws InterruptedException {
        publisher.anyEvent().fire("hello");
        publisher.anyEvent().fire(new StringBuilder("world"));

        assertEquals(List.of("hello", "world"), recorder.await(TextObserver.TEXTS, 2));
    }

    @Test
    void eventsGetOnlyDeliveredToObserversOfTheSameRingBuffer() throws InterruptedException {
        publisher.auditOrderEvent().fire(new Order("audit-1"));
        publisher.orderEvent().fire(new Order("default-1"));
        publisher.orderEvent().select(RingBuffer.Literal.of("audit")).fire(new Order("audit-2"));
        publisher.orderEvent().fire(new Order("default-2"));

        assertEquals(List.of("default-1", "default-2"), recorder.await(OrderObserver.ORDERS, 2));
        assertEquals(List.of("audit-1", "audit-2"), recorder.await(AuditObserver.AUDIT_ORDERS, 2));
        assertTrue(recorder.await(AuditObserver.AUDIT_THREADS, 2).stream()
            .allMatch(threadName -> threadName.toString().startsWith("cdi-disruptor-audit-")));
    }

    @Test
    void circuitBreakerProtectsTheProducer() throws InterruptedException {
        DisruptorEvent<BlockingTask> guardedEvent = publisher.guardedEvent();
        CountDownLatch release = new CountDownLatch(1);
        try {
            // the ring-buffer (size 2) gets filled - the observer is stuck at the first event
            guardedEvent.fire(new BlockingTask(release));
            guardedEvent.fire(new BlockingTask(release));

            EventRejectedException rejected =
                assertThrows(EventRejectedException.class, () -> guardedEvent.fire(new BlockingTask(release)));
            assertFalse(rejected.isCircuitBreakerOpen());
            // second consecutive rejection - opens the circuit-breaker (configured via TestDisruptorConfig)
            assertFalse(guardedEvent.tryFire(new BlockingTask(release)));

            release.countDown();
            awaitFreeSlot(publisher.unguardedEvent());

            rejected = assertThrows(EventRejectedException.class, () -> guardedEvent.fire(new BlockingTask(release)));
            assertTrue(rejected.isCircuitBreakerOpen());

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

        // two consecutive failures open the circuit-breaker (configured via application.properties)
        flakyEvent.fire(new FlakyTask("fail-1", true));
        flakyEvent.fire(new FlakyTask("fail-2", true));
        flakyEvent.fire(new FlakyTask("skipped", false));
        recorder.await(FlakyObserver.NOTIFICATIONS, 2);

        Thread.sleep(400);
        flakyEvent.fire(new FlakyTask("trial", false));
        flakyEvent.fire(new FlakyTask("closed", false));

        assertEquals(List.of("fail-1", "fail-2", "trial", "closed"), recorder.await(FlakyObserver.NOTIFICATIONS, 4));
    }

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
