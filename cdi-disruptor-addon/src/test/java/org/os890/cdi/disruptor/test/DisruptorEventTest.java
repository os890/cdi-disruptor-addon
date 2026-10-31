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

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.os890.cdi.disruptor.DisruptorEvent;
import org.os890.cdi.disruptor.test.beans.EventRecorder;
import org.os890.cdi.disruptor.test.beans.Order;
import org.os890.cdi.disruptor.test.beans.OrderObserver;
import org.os890.cdi.disruptor.test.beans.OrderPublisher;
import org.os890.cdi.disruptor.test.beans.Premium;
import org.os890.cdi.disruptor.test.beans.PremiumOrderObserver;
import org.os890.cdi.disruptor.test.beans.Region;
import org.os890.cdi.disruptor.test.beans.RegionObserver;
import org.os890.cdi.disruptor.test.beans.SlowTask;
import org.os890.cdi.disruptor.test.beans.SlowTaskObserver;
import org.os890.cdi.disruptor.test.beans.TextObserver;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisruptorEventTest {
    private SeContainer container;
    private OrderPublisher publisher;
    private EventRecorder recorder;

    @BeforeEach
    void startContainer() {
        container = newContainer();
        publisher = container.select(OrderPublisher.class).get();
        recorder = container.select(EventRecorder.class).get();
    }

    @AfterEach
    void stopContainer() {
        if (container.isRunning()) {
            container.close();
        }
    }

    @Test
    void observerReceivesEventsInOrderOnDisruptorThread() throws InterruptedException {
        List<String> expectedIds = IntStream.range(0, 5_000).mapToObj(String::valueOf).collect(Collectors.toList());
        expectedIds.forEach(id -> publisher.orderEvent().fire(new Order(id)));

        assertEquals(expectedIds, recorder.await(OrderObserver.ORDERS, expectedIds.size()));

        String callerThread = Thread.currentThread().getName();
        List<Object> observerThreads = recorder.await(OrderObserver.ORDER_THREADS, expectedIds.size());
        assertTrue(observerThreads.stream().noneMatch(callerThread::equals));
        assertEquals(1, observerThreads.stream().distinct().count(), "one thread per observer method expected");
    }

    @Test
    void qualifiedObserverReceivesOnlyQualifiedEvents() throws InterruptedException {
        publisher.orderEvent().fire(new Order("1"));
        publisher.premiumOrderEvent().fire(new Order("2"));
        publisher.orderEvent().select(Premium.Literal.INSTANCE).fire(new Order("3"));

        // observer without qualifiers observes all events
        assertEquals(List.of("1", "2", "3"), recorder.await(OrderObserver.ORDERS, 3));
        // event "1" would be received before "2" if the qualifier were ignored
        assertEquals(List.of("2", "3"), recorder.await(PremiumOrderObserver.PREMIUM_ORDERS, 2));
    }

    @Test
    void observerMethodGetsInvokedLikeCdiObserver() throws InterruptedException {
        publisher.premiumOrderEvent().fire(new Order("1"));
        publisher.premiumOrderEvent().fire(new Order("2"));

        assertEquals(List.of("1", "2"), recorder.await(PremiumOrderObserver.PREMIUM_ORDERS, 2));
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
    void processedEventsDontStayReferencedByTheRingBuffer() throws InterruptedException {
        // observed by two observer methods (OrderObserver and PremiumOrderObserver)
        WeakReference<Order> processedOrder = firePremiumOrder("collectable");
        recorder.await(OrderObserver.ORDERS, 1);
        recorder.await(PremiumOrderObserver.PREMIUM_ORDERS, 1);

        long timeout = System.currentTimeMillis() + 10_000;
        while (processedOrder.get() != null && System.currentTimeMillis() < timeout) {
            System.gc();
            Thread.sleep(10);
        }
        assertNull(processedOrder.get(), "the processed event is still referenced");
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
        publisher.anyEvent().fire(new Order("1"));

        assertEquals(List.of("hello", "world"), recorder.await(TextObserver.TEXTS, 2));
        assertEquals(List.of("1"), recorder.await(OrderObserver.ORDERS, 1));
    }

    @Test
    void pendingEventsGetProcessedDuringShutdown() {
        Queue<Object> processedTasks = recorder.received(SlowTaskObserver.SLOW_TASKS);
        for (int i = 0; i < 200; i++) {
            publisher.slowTaskEvent().fire(new SlowTask(i));
        }
        assertTrue(processedTasks.size() < 200, "test requires pending events");

        container.close();

        assertEquals(IntStream.range(0, 200).boxed().collect(Collectors.toList()), new ArrayList<>(processedTasks));
    }

    @Test
    void firingAfterShutdownFails() {
        DisruptorEvent<Order> orderEvent = publisher.orderEvent();
        container.close();

        assertThrows(IllegalStateException.class, () -> orderEvent.fire(new Order("1")));
    }

    @Test
    void tryFireWithoutObserverSucceeds() {
        assertTrue(publisher.anyEvent().tryFire(42));
    }

    @Test
    void selectRejectsNonQualifiers() {
        ApplicationScoped scope = OrderObserver.class.getAnnotation(ApplicationScoped.class);

        assertThrows(IllegalArgumentException.class, () -> publisher.orderEvent().select(scope));
        assertThrows(IllegalArgumentException.class,
            () -> publisher.premiumOrderEvent().select(Premium.Literal.INSTANCE));
    }


    // separate method - the caller mustn't keep a reference to the event
    private WeakReference<Order> firePremiumOrder(String id) {
        Order order = new Order(id);
        publisher.premiumOrderEvent().fire(order);
        return new WeakReference<>(order);
    }

    private static SeContainer newContainer() {
        // bean discovery stays enabled - Weld doesn't load portable extensions otherwise
        return SeContainerInitializer.newInstance().initialize();
    }
}
