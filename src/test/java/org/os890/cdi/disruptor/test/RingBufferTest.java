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
import org.os890.cdi.disruptor.RingBuffer;
import org.os890.cdi.disruptor.test.beans.AuditObserver;
import org.os890.cdi.disruptor.test.beans.BlockingTask;
import org.os890.cdi.disruptor.test.beans.EventRecorder;
import org.os890.cdi.disruptor.test.beans.Order;
import org.os890.cdi.disruptor.test.beans.OrderObserver;
import org.os890.cdi.disruptor.test.beans.OrderPublisher;
import org.os890.cdi.disruptor.test.beans.RingBufferPublisher;
import org.os890.cdi.disruptor.test.beans.TestDisruptorConfig;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RingBufferTest {
    // see META-INF/microprofile-config.properties
    private static final int TINY_SIZE = isMicroProfileConfigAvailable() ? 4 : 8;

    private SeContainer container;
    private OrderPublisher orderPublisher;
    private RingBufferPublisher ringBufferPublisher;
    private EventRecorder recorder;

    @BeforeEach
    void startContainer() {
        container = SeContainerInitializer.newInstance().initialize();
        orderPublisher = container.select(OrderPublisher.class).get();
        ringBufferPublisher = container.select(RingBufferPublisher.class).get();
        recorder = container.select(EventRecorder.class).get();
    }

    @AfterEach
    void stopContainer() {
        container.close();
    }

    @Test
    void eventsGetOnlyDeliveredToObserversOfTheSameRingBuffer() throws InterruptedException {
        ringBufferPublisher.auditOrderEvent().fire(new Order("audit-1"));
        orderPublisher.orderEvent().fire(new Order("default-1"));
        orderPublisher.orderEvent().select(RingBuffer.Literal.of("audit")).fire(new Order("audit-2"));
        orderPublisher.orderEvent().fire(new Order("default-2"));

        assertEquals(List.of("default-1", "default-2"), recorder.await(OrderObserver.ORDERS, 2));
        assertEquals(List.of("audit-1", "audit-2"), recorder.await(AuditObserver.AUDIT_ORDERS, 2));
        assertTrue(recorder.await(AuditObserver.AUDIT_THREADS, 2).stream()
            .allMatch(threadName -> threadName.toString().startsWith("cdi-disruptor-audit-")));
    }

    @Test
    void annotationConfiguresTheRingBuffer() {
        assertEquals(2, capacity(ringBufferPublisher.smallEvent()));
    }

    @Test
    void microProfileConfigOverridesTheAnnotation() {
        assertEquals(TINY_SIZE, capacity(ringBufferPublisher.tinyEvent()));
    }

    @Test
    void disruptorConfigOverridesMicroProfileConfig() {
        assertEquals(TestDisruptorConfig.CUSTOM_SIZE, capacity(ringBufferPublisher.customEvent()));
    }

    @Test
    void injectionPointsWithTheSameNameShareOneRingBuffer() {
        CountDownLatch release = new CountDownLatch(1);
        try {
            assertEquals(TINY_SIZE, fill(ringBufferPublisher.tinyEvent(), release));
            assertFalse(ringBufferPublisher.premiumTinyEvent().tryFire(new BlockingTask(release)));
        } finally {
            release.countDown();
        }
    }

    @Test
    void fullRingBufferDoesntAffectOtherRingBuffers() throws InterruptedException {
        CountDownLatch release = new CountDownLatch(1);
        try {
            // the observer of the ring-buffer "small" is stuck and its ring-buffer is full
            fill(ringBufferPublisher.smallEvent(), release);
            assertFalse(ringBufferPublisher.smallEvent().tryFire(new BlockingTask(release)));

            orderPublisher.orderEvent().fire(new Order("default-1"));
            ringBufferPublisher.auditOrderEvent().fire(new Order("audit-1"));

            assertEquals(List.of("default-1"), recorder.await(OrderObserver.ORDERS, 1));
            assertEquals(List.of("audit-1"), recorder.await(AuditObserver.AUDIT_ORDERS, 1));
        } finally {
            release.countDown();
        }
    }

    @Test
    void eventsForUndeclaredRingBuffersHaveNoObservers() {
        assertTrue(orderPublisher.anyEvent().select(RingBuffer.Literal.of("undeclared")).tryFire("ignored"));
    }

    @Test
    void anotherRingBufferCantBeSelected() {
        DisruptorEvent<Order> auditOrderEvent = ringBufferPublisher.auditOrderEvent();

        assertThrows(IllegalArgumentException.class, () -> auditOrderEvent.select(RingBuffer.Literal.of("other")));
    }

    private static int capacity(DisruptorEvent<BlockingTask> event) {
        CountDownLatch release = new CountDownLatch(1);
        try {
            return fill(event, release);
        } finally {
            release.countDown();
        }
    }

    // the first event blocks the observer, therefore no slot gets released
    private static int fill(DisruptorEvent<BlockingTask> event, CountDownLatch release) {
        int publishedEvents = 0;
        while (publishedEvents < 1_000 && event.tryFire(new BlockingTask(release))) {
            publishedEvents++;
        }
        return publishedEvents;
    }

    private static boolean isMicroProfileConfigAvailable() {
        try {
            Class.forName("org.eclipse.microprofile.config.ConfigProvider");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
