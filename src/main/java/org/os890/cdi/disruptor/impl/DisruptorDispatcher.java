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
package org.os890.cdi.disruptor.impl;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import jakarta.enterprise.context.control.RequestContextController;
import jakarta.enterprise.inject.spi.BeanManager;
import org.os890.cdi.disruptor.RingBufferSettings;

import java.lang.annotation.Annotation;
import java.time.Duration;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Publishes events to a ring-buffer and notifies the observer methods of the ring-buffer.
 * <p>
 * Every observer method has its own {@link EventHandler}, which skips events it doesn't observe.
 * The observer methods which observe an event get resolved once per event-type and qualifiers
 * (see {@link Publisher}).
 */
final class DisruptorDispatcher {
    private static final Logger LOG = Logger.getLogger(DisruptorDispatcher.class.getName());

    private static final EventTranslatorTwoArg<EventSlot, Object, BitSet> EVENT_TRANSLATOR =
        (slot, sequence, payload, recipients) -> {
            slot.payload = payload;
            slot.recipients = recipients;
        };

    private final BeanManager beanManager;
    private final List<DisruptorObserverMethod> observerMethods;
    private final RingBufferSettings settings;
    private final Map<Set<Annotation>, Publisher> publishers = new ConcurrentHashMap<>();

    private Disruptor<EventSlot> disruptor;
    private volatile RingBuffer<EventSlot> ringBuffer;
    private volatile boolean running;

    DisruptorDispatcher(BeanManager beanManager,
                        List<DisruptorObserverMethod> observerMethods,
                        RingBufferSettings settings) {
        this.beanManager = beanManager;
        this.observerMethods = List.copyOf(observerMethods);
        this.settings = settings;
    }

    synchronized void start() {
        if (!observerMethods.isEmpty()) {
            disruptor = new Disruptor<>(EventSlot::new, settings.ringBufferSize(), settings.threadFactory(),
                    settings.producerType(), settings.waitStrategy());
            disruptor.setDefaultExceptionHandler(new LoggingExceptionHandler());

            ObserverEventHandler[] eventHandlers = new ObserverEventHandler[observerMethods.size()];
            for (int i = 0; i < eventHandlers.length; i++) {
                eventHandlers[i] = new ObserverEventHandler(i, observerMethods.get(i));
            }
            // the last handler releases the references to the payload once all observers are done
            disruptor.handleEventsWith(eventHandlers).then((slot, sequence, endOfBatch) -> slot.clear());
            ringBuffer = disruptor.start();
        }
        running = true;
    }

    /**
     * Stops accepting new events and waits (max. {@link RingBufferSettings#shutdownTimeout()})
     * until all pending events are processed.
     */
    synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;

        if (disruptor != null) {
            Duration timeout = settings.shutdownTimeout();
            try {
                disruptor.shutdown(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                LOG.warning("pending events of the ring-buffer '" + settings.name() + "' weren't processed within "
                        + timeout + " - halting the disruptor");
                disruptor.halt();
            }
        }
    }

    /**
     * @param qualifiers normalized qualifiers of the events
     * @return publisher for events with the given qualifiers (cached - the lookup is only needed once per
     *         {@link org.os890.cdi.disruptor.DisruptorEvent} instance)
     */
    Publisher publisher(Set<Annotation> qualifiers) {
        return publishers.computeIfAbsent(qualifiers, Publisher::new);
    }

    /**
     * Hot path of firing events - resolves the observers once per event-type and avoids allocations.
     */
    final class Publisher {
        private final Set<Annotation> qualifiers;
        private final Map<Class<?>, BitSet> recipientsByEventType = new ConcurrentHashMap<>();

        private Publisher(Set<Annotation> qualifiers) {
            this.qualifiers = qualifiers;
        }

        boolean publish(Object event, boolean waitForCapacity) {
            if (event == null) {
                throw new IllegalArgumentException("the event must not be null");
            }
            if (!running) {
                throw new IllegalStateException("the ring-buffer '" + settings.name() + "' isn't running");
            }

            Class<?> eventType = event.getClass();
            BitSet recipients = recipientsByEventType.get(eventType);
            if (recipients == null) {
                recipients = recipientsByEventType.computeIfAbsent(eventType, this::resolveRecipients);
            }
            if (recipients.isEmpty()) {
                return true;
            }
            if (waitForCapacity) {
                ringBuffer.publishEvent(EVENT_TRANSLATOR, event, recipients);
                return true;
            }
            return ringBuffer.tryPublishEvent(EVENT_TRANSLATOR, event, recipients);
        }

        private BitSet resolveRecipients(Class<?> eventType) {
            BitSet result = new BitSet(observerMethods.size());
            for (int i = 0; i < observerMethods.size(); i++) {
                if (observerMethods.get(i).observes(eventType, qualifiers)) {
                    result.set(i);
                }
            }
            return result;
        }
    }

    /**
     * Entry of the ring-buffer.
     */
    static final class EventSlot {
        private Object payload;
        private BitSet recipients;

        void clear() {
            payload = null;
            recipients = null;
        }
    }

    private final class ObserverEventHandler implements EventHandler<EventSlot> {
        private final int index;
        private final DisruptorObserverMethod observerMethod;

        // only used by the thread of this handler (and only for observer methods which require it)
        private RequestContextController requestContextController;

        ObserverEventHandler(int index, DisruptorObserverMethod observerMethod) {
            this.index = index;
            this.observerMethod = observerMethod;
        }

        @Override
        public void onEvent(EventSlot slot, long sequence, boolean endOfBatch) {
            if (!slot.recipients.get(index)) {
                return;
            }

            if (!observerMethod.isRequestContextRequired()) {
                notifyObserverMethod(slot, sequence);
                return;
            }

            if (requestContextController == null) {
                requestContextController = beanManager.createInstance().select(RequestContextController.class).get();
            }
            boolean activated = requestContextController.activate();
            try {
                notifyObserverMethod(slot, sequence);
            } finally {
                if (activated) {
                    requestContextController.deactivate();
                }
            }
        }

        private void notifyObserverMethod(EventSlot slot, long sequence) {
            try {
                observerMethod.notify(slot.payload);
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "observer method " + observerMethod + " failed to process event "
                        + slot.payload + " (sequence " + sequence + ")", e);
            }
        }
    }

    private static final class LoggingExceptionHandler implements ExceptionHandler<EventSlot> {
        @Override
        public void handleEventException(Throwable throwable, long sequence, EventSlot slot) {
            LOG.log(Level.SEVERE, "failed to process event with sequence " + sequence, throwable);
        }

        @Override
        public void handleOnStartException(Throwable throwable) {
            LOG.log(Level.SEVERE, "failed to start event-handler", throwable);
        }

        @Override
        public void handleOnShutdownException(Throwable throwable) {
            LOG.log(Level.SEVERE, "failed to stop event-handler", throwable);
        }
    }
}
