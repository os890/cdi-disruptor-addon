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

import jakarta.enterprise.inject.spi.CDI;
import org.os890.cdi.disruptor.DisruptorEvent;
import org.os890.cdi.disruptor.EventRejectedException;
import org.os890.cdi.disruptor.RingBuffer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Set;

/**
 * Default implementation of {@link DisruptorEvent} - instances are immutable.
 *
 * @param <T> the type of the event
 */
public final class DisruptorEventImpl<T> implements DisruptorEvent<T>, Serializable {
    private static final long serialVersionUID = 1L;

    private final String ringBufferName;
    private final Set<Annotation> qualifiers;
    private final boolean circuitBreakerEnabled;

    private transient DisruptorRuntime runtime;

    // racy single-check idiom: the publisher is thread-safe and its fields are final
    private transient DisruptorDispatcher.Publisher publisher;
    private transient CircuitBreakerState circuitBreaker;

    /**
     * @param runtime               runtime of the addon
     * @param ringBufferName        name of the ring-buffer
     * @param qualifiers            normalized qualifiers of the events
     * @param circuitBreakerEnabled true if the injection point is annotated with
     *                              {@link org.os890.cdi.disruptor.CircuitBreaker}
     */
    public DisruptorEventImpl(DisruptorRuntime runtime,
                              String ringBufferName,
                              Set<Annotation> qualifiers,
                              boolean circuitBreakerEnabled) {
        this.runtime = runtime;
        this.ringBufferName = ringBufferName;
        this.qualifiers = qualifiers;
        this.circuitBreakerEnabled = circuitBreakerEnabled;
    }

    @Override
    public void fire(T event) {
        if (!circuitBreakerEnabled) {
            publisher().publish(event, true);
            return;
        }
        CircuitBreakerState currentCircuitBreaker = circuitBreaker();
        if (!currentCircuitBreaker.publish(publisher(), event)) {
            throw new EventRejectedException(ringBufferName, currentCircuitBreaker.isOpen());
        }
    }

    @Override
    public boolean tryFire(T event) {
        if (!circuitBreakerEnabled) {
            return publisher().publish(event, false);
        }
        return circuitBreaker().publish(publisher(), event);
    }

    @Override
    public DisruptorEvent<T> select(Annotation... additionalQualifiers) {
        return child(additionalQualifiers);
    }

    @Override
    public <U extends T> DisruptorEvent<U> select(Class<U> subtype, Annotation... additionalQualifiers) {
        return child(additionalQualifiers);
    }

    @Override
    public String toString() {
        return "DisruptorEvent[ringBuffer=" + ringBufferName + ", qualifiers=" + qualifiers
            + ", circuitBreaker=" + circuitBreakerEnabled + "]";
    }

    private DisruptorDispatcher.Publisher publisher() {
        DisruptorDispatcher.Publisher result = publisher;
        if (result == null) {
            result = runtime.dispatcher(ringBufferName).publisher(qualifiers);
            publisher = result;
        }
        return result;
    }

    private CircuitBreakerState circuitBreaker() {
        CircuitBreakerState result = circuitBreaker;
        if (result == null) {
            result = runtime.dispatcher(ringBufferName).circuitBreaker();
            circuitBreaker = result;
        }
        return result;
    }

    private <U> DisruptorEventImpl<U> child(Annotation[] additionalQualifiers) {
        String childRingBufferName = ringBufferName;
        RingBuffer ringBuffer = Qualifiers.ringBufferQualifier(Arrays.asList(additionalQualifiers));
        if (ringBuffer != null) {
            if (!RingBuffer.DEFAULT.equals(ringBufferName) && !ringBufferName.equals(ringBuffer.name())) {
                throw new IllegalArgumentException("the ring-buffer '" + ringBufferName + "' is already selected");
            }
            childRingBufferName = ringBuffer.name();
        }
        return new DisruptorEventImpl<>(runtime, childRingBufferName,
            Qualifiers.merge(qualifiers, additionalQualifiers, runtime::isQualifier), circuitBreakerEnabled);
    }

    private void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject();
        runtime = CDI.current().select(DisruptorRuntime.class).get();
    }
}
