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
final class DisruptorEventImpl<T> implements DisruptorEvent<T>, Serializable {
    private static final long serialVersionUID = 1L;

    private final String ringBufferName;
    private final Set<Annotation> qualifiers;

    private transient DisruptorExtension extension;

    // racy single-check idiom: the publisher is thread-safe and its fields are final
    private transient DisruptorDispatcher.Publisher publisher;

    DisruptorEventImpl(DisruptorExtension extension, String ringBufferName, Set<Annotation> qualifiers) {
        this.extension = extension;
        this.ringBufferName = ringBufferName;
        this.qualifiers = qualifiers;
    }

    @Override
    public void fire(T event) {
        publisher().publish(event, true);
    }

    @Override
    public boolean tryFire(T event) {
        return publisher().publish(event, false);
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
        return "DisruptorEvent[ringBuffer=" + ringBufferName + ", qualifiers=" + qualifiers + "]";
    }

    private DisruptorDispatcher.Publisher publisher() {
        DisruptorDispatcher.Publisher result = publisher;
        if (result == null) {
            result = extension.dispatcher(ringBufferName).publisher(qualifiers);
            publisher = result;
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
        return new DisruptorEventImpl<>(extension, childRingBufferName,
            Qualifiers.merge(qualifiers, additionalQualifiers, extension.beanManager()));
    }

    private void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject();
        extension = CDI.current().getBeanManager().getExtension(DisruptorExtension.class);
    }
}
