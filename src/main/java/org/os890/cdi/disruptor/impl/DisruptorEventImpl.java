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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.util.Set;

/**
 * Default implementation of {@link DisruptorEvent} - instances are immutable.
 *
 * @param <T> the type of the event
 */
final class DisruptorEventImpl<T> implements DisruptorEvent<T>, Serializable {
    private static final long serialVersionUID = 1L;

    private final Set<Annotation> qualifiers;

    private transient DisruptorExtension extension;

    DisruptorEventImpl(DisruptorExtension extension, Set<Annotation> qualifiers) {
        this.extension = extension;
        this.qualifiers = qualifiers;
    }

    @Override
    public void fire(T event) {
        extension.dispatcher().dispatch(event, qualifiers, true);
    }

    @Override
    public boolean tryFire(T event) {
        return extension.dispatcher().dispatch(event, qualifiers, false);
    }

    @Override
    public DisruptorEvent<T> select(Annotation... additionalQualifiers) {
        return new DisruptorEventImpl<>(extension,
                Qualifiers.merge(qualifiers, additionalQualifiers, extension.beanManager()));
    }

    @Override
    public <U extends T> DisruptorEvent<U> select(Class<U> subtype, Annotation... additionalQualifiers) {
        return new DisruptorEventImpl<>(extension,
                Qualifiers.merge(qualifiers, additionalQualifiers, extension.beanManager()));
    }

    @Override
    public String toString() {
        return "DisruptorEvent" + qualifiers;
    }

    private void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject();
        extension = CDI.current().getBeanManager().getExtension(DisruptorExtension.class);
    }
}
