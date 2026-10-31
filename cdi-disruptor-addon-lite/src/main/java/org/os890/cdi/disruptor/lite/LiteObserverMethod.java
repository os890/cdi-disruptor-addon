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
package org.os890.cdi.disruptor.lite;

import jakarta.enterprise.invoke.Invoker;
import org.os890.cdi.disruptor.impl.DisruptorObserver;
import org.os890.cdi.disruptor.impl.Qualifiers;

import java.lang.annotation.Annotation;
import java.util.Set;

/**
 * Observer method which gets invoked via a CDI invoker (instance- and argument-lookup by the container).
 * <p>
 * The request-context (if required) gets activated by the interceptor {@code @ActivateRequestContext}, which is
 * added by {@link DisruptorBuildCompatibleExtension} - therefore the dispatcher doesn't need to activate it.
 */
final class LiteObserverMethod implements DisruptorObserver {
    private final Invoker<Object, Object> invoker;
    private final Class<?> observedType;
    private final Set<Annotation> observedQualifiers;
    private final String ringBufferName;
    private final boolean circuitBreakerEnabled;
    private final int eventParameterPosition;
    private final int parameterCount;
    private final String description;

    @SuppressWarnings("unchecked")
    LiteObserverMethod(Invoker<?, ?> invoker,
                       Class<?> observedType,
                       Set<Annotation> observedQualifiers,
                       String ringBufferName,
                       boolean circuitBreakerEnabled,
                       int eventParameterPosition,
                       int parameterCount,
                       String description) {
        this.invoker = (Invoker<Object, Object>) invoker;
        this.observedType = observedType;
        this.observedQualifiers = observedQualifiers;
        this.ringBufferName = ringBufferName;
        this.circuitBreakerEnabled = circuitBreakerEnabled;
        this.eventParameterPosition = eventParameterPosition;
        this.parameterCount = parameterCount;
        this.description = description;
    }

    @Override
    public String ringBufferName() {
        return ringBufferName;
    }

    @Override
    public boolean observes(Class<?> eventType, Set<Annotation> eventQualifiers) {
        return Qualifiers.observes(observedType, observedQualifiers, eventType, eventQualifiers,
            Qualifiers::areEquivalent);
    }

    @Override
    public void notify(Object event) throws Exception {
        // all other arguments get looked up by the invoker
        Object[] arguments = new Object[parameterCount];
        arguments[eventParameterPosition] = event;
        invoker.invoke(null, arguments);
    }

    @Override
    public boolean isRequestContextRequired() {
        return false;
    }

    @Override
    public boolean isCircuitBreakerEnabled() {
        return circuitBreakerEnabled;
    }

    @Override
    public String toString() {
        return description;
    }
}
