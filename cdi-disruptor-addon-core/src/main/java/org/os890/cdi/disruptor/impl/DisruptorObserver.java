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

import java.lang.annotation.Annotation;
import java.util.Set;

/**
 * Observer method (of a parameter annotated with {@link org.os890.cdi.disruptor.ObservesDisruptor}) as seen by the
 * {@link DisruptorDispatcher} - implemented by the portable extension and the build-compatible extension.
 * <p>
 * Internal API - not intended to be used by applications.
 */
public interface DisruptorObserver {
    /**
     * @return name of the ring-buffer of the observer method
     */
    String ringBufferName();

    /**
     * @param eventType       runtime type of the event
     * @param eventQualifiers normalized qualifiers of the event
     * @return true if the observer method observes such events
     */
    boolean observes(Class<?> eventType, Set<Annotation> eventQualifiers);

    /**
     * Invokes the observer method - only called by the event-handler thread of the observer method.
     *
     * @param event the event
     * @throws Exception exception of the observer method
     */
    void notify(Object event) throws Exception;

    /**
     * @return true if a request-context has to be active during the notification
     */
    boolean isRequestContextRequired();

    /**
     * @return true if the observer method is protected by a {@link org.os890.cdi.disruptor.CircuitBreaker}
     */
    boolean isCircuitBreakerEnabled();
}
