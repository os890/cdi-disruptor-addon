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
package org.os890.cdi.disruptor;

/**
 * Thrown by {@link DisruptorEvent#fire(Object)} of injection points with {@link CircuitBreaker},
 * if the ring-buffer is full or the circuit-breaker is open.
 */
public class EventRejectedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final String ringBufferName;
    private final boolean circuitBreakerOpen;

    /**
     * @param ringBufferName     name of the ring-buffer
     * @param circuitBreakerOpen true if the event was rejected, because the circuit-breaker is open
     */
    public EventRejectedException(String ringBufferName, boolean circuitBreakerOpen) {
        super(circuitBreakerOpen
            ? "the circuit-breaker of the ring-buffer '" + ringBufferName + "' is open"
            : "the ring-buffer '" + ringBufferName + "' is full");
        this.ringBufferName = ringBufferName;
        this.circuitBreakerOpen = circuitBreakerOpen;
    }

    /**
     * @return name of the ring-buffer
     */
    public String getRingBufferName() {
        return ringBufferName;
    }

    /**
     * @return true if the event was rejected, because the circuit-breaker is open -
     *         false if the ring-buffer was full
     */
    public boolean isCircuitBreakerOpen() {
        return circuitBreakerOpen;
    }
}
