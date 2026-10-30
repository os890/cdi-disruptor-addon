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

import org.os890.cdi.disruptor.CircuitBreaker;
import org.os890.cdi.disruptor.RingBufferSettings;

import java.util.logging.Logger;

/**
 * State of the {@link CircuitBreaker} of an observer method (closed, open, half-open).
 * <p>
 * Only used by the event-handler thread of the observer method - therefore no synchronization is needed.
 */
final class ObserverCircuitBreaker {
    private static final Logger LOG = Logger.getLogger(ObserverCircuitBreaker.class.getName());

    private final String observerMethod;
    private final int failureThreshold;
    private final long delayNanos;

    private int consecutiveFailures;
    private boolean open;
    private long openUntil;
    private long skippedEvents;

    ObserverCircuitBreaker(String observerMethod, RingBufferSettings settings) {
        this.observerMethod = observerMethod;
        this.failureThreshold = settings.circuitBreakerFailureThreshold();
        this.delayNanos = settings.circuitBreakerDelay().toNanos();
    }

    /**
     * @return true if the observer method should get notified - false if the event gets skipped, because the
     *         circuit-breaker is open (after the delay one event gets passed to the observer method as trial)
     */
    boolean allowsNotification() {
        if (open && System.nanoTime() - openUntil < 0) {
            skippedEvents++;
            return false;
        }
        return true;
    }

    void onSuccess() {
        if (open) {
            open = false;
            LOG.info("the circuit-breaker of the observer method " + observerMethod + " is closed again - "
                + skippedEvents + " event(s) were skipped");
            skippedEvents = 0;
        }
        consecutiveFailures = 0;
    }

    void onFailure() {
        if (open) {
            // the trial failed - stay open for another delay
            openUntil = System.nanoTime() + delayNanos;
            return;
        }
        consecutiveFailures++;
        if (consecutiveFailures >= failureThreshold) {
            open = true;
            openUntil = System.nanoTime() + delayNanos;
            LOG.warning("the circuit-breaker of the observer method " + observerMethod + " is open after "
                + failureThreshold + " consecutive failures - events get skipped");
        }
    }
}
