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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * State of the {@link CircuitBreaker} of a ring-buffer (closed, open, half-open).
 * <p>
 * The hot path (closed and an accepted event) only reads volatile fields.
 */
final class CircuitBreakerState {
    private static final Logger LOG = Logger.getLogger(CircuitBreakerState.class.getName());

    private final String ringBufferName;
    private final int failureThreshold;
    private final long delayNanos;
    private final long maxWaitNanos;

    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    private final AtomicBoolean trialInProgress = new AtomicBoolean();
    private volatile long openUntil;
    // written after openUntil - readers which see open == true also see the corresponding openUntil
    private volatile boolean open;

    CircuitBreakerState(RingBufferSettings settings) {
        this.ringBufferName = settings.name();
        this.failureThreshold = settings.circuitBreakerFailureThreshold();
        this.delayNanos = settings.circuitBreakerDelay().toNanos();
        this.maxWaitNanos = settings.circuitBreakerMaxWait().toNanos();
    }

    /**
     * @param publisher publisher of the event
     * @param event     the event
     * @return true if the event was accepted - false if it was rejected (the ring-buffer is full or this
     *         circuit-breaker is open)
     */
    boolean publish(DisruptorDispatcher.Publisher publisher, Object event) {
        boolean trial = false;
        if (open) {
            // open: reject until the delay is over, afterwards only one event gets published as trial (half-open)
            if (System.nanoTime() - openUntil < 0 || !trialInProgress.compareAndSet(false, true)) {
                return false;
            }
            trial = true;
        }

        boolean published;
        try {
            published = publisher.publish(event, maxWaitNanos);
        } catch (RuntimeException e) {
            if (trial) {
                trialInProgress.set(false);
            }
            throw e;
        }

        if (published) {
            onAccepted(trial);
        } else {
            onRejected(trial);
        }
        return published;
    }

    boolean isOpen() {
        return open;
    }

    private void onAccepted(boolean trial) {
        if (trial) {
            consecutiveFailures.set(0);
            open = false;
            trialInProgress.set(false);
            LOG.info("the circuit-breaker of the ring-buffer '" + ringBufferName + "' is closed again");
        } else if (consecutiveFailures.get() != 0) {
            consecutiveFailures.set(0);
        }
    }

    private void onRejected(boolean trial) {
        if (trial) {
            // the trial failed - stay open for another delay
            openUntil = System.nanoTime() + delayNanos;
            trialInProgress.set(false);
            return;
        }
        if (consecutiveFailures.incrementAndGet() >= failureThreshold && !open) {
            openUntil = System.nanoTime() + delayNanos;
            open = true;
            LOG.warning("the circuit-breaker of the ring-buffer '" + ringBufferName + "' is open after "
                + failureThreshold + " rejected events");
        }
    }
}
