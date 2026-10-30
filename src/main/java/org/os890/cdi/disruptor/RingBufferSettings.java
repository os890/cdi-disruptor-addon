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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Settings of a single ring-buffer - see {@link RingBuffer} for the resolution order.
 */
public final class RingBufferSettings {
    private final String name;

    private int ringBufferSize = 1024;
    private WaitStrategy waitStrategy = new BlockingWaitStrategy();
    private ProducerType producerType = ProducerType.MULTI;
    private Duration shutdownTimeout = Duration.ofSeconds(10);
    private ThreadFactory threadFactory;
    private int circuitBreakerFailureThreshold = 5;
    private Duration circuitBreakerDelay = Duration.ofSeconds(10);
    private Duration circuitBreakerMaxWait = Duration.ZERO;

    /**
     * @param name name of the ring-buffer
     */
    public RingBufferSettings(String name) {
        this.name = Objects.requireNonNull(name, "name");
        this.threadFactory = daemonThreadFactory(name);
    }

    /**
     * @return name of the ring-buffer
     */
    public String name() {
        return name;
    }

    /**
     * @return size of the ring-buffer (default: 1024)
     */
    public int ringBufferSize() {
        return ringBufferSize;
    }

    /**
     * @param size size of the ring-buffer
     * @return this instance
     * @throws IllegalArgumentException if the size isn't a power of 2
     */
    public RingBufferSettings ringBufferSize(int size) {
        if (size < 1 || Integer.bitCount(size) != 1) {
            throw new IllegalArgumentException("the size of the ring-buffer '" + name
                + "' has to be a power of 2, but it is " + size);
        }
        this.ringBufferSize = size;
        return this;
    }

    /**
     * @return wait-strategy of the event-handlers (default: {@link BlockingWaitStrategy})
     */
    public WaitStrategy waitStrategy() {
        return waitStrategy;
    }

    /**
     * @param waitStrategy wait-strategy of the event-handlers (an instance must not be shared by ring-buffers)
     * @return this instance
     */
    public RingBufferSettings waitStrategy(WaitStrategy waitStrategy) {
        this.waitStrategy = Objects.requireNonNull(waitStrategy, "waitStrategy");
        return this;
    }

    /**
     * @return producer-type (default: {@link ProducerType#MULTI})
     */
    public ProducerType producerType() {
        return producerType;
    }

    /**
     * @param producerType {@link ProducerType#SINGLE} only if events for this ring-buffer get fired by one thread
     * @return this instance
     */
    public RingBufferSettings producerType(ProducerType producerType) {
        this.producerType = Objects.requireNonNull(producerType, "producerType");
        return this;
    }

    /**
     * @return max. time to wait for pending events during the shutdown (default: 10 seconds)
     */
    public Duration shutdownTimeout() {
        return shutdownTimeout;
    }

    /**
     * @param shutdownTimeout max. time to wait for pending events during the shutdown
     * @return this instance
     */
    public RingBufferSettings shutdownTimeout(Duration shutdownTimeout) {
        if (Objects.requireNonNull(shutdownTimeout, "shutdownTimeout").isNegative()) {
            throw new IllegalArgumentException("the shutdown-timeout must not be negative");
        }
        this.shutdownTimeout = shutdownTimeout;
        return this;
    }

    /**
     * @return factory for the event-handler threads (default: daemon threads named after the ring-buffer)
     */
    public ThreadFactory threadFactory() {
        return threadFactory;
    }

    /**
     * @param threadFactory factory for the event-handler threads (one thread per observer method)
     * @return this instance
     */
    public RingBufferSettings threadFactory(ThreadFactory threadFactory) {
        this.threadFactory = Objects.requireNonNull(threadFactory, "threadFactory");
        return this;
    }

    /**
     * @return number of consecutive rejected events which open the {@link CircuitBreaker} (default: 5)
     */
    public int circuitBreakerFailureThreshold() {
        return circuitBreakerFailureThreshold;
    }

    /**
     * @param failureThreshold number of consecutive rejected events which open the {@link CircuitBreaker}
     * @return this instance
     */
    public RingBufferSettings circuitBreakerFailureThreshold(int failureThreshold) {
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("the failure-threshold of the circuit-breaker has to be positive");
        }
        this.circuitBreakerFailureThreshold = failureThreshold;
        return this;
    }

    /**
     * @return time the {@link CircuitBreaker} stays open before an event gets published as trial (default: 10s)
     */
    public Duration circuitBreakerDelay() {
        return circuitBreakerDelay;
    }

    /**
     * @param delay time the {@link CircuitBreaker} stays open before an event gets published as trial
     * @return this instance
     */
    public RingBufferSettings circuitBreakerDelay(Duration delay) {
        this.circuitBreakerDelay = requireNonNegative(delay, "delay");
        return this;
    }

    /**
     * @return max. time to wait for a free slot, before an event of a {@link CircuitBreaker} injection point
     *         gets rejected (default: 0 - no waiting)
     */
    public Duration circuitBreakerMaxWait() {
        return circuitBreakerMaxWait;
    }

    /**
     * @param maxWait max. time to wait for a free slot, before an event of a {@link CircuitBreaker} injection
     *                point gets rejected
     * @return this instance
     */
    public RingBufferSettings circuitBreakerMaxWait(Duration maxWait) {
        this.circuitBreakerMaxWait = requireNonNegative(maxWait, "maxWait");
        return this;
    }

    @Override
    public String toString() {
        return "RingBufferSettings[name=" + name + ", size=" + ringBufferSize
            + ", waitStrategy=" + waitStrategy.getClass().getSimpleName() + ", producerType=" + producerType
            + ", shutdownTimeout=" + shutdownTimeout + ", circuitBreakerFailureThreshold="
            + circuitBreakerFailureThreshold + ", circuitBreakerDelay=" + circuitBreakerDelay
            + ", circuitBreakerMaxWait=" + circuitBreakerMaxWait + "]";
    }

    private static Duration requireNonNegative(Duration duration, String name) {
        if (Objects.requireNonNull(duration, name).isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return duration;
    }

    private static ThreadFactory daemonThreadFactory(String ringBufferName) {
        AtomicInteger threadCount = new AtomicInteger();
        return runnable -> {
            String threadName = "cdi-disruptor-" + ringBufferName + "-" + threadCount.incrementAndGet();
            Thread thread = new Thread(runnable, threadName);
            thread.setDaemon(true);
            return thread;
        };
    }
}
