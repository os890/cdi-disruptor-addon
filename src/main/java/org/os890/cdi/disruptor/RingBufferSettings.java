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

    @Override
    public String toString() {
        return "RingBufferSettings[name=" + name + ", size=" + ringBufferSize
            + ", waitStrategy=" + waitStrategy.getClass().getSimpleName() + ", producerType=" + producerType
            + ", shutdownTimeout=" + shutdownTimeout + "]";
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
