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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Configuration of the disruptor.
 * <p>
 * Provide a bean which implements this interface to override the defaults, e.g.:
 * <pre>
 * &#064;ApplicationScoped
 * public class LowLatencyConfig implements DisruptorConfig {
 *     &#064;Override
 *     public WaitStrategy waitStrategy() {
 *         return new BusySpinWaitStrategy();
 *     }
 * }
 * </pre>
 * The configuration is read once during the bootstrapping of the container.
 */
public interface DisruptorConfig {
    /**
     * @return size of the ring-buffer (has to be a power of 2)
     */
    default int ringBufferSize() {
        return 1024;
    }

    /**
     * @return wait-strategy used by the event-handlers (one per observer method)
     */
    default WaitStrategy waitStrategy() {
        return new BlockingWaitStrategy();
    }

    /**
     * @return {@link ProducerType#MULTI} if events get fired by multiple threads
     */
    default ProducerType producerType() {
        return ProducerType.MULTI;
    }

    /**
     * @return max. time to wait for the processing of pending events during the shutdown of the container
     */
    default Duration shutdownTimeout() {
        return Duration.ofSeconds(10);
    }

    /**
     * @return factory for the threads of the event-handlers (one thread per observer method)
     */
    default ThreadFactory threadFactory() {
        AtomicInteger threadCount = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, "cdi-disruptor-observer-" + threadCount.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
