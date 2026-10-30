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
package org.os890.cdi.disruptor.test.perf;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Simple dispatch from the client code to the consumer only with virtual threads (without CDI and without
 * a ring-buffer) - one virtual thread per event.
 */
final class VirtualThreadBenchTarget implements BenchTarget {
    private final BenchStatistics statistics = new BenchStatistics();
    private final ExecutorService executor = VirtualThreads.newVirtualThreadPerTaskExecutor();

    @Override
    public void fire(BenchEvent event) {
        executor.execute(() -> statistics.record(event));
    }

    @Override
    public long received() {
        return statistics.received();
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
