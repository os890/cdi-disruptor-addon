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
import java.util.concurrent.Executors;

/**
 * Virtual threads are available with Java 21+ - the project is compiled for Java 17.
 */
final class VirtualThreads {
    private VirtualThreads() {
        // utility class
    }

    static boolean isAvailable() {
        return Runtime.version().feature() >= 21;
    }

    static ExecutorService newVirtualThreadPerTaskExecutor() {
        try {
            return (ExecutorService) Executors.class.getMethod("newVirtualThreadPerTaskExecutor").invoke(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("virtual threads require Java 21+", e);
        }
    }
}
