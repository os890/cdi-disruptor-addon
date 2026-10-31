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

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Variant which is backed by a CDI container.
 */
final class CdiBenchTarget implements BenchTarget {
    private final SeContainer container;
    private final BenchStatistics statistics;
    private final Consumer<BenchEvent> fire;

    CdiBenchTarget(Class<?> observerClass, Function<BenchPublisher, Consumer<BenchEvent>> fireMethod) {
        this.container = SeContainerInitializer.newInstance()
            .addBeanClasses(BenchStatistics.class, BenchPublisher.class, observerClass)
            .initialize();
        this.statistics = container.select(BenchStatistics.class).get();
        this.fire = fireMethod.apply(container.select(BenchPublisher.class).get());
    }

    @Override
    public void fire(BenchEvent event) {
        fire.accept(event);
    }

    @Override
    public long received() {
        return statistics.received();
    }

    @Override
    public void close() {
        container.close();
    }
}
