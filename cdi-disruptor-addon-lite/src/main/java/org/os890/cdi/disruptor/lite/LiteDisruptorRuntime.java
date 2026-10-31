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
package org.os890.cdi.disruptor.lite;

import jakarta.enterprise.context.control.RequestContextController;
import jakarta.enterprise.inject.spi.CDI;
import org.os890.cdi.disruptor.DisruptorConfig;
import org.os890.cdi.disruptor.RingBufferSettings;
import org.os890.cdi.disruptor.impl.DisruptorDispatcher;
import org.os890.cdi.disruptor.impl.DisruptorRuntime;
import org.os890.cdi.disruptor.impl.RingBufferDeclaration;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Runtime of the build-compatible extension - started by the {@link jakarta.enterprise.event.Startup} event and
 * stopped by the {@link jakarta.enterprise.event.Shutdown} event of the container.
 * <p>
 * {@link org.os890.cdi.disruptor.DisruptorEvent} instances use {@link #ACCESS}, which delegates to the started
 * runtime - therefore they can be created before the runtime gets started.
 */
final class LiteDisruptorRuntime implements DisruptorRuntime {
    static final DisruptorRuntime ACCESS = new DisruptorRuntime() {
        @Override
        public DisruptorDispatcher dispatcher(String ringBufferName) {
            return current().dispatcher(ringBufferName);
        }

        @Override
        public boolean isQualifier(Class<? extends Annotation> annotationType) {
            return CDI.current().getBeanContainer().isQualifier(annotationType);
        }
    };

    private static volatile LiteDisruptorRuntime current;

    private final Map<String, DisruptorDispatcher> dispatchers;
    private final DisruptorDispatcher undeclaredRingBufferDispatcher;

    private LiteDisruptorRuntime(Map<String, DisruptorDispatcher> dispatchers,
                                 DisruptorDispatcher undeclaredRingBufferDispatcher) {
        this.dispatchers = Map.copyOf(dispatchers);
        this.undeclaredRingBufferDispatcher = undeclaredRingBufferDispatcher;
    }

    static synchronized void start(List<RingBufferDeclaration> declarations, List<LiteObserverMethod> observerMethods) {
        if (current != null) {
            current.stop();
        }

        Supplier<RequestContextController> requestContextControllerFactory =
            () -> CDI.current().select(RequestContextController.class).get();
        List<DisruptorConfig> configs = new ArrayList<>();
        CDI.current().select(DisruptorConfig.class).forEach(configs::add);

        Map<String, List<LiteObserverMethod>> observerMethodsByRingBuffer = new HashMap<>();
        for (LiteObserverMethod observerMethod : observerMethods) {
            observerMethodsByRingBuffer.computeIfAbsent(observerMethod.ringBufferName(), name -> new ArrayList<>())
                .add(observerMethod);
        }

        Map<String, DisruptorDispatcher> dispatchers = new HashMap<>();
        for (RingBufferDeclaration declaration : declarations) {
            dispatchers.put(declaration.name(), new DisruptorDispatcher(
                observerMethodsByRingBuffer.getOrDefault(declaration.name(), List.of()),
                declaration.resolveSettings(configs), requestContextControllerFactory));
        }
        DisruptorDispatcher undeclared = new DisruptorDispatcher(
            List.of(), new RingBufferSettings("undeclared"), requestContextControllerFactory);

        dispatchers.values().forEach(DisruptorDispatcher::start);
        undeclared.start();
        current = new LiteDisruptorRuntime(dispatchers, undeclared);
    }

    static synchronized void shutdown() {
        if (current != null) {
            current.stop();
            current = null;
        }
    }

    private static LiteDisruptorRuntime current() {
        LiteDisruptorRuntime result = current;
        if (result == null) {
            throw new IllegalStateException("the disruptor isn't running - events can be fired after the startup");
        }
        return result;
    }

    @Override
    public DisruptorDispatcher dispatcher(String ringBufferName) {
        DisruptorDispatcher result = dispatchers.get(ringBufferName);
        return result != null ? result : undeclaredRingBufferDispatcher;
    }

    @Override
    public boolean isQualifier(Class<? extends Annotation> annotationType) {
        return ACCESS.isQualifier(annotationType);
    }

    private void stop() {
        dispatchers.values().forEach(DisruptorDispatcher::stop);
        undeclaredRingBufferDispatcher.stop();
    }
}
