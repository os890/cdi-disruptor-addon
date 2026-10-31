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

import jakarta.enterprise.event.Startup;
import jakarta.enterprise.inject.build.compatible.spi.Parameters;
import jakarta.enterprise.inject.build.compatible.spi.SyntheticObserver;
import jakarta.enterprise.inject.spi.EventContext;
import jakarta.enterprise.invoke.Invoker;
import org.os890.cdi.disruptor.impl.RingBufferDeclaration;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Starts the runtime with the metadata collected by {@link DisruptorBuildCompatibleExtension} during the build.
 */
public class StartupObserver implements SyntheticObserver<Startup> {
    static final String INVOKERS = "invokers";
    static final String OBSERVED_TYPES = "observedTypes";
    static final String QUALIFIERS = "qualifiers";
    static final String QUALIFIER_COUNTS = "qualifierCounts";
    static final String RING_BUFFER_NAMES = "ringBufferNames";
    static final String CIRCUIT_BREAKER = "circuitBreaker";
    static final String EVENT_PARAMETER_POSITIONS = "eventParameterPositions";
    static final String PARAMETER_COUNTS = "parameterCounts";
    static final String DESCRIPTIONS = "descriptions";
    static final String DECLARED_RING_BUFFERS = "declaredRingBuffers";
    static final String DECLARED_SIZES = "declaredSizes";
    static final String DECLARED_WAIT_STRATEGIES = "declaredWaitStrategies";
    static final String DECLARED_AT = "declaredAt";

    @Override
    public void observe(EventContext<Startup> event, Parameters params) throws Exception {
        String[] ringBufferNames = params.get(RING_BUFFER_NAMES, String[].class);
        Invoker<?, ?>[] invokers = params.get(INVOKERS, Invoker[].class);
        String[] observedTypes = params.get(OBSERVED_TYPES, String[].class);
        Annotation[] qualifiers = params.get(QUALIFIERS, Annotation[].class);
        int[] qualifierCounts = params.get(QUALIFIER_COUNTS, int[].class);
        boolean[] circuitBreaker = params.get(CIRCUIT_BREAKER, boolean[].class);
        int[] eventParameterPositions = params.get(EVENT_PARAMETER_POSITIONS, int[].class);
        int[] parameterCounts = params.get(PARAMETER_COUNTS, int[].class);
        String[] descriptions = params.get(DESCRIPTIONS, String[].class);

        List<LiteObserverMethod> observerMethods = new ArrayList<>(ringBufferNames.length);
        int qualifierOffset = 0;
        for (int i = 0; i < ringBufferNames.length; i++) {
            Set<Annotation> observedQualifiers = new HashSet<>(
                Arrays.asList(qualifiers).subList(qualifierOffset, qualifierOffset + qualifierCounts[i]));
            qualifierOffset += qualifierCounts[i];

            observerMethods.add(new LiteObserverMethod(invokers[i], loadClass(observedTypes[i]),
                Set.copyOf(observedQualifiers), ringBufferNames[i], circuitBreaker[i],
                eventParameterPositions[i], parameterCounts[i], descriptions[i]));
        }

        String[] declaredRingBuffers = params.get(DECLARED_RING_BUFFERS, String[].class);
        int[] declaredSizes = params.get(DECLARED_SIZES, int[].class);
        String[] declaredWaitStrategies = params.get(DECLARED_WAIT_STRATEGIES, String[].class);
        String[] declaredAt = params.get(DECLARED_AT, String[].class);
        List<RingBufferDeclaration> declarations = new ArrayList<>(declaredRingBuffers.length);
        for (int i = 0; i < declaredRingBuffers.length; i++) {
            RingBufferDeclaration declaration = new RingBufferDeclaration(declaredRingBuffers[i]);
            declaration.merge(declaredSizes[i], declaredWaitStrategies[i], declaredAt[i]);
            declarations.add(declaration);
        }

        LiteDisruptorRuntime.start(declarations, observerMethods);
    }

    private static Class<?> loadClass(String className) throws ClassNotFoundException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = StartupObserver.class.getClassLoader();
        }
        return Class.forName(className, false, classLoader);
    }
}
