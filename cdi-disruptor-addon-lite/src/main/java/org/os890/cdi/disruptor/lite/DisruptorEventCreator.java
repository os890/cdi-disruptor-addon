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

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.build.compatible.spi.Parameters;
import jakarta.enterprise.inject.build.compatible.spi.SyntheticBeanCreator;
import org.os890.cdi.disruptor.impl.DisruptorEventImpl;
import org.os890.cdi.disruptor.impl.Qualifiers;

import java.lang.annotation.Annotation;
import java.util.Arrays;

/**
 * Creates the {@link org.os890.cdi.disruptor.DisruptorEvent} instances of a channel
 * (ring-buffer, qualifiers and circuit-breaker of injection points).
 */
@SuppressWarnings("rawtypes")
public class DisruptorEventCreator implements SyntheticBeanCreator<DisruptorEventImpl> {
    static final String RING_BUFFER_NAME = "ringBufferName";
    static final String QUALIFIERS = "qualifiers";
    static final String CIRCUIT_BREAKER = "circuitBreaker";

    @Override
    public DisruptorEventImpl create(Instance<Object> lookup, Parameters params) {
        return new DisruptorEventImpl<>(LiteDisruptorRuntime.ACCESS,
            params.get(RING_BUFFER_NAME, String.class),
            Qualifiers.normalizeEventQualifiers(Arrays.asList(params.get(QUALIFIERS, Annotation[].class))),
            params.get(CIRCUIT_BREAKER, Boolean.class));
    }
}
