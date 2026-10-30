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

import com.lmax.disruptor.WaitStrategy;
import jakarta.enterprise.inject.spi.DefinitionException;
import org.os890.cdi.disruptor.RingBuffer;
import org.os890.cdi.disruptor.RingBufferSettings;

import java.util.Set;
import java.util.TreeSet;

/**
 * Merged configuration of all usages of {@link RingBuffer} with the same name.
 */
final class RingBufferDeclaration {
    private final String name;
    private final Set<String> declaredAt = new TreeSet<>();

    private int size;
    private Class<? extends WaitStrategy> waitStrategy = WaitStrategy.class;

    RingBufferDeclaration(String name) {
        this.name = name;
    }

    /**
     * @param annotation usage of {@link RingBuffer} with the name of this declaration
     * @param source     description of the annotated element
     * @throws DefinitionException if the configuration conflicts with another usage
     */
    void merge(RingBuffer annotation, String source) {
        declaredAt.add(source);

        if (annotation.size() != 0) {
            if (size != 0 && size != annotation.size()) {
                throw conflict("size", size, annotation.size());
            }
            size = annotation.size();
        }
        if (annotation.waitStrategy() != WaitStrategy.class) {
            if (waitStrategy != WaitStrategy.class && waitStrategy != annotation.waitStrategy()) {
                throw conflict("waitStrategy", waitStrategy.getName(), annotation.waitStrategy().getName());
            }
            waitStrategy = annotation.waitStrategy();
        }
    }

    void applyTo(RingBufferSettings settings) {
        if (size != 0) {
            settings.ringBufferSize(size);
        }
        if (waitStrategy != WaitStrategy.class) {
            settings.waitStrategy(WaitStrategies.newInstance(waitStrategy));
        }
    }

    String name() {
        return name;
    }

    Set<String> declaredAt() {
        return declaredAt;
    }

    private DefinitionException conflict(String attribute, Object value, Object otherValue) {
        return new DefinitionException("conflicting values for " + attribute + " of @"
            + RingBuffer.class.getSimpleName() + "(\"" + name + "\"): " + value + " vs. " + otherValue
            + " - declared at " + declaredAt);
    }
}
