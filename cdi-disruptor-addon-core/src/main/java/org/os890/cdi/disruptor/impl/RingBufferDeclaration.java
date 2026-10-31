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
import org.os890.cdi.disruptor.DisruptorConfig;
import org.os890.cdi.disruptor.RingBuffer;
import org.os890.cdi.disruptor.RingBufferSettings;

import java.util.Set;
import java.util.TreeSet;

/**
 * Merged configuration of all usages of {@link RingBuffer} with the same name.
 * <p>
 * Internal API - not intended to be used by applications.
 */
public final class RingBufferDeclaration {
    private static final String UNSPECIFIED_WAIT_STRATEGY = WaitStrategy.class.getName();

    private final String name;
    private final Set<String> declaredAt = new TreeSet<>();

    private int size;
    private String waitStrategy = UNSPECIFIED_WAIT_STRATEGY;

    /**
     * @param name name of the ring-buffer
     */
    public RingBufferDeclaration(String name) {
        this.name = name;
    }

    /**
     * @param annotation usage of {@link RingBuffer} with the name of this declaration
     * @param source     description of the annotated element
     * @throws IllegalArgumentException if the configuration conflicts with another usage
     */
    public void merge(RingBuffer annotation, String source) {
        merge(annotation.size(), annotation.waitStrategy().getName(), source);
    }

    /**
     * @param size             size of the ring-buffer ({@code 0} for not specified)
     * @param waitStrategyName class-name of the wait-strategy ({@code WaitStrategy} for not specified)
     * @param source           description of the annotated element
     * @throws IllegalArgumentException if the configuration conflicts with another usage
     */
    public void merge(int size, String waitStrategyName, String source) {
        declaredAt.add(source);

        if (size != 0) {
            if (this.size != 0 && this.size != size) {
                throw conflict("size", this.size, size);
            }
            this.size = size;
        }
        if (!UNSPECIFIED_WAIT_STRATEGY.equals(waitStrategyName)) {
            if (!UNSPECIFIED_WAIT_STRATEGY.equals(waitStrategy) && !waitStrategy.equals(waitStrategyName)) {
                throw conflict("waitStrategy", waitStrategy, waitStrategyName);
            }
            waitStrategy = waitStrategyName;
        }
    }

    /**
     * Resolves the settings of the ring-buffer (later entries override former ones):
     * defaults, the attributes of {@link RingBuffer}, MicroProfile Config (if available) and the given configs.
     *
     * @param configs beans implementing {@link DisruptorConfig}
     * @return the settings of the ring-buffer
     */
    public RingBufferSettings resolveSettings(Iterable<? extends DisruptorConfig> configs) {
        RingBufferSettings settings = new RingBufferSettings(name);
        if (size != 0) {
            settings.ringBufferSize(size);
        }
        if (!UNSPECIFIED_WAIT_STRATEGY.equals(waitStrategy)) {
            settings.waitStrategy(WaitStrategies.newInstance(waitStrategy));
        }
        MicroProfileConfigSupport.applyTo(settings);
        for (DisruptorConfig config : configs) {
            config.configure(settings);
        }
        return settings;
    }

    /**
     * @return name of the ring-buffer
     */
    public String name() {
        return name;
    }

    /**
     * @return merged size of the ring-buffer ({@code 0} for not specified)
     */
    public int size() {
        return size;
    }

    /**
     * @return class-name of the merged wait-strategy ({@code WaitStrategy} for not specified)
     */
    public String waitStrategyName() {
        return waitStrategy;
    }

    /**
     * @return descriptions of the elements which use the ring-buffer
     */
    public Set<String> declaredAt() {
        return declaredAt;
    }

    private IllegalArgumentException conflict(String attribute, Object value, Object otherValue) {
        return new IllegalArgumentException("conflicting values for " + attribute + " of @"
            + RingBuffer.class.getSimpleName() + "(name = \"" + name + "\"): " + value + " vs. " + otherValue
            + " - declared at " + declaredAt);
    }
}
