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

final class WaitStrategies {
    private WaitStrategies() {
        // utility class
    }

    static WaitStrategy newInstance(Class<? extends WaitStrategy> type) {
        try {
            return type.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("can't create the wait-strategy " + type.getName()
                + " - a public no-arg constructor is required", e);
        }
    }

    /**
     * @param typeName fully qualified class-name or the simple class-name of a wait-strategy of the disruptor
     *                 (e.g. {@code YieldingWaitStrategy})
     * @return new wait-strategy
     */
    static WaitStrategy newInstance(String typeName) {
        String className = typeName.contains(".") ? typeName.trim() : "com.lmax.disruptor." + typeName.trim();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = WaitStrategies.class.getClassLoader();
        }

        Class<?> type;
        try {
            type = Class.forName(className, true, classLoader);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("unknown wait-strategy " + className, e);
        }
        if (!WaitStrategy.class.isAssignableFrom(type)) {
            throw new IllegalArgumentException(className + " isn't a " + WaitStrategy.class.getName());
        }
        return newInstance(type.asSubclass(WaitStrategy.class));
    }
}
