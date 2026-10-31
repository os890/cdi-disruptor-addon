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

import java.lang.annotation.Annotation;

/**
 * Runtime of the addon used by {@link org.os890.cdi.disruptor.DisruptorEvent} instances - implemented by the
 * portable extension and the build-compatible extension.
 * <p>
 * Internal API - not intended to be used by applications.
 */
public interface DisruptorRuntime {
    /**
     * @param ringBufferName name of the ring-buffer
     * @return dispatcher of the ring-buffer (or a dispatcher without observers for unknown names)
     * @throws IllegalStateException if the runtime isn't started
     */
    DisruptorDispatcher dispatcher(String ringBufferName);

    /**
     * @param annotationType annotation type
     * @return true if the given annotation is a qualifier
     */
    boolean isQualifier(Class<? extends Annotation> annotationType);
}
