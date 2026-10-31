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
package org.os890.cdi.disruptor;

/**
 * Programmatic configuration of ring-buffers.
 * <p>
 * All beans implementing this interface get invoked once per ring-buffer during the bootstrapping of the
 * container. They are applied last, therefore they can override values of {@link RingBuffer} and
 * MicroProfile Config, e.g.:
 * <pre>
 * &#064;ApplicationScoped
 * public class LowLatencyConfig implements DisruptorConfig {
 *     &#064;Override
 *     public void configure(RingBufferSettings settings) {
 *         if ("market-data".equals(settings.name())) {
 *             settings.waitStrategy(new BusySpinWaitStrategy());
 *         }
 *     }
 * }
 * </pre>
 */
public interface DisruptorConfig {
    /**
     * @param settings settings of a ring-buffer, which already contain the values of {@link RingBuffer}
     *                 and MicroProfile Config
     */
    void configure(RingBufferSettings settings);
}
