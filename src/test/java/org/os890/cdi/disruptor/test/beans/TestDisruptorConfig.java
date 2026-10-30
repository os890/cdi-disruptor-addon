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
package org.os890.cdi.disruptor.test.beans;

import jakarta.enterprise.context.ApplicationScoped;
import org.os890.cdi.disruptor.DisruptorConfig;
import org.os890.cdi.disruptor.RingBufferSettings;

import java.time.Duration;

@ApplicationScoped
public class TestDisruptorConfig implements DisruptorConfig {
    public static final int CUSTOM_SIZE = 16;
    // circuit-breaker settings of the ring-buffers "guarded" and "flaky"
    public static final int GUARDED_FAILURE_THRESHOLD = 2;
    public static final Duration GUARDED_DELAY = Duration.ofMillis(300);

    @Override
    public void configure(RingBufferSettings settings) {
        if ("custom".equals(settings.name())) {
            settings.ringBufferSize(CUSTOM_SIZE);
        }
        if ("guarded".equals(settings.name()) || "flaky".equals(settings.name())) {
            settings.circuitBreakerFailureThreshold(GUARDED_FAILURE_THRESHOLD).circuitBreakerDelay(GUARDED_DELAY);
        }
    }
}
