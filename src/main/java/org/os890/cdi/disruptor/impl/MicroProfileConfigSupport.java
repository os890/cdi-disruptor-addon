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

import com.lmax.disruptor.dsl.ProducerType;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.os890.cdi.disruptor.RingBufferSettings;

import java.time.Duration;
import java.util.Locale;
import java.util.ServiceConfigurationError;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Applies the values of MicroProfile Config (optional dependency) to the settings of a ring-buffer.
 */
final class MicroProfileConfigSupport {
    static final String KEY_PREFIX = "cdi.disruptor.";

    private static final Logger LOG = Logger.getLogger(MicroProfileConfigSupport.class.getName());
    private static final boolean API_AVAILABLE = isApiAvailable();

    private MicroProfileConfigSupport() {
        // utility class
    }

    static void applyTo(RingBufferSettings settings) {
        if (!API_AVAILABLE) {
            return;
        }

        try {
            MicroProfileConfigReader.applyTo(settings);
        } catch (IllegalStateException | ServiceConfigurationError e) {
            LOG.log(Level.FINE, "MicroProfile Config API without implementation - config is ignored", e);
        }
    }

    private static boolean isApiAvailable() {
        try {
            Class.forName("org.eclipse.microprofile.config.ConfigProvider", false,
                MicroProfileConfigSupport.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    // all usages of the API are encapsulated in this class, which only gets loaded if the API is available
    private static final class MicroProfileConfigReader {
        static void applyTo(RingBufferSettings settings) {
            Config config = ConfigProvider.getConfig();
            String prefix = KEY_PREFIX + settings.name() + ".";

            config.getOptionalValue(prefix + "size", Integer.class)
                .ifPresent(settings::ringBufferSize);
            config.getOptionalValue(prefix + "wait-strategy", String.class)
                .map(WaitStrategies::newInstance)
                .ifPresent(settings::waitStrategy);
            config.getOptionalValue(prefix + "producer-type", String.class)
                .map(value -> ProducerType.valueOf(value.trim().toUpperCase(Locale.ROOT)))
                .ifPresent(settings::producerType);
            config.getOptionalValue(prefix + "shutdown-timeout-ms", Long.class)
                .map(Duration::ofMillis)
                .ifPresent(settings::shutdownTimeout);
        }
    }
}
