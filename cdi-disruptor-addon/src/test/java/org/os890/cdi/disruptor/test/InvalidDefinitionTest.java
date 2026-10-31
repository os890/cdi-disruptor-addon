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
package org.os890.cdi.disruptor.test;

import jakarta.enterprise.inject.se.SeContainerInitializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.os890.cdi.disruptor.test.invalid.CircuitBreakerOnEventParameter;
import org.os890.cdi.disruptor.test.invalid.CircuitBreakerOnOtherMethod;
import org.os890.cdi.disruptor.test.invalid.CircuitBreakerOnOtherInjectionPoint;
import org.os890.cdi.disruptor.test.invalid.ConflictingRingBufferConfig;
import org.os890.cdi.disruptor.test.invalid.InvalidRingBufferSize;
import org.os890.cdi.disruptor.test.invalid.PrivateObserver;
import org.os890.cdi.disruptor.test.invalid.TwoEventParametersObserver;
import org.os890.cdi.disruptor.test.invalid.UnsatisfiedParameterObserver;
import org.os890.cdi.disruptor.test.invalid.WildcardInjectionPoint;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Separated from {@link DisruptorEventTest}, because not all containers support multiple containers at the same time.
 */
class InvalidDefinitionTest {
    private final StringWriter loggedErrors = new StringWriter();

    // some containers (e.g. OWB) just log the details of definition errors
    private final Handler logCapture = new Handler() {
        @Override
        public void publish(LogRecord logRecord) {
            loggedErrors.append(logRecord.getMessage()).append('\n');
            if (logRecord.getThrown() != null) {
                logRecord.getThrown().printStackTrace(new PrintWriter(loggedErrors));
            }
        }

        @Override
        public void flush() {
            // nothing to do
        }

        @Override
        public void close() {
            // nothing to do
        }
    };

    @BeforeEach
    void captureLogs() {
        Logger.getLogger("").addHandler(logCapture);
    }

    @AfterEach
    void releaseFailedContainer() throws ReflectiveOperationException {
        Logger.getLogger("").removeHandler(logCapture);

        // OWB keeps the context of a failed deployment, which would prevent a new deployment
        try {
            Class<?> webBeansFinder = Class.forName("org.apache.webbeans.config.WebBeansFinder");
            Object currentKey = webBeansFinder.getMethod("getCurrentKey").invoke(null);
            webBeansFinder.getMethod("clearInstances", Object.class).invoke(null, currentKey);
        } catch (ClassNotFoundException e) {
            // not OWB
        }
    }

    @ParameterizedTest
    @ValueSource(classes = {
        TwoEventParametersObserver.class, PrivateObserver.class,
        UnsatisfiedParameterObserver.class, WildcardInjectionPoint.class,
        ConflictingRingBufferConfig.class, InvalidRingBufferSize.class,
        CircuitBreakerOnOtherInjectionPoint.class, CircuitBreakerOnEventParameter.class,
        CircuitBreakerOnOtherMethod.class
    })
    void invalidDefinitionFailsTheDeployment(Class<?> invalidBean) {
        RuntimeException exception = assertThrows(RuntimeException.class,
            () -> SeContainerInitializer.newInstance().addBeanClasses(invalidBean).initialize().close());

        StringWriter failure = new StringWriter();
        exception.printStackTrace(new PrintWriter(failure));
        failure.append(loggedErrors.toString());
        assertTrue(failure.toString().contains(invalidBean.getSimpleName()), "unexpected failure: " + failure);
    }
}
