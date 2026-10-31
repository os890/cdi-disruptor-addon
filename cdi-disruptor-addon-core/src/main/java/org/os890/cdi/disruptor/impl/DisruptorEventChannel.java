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

import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Qualifier;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Internal qualifier which replaces the qualifiers of {@link org.os890.cdi.disruptor.DisruptorEvent}
 * injection points. Every distinct set of qualifiers gets its own channel (and bean), which allows
 * arbitrary (user-defined) qualifiers at injection points - like for {@link jakarta.enterprise.event.Event}.
 * <p>
 * Not intended to be used by applications.
 */
@Qualifier
@Retention(RUNTIME)
@Target({TYPE, METHOD, FIELD, PARAMETER})
public @interface DisruptorEventChannel {
    /**
     * @return id of the channel
     */
    int value();

    /**
     * Literal for {@link DisruptorEventChannel}.
     */
    final class Literal extends AnnotationLiteral<DisruptorEventChannel> implements DisruptorEventChannel {
        private static final long serialVersionUID = 1L;

        private final int value;

        private Literal(int value) {
            this.value = value;
        }

        /**
         * @param value id of the channel
         * @return literal for the given channel
         */
        public static DisruptorEventChannel of(int value) {
            return new Literal(value);
        }

        @Override
        public int value() {
            return value;
        }
    }
}
