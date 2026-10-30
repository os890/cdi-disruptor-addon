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

import com.lmax.disruptor.WaitStrategy;
import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.enterprise.util.Nonbinding;
import jakarta.inject.Qualifier;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Qualifier which selects a named ring-buffer - every ring-buffer has its own configuration
 * and its own event-handler threads.
 * <p>
 * Used at {@link DisruptorEvent} injection points and at the event parameter of observer methods:
 * <pre>
 * &#064;Inject
 * &#064;RingBuffer(name = "audit", size = 4096)
 * private DisruptorEvent&lt;AuditEntry&gt; auditEvent;
 *
 * void onAuditEntry(&#064;ObservesDisruptor &#064;RingBuffer(name = "audit") AuditEntry entry) {
 *     // ...
 * }
 * </pre>
 * Events are only delivered to observers of the same ring-buffer.
 * Without this qualifier (or without a name) the ring-buffer {@value #DEFAULT} is used.
 * <p>
 * Not to be confused with {@link com.lmax.disruptor.RingBuffer}, which is used internally.
 * <p>
 * The configuration of a ring-buffer gets resolved once during the bootstrapping of the container
 * (later entries override former ones):
 * <ol>
 * <li>defaults of {@link RingBufferSettings}</li>
 * <li>the non-binding attributes of this annotation
 *     (all usages with the same name have to specify the same values or none)</li>
 * <li>MicroProfile Config (if available):
 *     {@code cdi.disruptor.<name>.size}, {@code cdi.disruptor.<name>.wait-strategy} (class-name),
 *     {@code cdi.disruptor.<name>.producer-type} ({@code SINGLE} or {@code MULTI}),
 *     {@code cdi.disruptor.<name>.shutdown-timeout-ms}</li>
 * <li>beans implementing {@link DisruptorConfig}</li>
 * </ol>
 */
@Qualifier
@Retention(RUNTIME)
@Target({FIELD, PARAMETER})
@Documented
public @interface RingBuffer {
    /**
     * Name of the ring-buffer which is used without {@link RingBuffer}.
     */
    String DEFAULT = "default";

    /**
     * @return name of the ring-buffer
     */
    String name() default DEFAULT;

    /**
     * @return size of the ring-buffer (has to be a power of 2) - {@code 0} for not specified
     */
    @Nonbinding
    int size() default 0;

    /**
     * @return type of the wait-strategy (requires a public no-arg constructor) -
     *         {@code WaitStrategy.class} for not specified
     */
    @Nonbinding
    Class<? extends WaitStrategy> waitStrategy() default WaitStrategy.class;

    /**
     * Literal for {@link RingBuffer} (without configuration) - e.g. for {@link DisruptorEvent#select}.
     */
    final class Literal extends AnnotationLiteral<RingBuffer> implements RingBuffer {
        private static final long serialVersionUID = 1L;

        private final String name;

        private Literal(String name) {
            this.name = name;
        }

        /**
         * @param ringBufferName name of the ring-buffer
         * @return literal for the given name
         */
        public static RingBuffer of(String ringBufferName) {
            return new Literal(ringBufferName);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public Class<? extends WaitStrategy> waitStrategy() {
            return WaitStrategy.class;
        }
    }
}
