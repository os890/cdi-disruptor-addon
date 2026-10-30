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

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Counterpart of {@link jakarta.enterprise.event.Observes} for events fired via {@link DisruptorEvent}.
 * <p>
 * Marks the event parameter of an observer method of a managed bean:
 * <pre>
 * &#064;ApplicationScoped
 * public class OrderObserver {
 *     public void onOrder(&#064;ObservesDisruptor &#064;Premium OrderPlaced event, AuditService auditService) {
 *         // ...
 *     }
 * }
 * </pre>
 * Qualifiers of the event parameter restrict the observed events (like for CDI observers).
 * All other parameters are injection points.
 * The method gets invoked on a thread of the disruptor with an active request-context.
 * <p>
 * Restrictions:
 * <ul>
 * <li>exactly one parameter per method may be annotated with {@code @ObservesDisruptor}</li>
 * <li>the event type has to be a class or a parameterized type
 *     (events get matched against the raw type)</li>
 * <li>private observer methods aren't supported for normal-scoped beans</li>
 * </ul>
 */
@Target(PARAMETER)
@Retention(RUNTIME)
@Documented
public @interface ObservesDisruptor {
}
