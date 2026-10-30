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

import java.lang.annotation.Annotation;

/**
 * Counterpart of {@link jakarta.enterprise.event.Event} which dispatches events via the LMAX Disruptor.
 * <p>
 * Usage:
 * <pre>
 * &#064;Inject
 * private DisruptorEvent&lt;OrderPlaced&gt; orderPlacedEvent;
 *
 * orderPlacedEvent.fire(new OrderPlaced(...));
 * </pre>
 * Events get delivered asynchronously to all observer methods with a parameter annotated with
 * {@link ObservesDisruptor} whose type is assignable from the runtime type of the event and whose
 * qualifiers are a subset of the qualifiers of the event.
 * <p>
 * Every observer method is backed by its own event-handler (thread) of the disruptor, therefore
 * different observer methods get notified in parallel, while a single observer method receives the
 * events in the order they were published.
 *
 * @param <T> the type of the event
 */
public interface DisruptorEvent<T> {
    /**
     * Publishes the event to the ring-buffer.
     * Blocks if the ring-buffer is full until the slowest observer catches up.
     * <p>
     * Don't use it within an observer method if the ring-buffer might be full
     * (the observer would wait for itself) - use {@link #tryFire(Object)} instead.
     *
     * @param event the event to publish
     * @throws IllegalStateException if the dispatcher isn't running (before deployment or after shutdown)
     */
    void fire(T event);

    /**
     * Publishes the event to the ring-buffer if there is free capacity.
     *
     * @param event the event to publish
     * @return true if the event was published (or if there is no observer for it),
     *         false if the ring-buffer is full
     * @throws IllegalStateException if the dispatcher isn't running (before deployment or after shutdown)
     */
    boolean tryFire(T event);

    /**
     * @param qualifiers additional qualifiers
     * @return a child event with the qualifiers of this event and the given qualifiers
     * @throws IllegalArgumentException if one of the given annotations isn't a qualifier
     */
    DisruptorEvent<T> select(Annotation... qualifiers);

    /**
     * @param subtype    the subtype of the event
     * @param qualifiers additional qualifiers
     * @param <U>        the subtype
     * @return a child event for the given subtype with the qualifiers of this event and the given qualifiers
     * @throws IllegalArgumentException if one of the given annotations isn't a qualifier
     */
    <U extends T> DisruptorEvent<U> select(Class<U> subtype, Annotation... qualifiers);
}
