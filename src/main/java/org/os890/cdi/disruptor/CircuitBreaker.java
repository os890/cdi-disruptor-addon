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

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Circuit-breaker marker for {@link DisruptorEvent} injection points (protects the producer) and for
 * observer methods (protects against failing observers).
 * <p>
 * <b>{@link DisruptorEvent} injection points:</b>
 * the disruptor protects the observers from overload (they process the events of a bounded ring-buffer
 * at their own pace) - if they can't keep up, the ring-buffer gets full and {@link DisruptorEvent#fire(Object)}
 * waits for free slots (backpressure). With this marker the producer gets protected instead:
 * <ul>
 * <li>closed: an event gets rejected, if there is no free slot within the max. wait-time
 *     ({@link DisruptorEvent#fire(Object)} throws an {@link EventRejectedException},
 *     {@link DisruptorEvent#tryFire(Object)} returns false)</li>
 * <li>open: after a number of consecutive rejections all events get rejected immediately for a delay -
 *     the observers get time to process the pending events</li>
 * <li>half-open: after the delay one event gets published as trial - the circuit-breaker gets closed,
 *     if it was accepted, otherwise it stays open for another delay</li>
 * </ul>
 * There is one circuit-breaker per ring-buffer, which is shared by all injection points of the ring-buffer
 * with this marker. Injection points without this marker aren't affected.
 * <pre>
 * &#064;Inject
 * &#064;CircuitBreaker
 * private DisruptorEvent&lt;OrderPlaced&gt; orderPlacedEvent;
 * </pre>
 * <b>Observer methods:</b>
 * <ul>
 * <li>closed: the observer method gets notified</li>
 * <li>open: after a number of consecutive exceptions of the observer method, it doesn't get notified for a delay
 *     (the events get skipped and counted)</li>
 * <li>half-open: after the delay the next event gets passed to the observer method as trial - the
 *     circuit-breaker gets closed, if it doesn't fail, otherwise it stays open for another delay</li>
 * </ul>
 * Every observer method has its own circuit-breaker - other observer methods aren't affected.
 * <pre>
 * &#064;CircuitBreaker
 * void onOrderPlaced(&#064;ObservesDisruptor OrderPlaced event, RemoteService remoteService) {
 *     remoteService.send(event); // might fail
 * }
 * </pre>
 * The failure-threshold, the delay and the max. wait-time (only used for injection points) are part of the
 * {@link RingBufferSettings} of the ring-buffer (see {@link RingBuffer} for the configuration options).
 */
@Target({FIELD, PARAMETER, METHOD})
@Retention(RUNTIME)
@Documented
public @interface CircuitBreaker {
}
