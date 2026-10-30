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
import org.os890.cdi.disruptor.ObservesDisruptor;
import org.os890.cdi.disruptor.RingBuffer;

@ApplicationScoped
public class AuditObserver {
    public static final String AUDIT_ORDERS = "audit-orders";
    public static final String AUDIT_THREADS = "audit-threads";

    void onAuditOrder(@ObservesDisruptor @RingBuffer(name = "audit") Order order, EventRecorder recorder) {
        recorder.record(AUDIT_ORDERS, order.id());
        recorder.record(AUDIT_THREADS, Thread.currentThread().getName());
    }
}
