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
import jakarta.enterprise.context.ContextNotActiveException;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.spi.BeanManager;
import org.os890.cdi.disruptor.ObservesDisruptor;

@ApplicationScoped
public class OrderObserver {
    public static final String ORDERS = "orders";
    public static final String ORDER_THREADS = "order-threads";
    public static final String REQUEST_CONTEXT_ACTIVE = "order-request-context-active";

    void onOrder(@ObservesDisruptor Order order, EventRecorder recorder, BeanManager beanManager) {
        recorder.record(ORDERS, order.id());
        recorder.record(ORDER_THREADS, Thread.currentThread().getName());
        recorder.record(REQUEST_CONTEXT_ACTIVE, isRequestContextActive(beanManager));
    }

    private static boolean isRequestContextActive(BeanManager beanManager) {
        try {
            return beanManager.getContext(RequestScoped.class).isActive();
        } catch (ContextNotActiveException e) {
            return false;
        }
    }
}
