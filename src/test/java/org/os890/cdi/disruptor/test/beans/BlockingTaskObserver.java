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
public class BlockingTaskObserver {
    void onSmall(@ObservesDisruptor @RingBuffer(name = "small") BlockingTask task) throws InterruptedException {
        task.release().await();
    }

    // observes the events of both injection points of the ring-buffer "tiny"
    void onTiny(@ObservesDisruptor @RingBuffer(name = "tiny") BlockingTask task) throws InterruptedException {
        task.release().await();
    }

    void onCustom(@ObservesDisruptor @RingBuffer(name = "custom") BlockingTask task) throws InterruptedException {
        task.release().await();
    }
}
