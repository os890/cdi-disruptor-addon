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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@ApplicationScoped
public class EventRecorder {
    private final Map<String, Queue<Object>> recordedEvents = new ConcurrentHashMap<>();

    public void record(String key, Object value) {
        received(key).add(value);
    }

    /**
     * @return live view of the recorded values - stays usable after the shutdown of the container
     */
    public Queue<Object> received(String key) {
        return recordedEvents.computeIfAbsent(key, newKey -> new ConcurrentLinkedQueue<>());
    }

    public List<Object> await(String key, int expectedCount) throws InterruptedException {
        Queue<Object> values = received(key);
        long timeout = System.currentTimeMillis() + 10_000;
        while (values.size() < expectedCount && System.currentTimeMillis() < timeout) {
            Thread.sleep(5);
        }
        return new ArrayList<>(values);
    }
}
