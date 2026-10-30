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
package org.os890.cdi.disruptor.test.perf;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;

/**
 * Apache Pekko (open-source fork of Akka) without CDI - one actor with the default dispatcher and mailbox,
 * the events get sent via {@code tell} (fire-and-forget).
 */
final class PekkoActorBenchTarget implements BenchTarget {
    private final BenchStatistics statistics = new BenchStatistics();
    private final ActorSystem actorSystem = ActorSystem.create("bench");
    private final ActorRef actor =
        actorSystem.actorOf(Props.create(BenchActor.class, () -> new BenchActor(statistics)));

    @Override
    public void fire(BenchEvent event) {
        actor.tell(event, ActorRef.noSender());
    }

    @Override
    public long received() {
        return statistics.received();
    }

    @Override
    public void close() {
        actorSystem.terminate();
        actorSystem.getWhenTerminated().toCompletableFuture().join();
    }

    static final class BenchActor extends AbstractActor {
        private final BenchStatistics statistics;

        BenchActor(BenchStatistics statistics) {
            this.statistics = statistics;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                .match(BenchEvent.class, statistics::record)
                .build();
        }
    }
}
