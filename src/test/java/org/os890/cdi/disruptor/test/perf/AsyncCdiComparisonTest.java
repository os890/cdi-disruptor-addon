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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Manual comparison with CDI async events, the plain Disruptor and virtual threads:
 * {@code mvn test -Dtest=AsyncCdiComparisonTest -Dperf=true -Dsurefire.failIfNoSpecifiedTests=false}
 * (add {@code -Powb} for OpenWebBeans).
 * <p>
 * Measures the duration from firing the first event until the last event was processed by the observer.
 * Every variant runs in its own JVM (otherwise the JIT-profiles of the previous variants would distort the results)
 * and in its own container (or without CDI) with one producer thread and one observer.
 */
@EnabledIfSystemProperty(named = "perf", matches = "true")
class AsyncCdiComparisonTest {
    private static final int EVENTS = 2_000_000;
    private static final int WARMUP_ROUNDS = 2;
    private static final int ROUNDS = 5;

    private static final List<Variant> VARIANTS = List.of(
        new Variant("CDI fireAsync / @ObservesAsync",
            () -> new CdiBenchTarget(CdiAsyncObserver.class, BenchPublisher::cdiAsync)),
        new Variant("virtual thread per event (without CDI)", VirtualThreadBenchTarget::new),
        new Variant("DisruptorEvent, default config",
            () -> new CdiBenchTarget(DisruptorBlockingObserver.class, BenchPublisher::disruptor)),
        new Variant("DisruptorEvent, default config, size 65536",
            () -> new CdiBenchTarget(DisruptorBlockingLargeObserver.class, BenchPublisher::disruptor)),
        new Variant("DisruptorEvent, requestContext=true",
            () -> new CdiBenchTarget(DisruptorRequestContextObserver.class, BenchPublisher::disruptor)),
        new Variant("DisruptorEvent, YieldingWaitStrategy, size 1024",
            () -> new CdiBenchTarget(DisruptorYieldingSmallObserver.class, BenchPublisher::disruptor)),
        new Variant("DisruptorEvent, YieldingWaitStrategy",
            () -> new CdiBenchTarget(DisruptorYieldingObserver.class, BenchPublisher::disruptor)),
        new Variant("DisruptorEvent, SleepingWaitStrategy",
            () -> new CdiBenchTarget(DisruptorSleepingObserver.class, BenchPublisher::disruptor)),
        new Variant("DisruptorEvent, LiteBlockingWaitStrategy",
            () -> new CdiBenchTarget(DisruptorLiteBlockingObserver.class, BenchPublisher::disruptor)),
        new Variant("plain Disruptor, Blocking, 1024, event objects",
            () -> new PlainDisruptorBenchTarget(1024, new BlockingWaitStrategy(), false)),
        new Variant("plain Disruptor, Yielding, 65536, event objects",
            () -> new PlainDisruptorBenchTarget(65536, new YieldingWaitStrategy(), false)),
        new Variant("plain Disruptor, Blocking, 1024, preallocated",
            () -> new PlainDisruptorBenchTarget(1024, new BlockingWaitStrategy(), true)),
        new Variant("plain Disruptor, Yielding, 65536, preallocated",
            () -> new PlainDisruptorBenchTarget(65536, new YieldingWaitStrategy(), true)));

    static Stream<String> variants() {
        return VARIANTS.stream().map(Variant::name);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("variants")
    void benchmark(String variant) throws IOException, InterruptedException {
        assumeTrue(!variant.contains("virtual thread") || VirtualThreads.isAvailable(), "requires Java 21+");

        Process process = new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp", System.getProperty("java.class.path"),
            AsyncCdiComparisonTest.class.getName(), variant)
            .redirectErrorStream(true)
            .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode = process.waitFor();

        output.lines().filter(line -> line.startsWith("BENCHCSV")).forEach(System.out::println);
        assertEquals(0, exitCode, output);
    }

    /**
     * Entry point of the JVM which runs a single variant.
     *
     * @param args name of the variant
     */
    public static void main(String[] args) {
        Variant variant = VARIANTS.stream()
            .filter(candidate -> candidate.name().equals(args[0]))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("unknown variant " + args[0]));

        try (BenchTarget target = variant.targetFactory().get()) {
            for (int round = 0; round < WARMUP_ROUNDS; round++) {
                measureFullRun(target);
            }
            long[] durations = new long[ROUNDS];
            for (int round = 0; round < ROUNDS; round++) {
                durations[round] = measureFullRun(target);
            }
            Arrays.sort(durations);

            System.out.printf(Locale.ROOT, "BENCHCSV;%s;%s;%d%n",
                variant.name(), cdiImplementation(), durations[ROUNDS / 2]);
        }
        // the observer threads are daemon threads, but e.g. thread-pools of CDI containers might not be
        System.exit(0);
    }

    // duration from firing the first event until the last event was processed
    private static long measureFullRun(BenchTarget target) {
        long expected = target.received() + EVENTS;
        long start = System.nanoTime();
        for (int i = 0; i < EVENTS; i++) {
            target.fire(new BenchEvent(i));
        }
        awaitReceived(target, expected);
        return System.nanoTime() - start;
    }

    private static void awaitReceived(BenchTarget target, long expected) {
        long timeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (target.received() < expected) {
            if (System.nanoTime() > timeout) {
                throw new IllegalStateException("received " + target.received() + " of " + expected + " events");
            }
            Thread.onSpinWait();
        }
    }

    private static String cdiImplementation() {
        try {
            Class.forName("org.jboss.weld.environment.se.Weld");
            return "Weld";
        } catch (ClassNotFoundException e) {
            return "OpenWebBeans";
        }
    }

    private record Variant(String name, Supplier<BenchTarget> targetFactory) {
    }
}
