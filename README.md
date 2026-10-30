<!--
  Copyright 2026 os890

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# cdi-disruptor-addon

Portable CDI extension (CDI 4 / Jakarta EE 10+) which dispatches events via the
[LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor) - used like CDI events,
but asynchronous, allocation-free and with bounded (named) ring-buffers.

## Usage

The add-on isn't published to Maven Central yet - install it into the local Maven repository first:

```
git clone https://github.com/os890/cdi-disruptor-addon.git
cd cdi-disruptor-addon
mvn install
```

Add the dependency - the extension gets activated automatically:

```xml
<dependency>
    <groupId>org.os890.cdi</groupId>
    <artifactId>cdi-disruptor-addon</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Fire events:

```java
@ApplicationScoped
public class OrderService {
    @Inject
    private DisruptorEvent<OrderPlaced> orderPlacedEvent;

    @Inject
    @Premium
    private DisruptorEvent<OrderPlaced> premiumOrderPlacedEvent;

    public void placeOrder(OrderPlaced order) {
        orderPlacedEvent.fire(order); // returns immediately - waits only if the ring-buffer is full
    }

    public boolean placePremiumOrder(OrderPlaced order) {
        return premiumOrderPlacedEvent.tryFire(order); // never waits - false if the ring-buffer is full
    }
}
```

Observe events:

```java
@ApplicationScoped
public class OrderObserver {
    // qualifiers and additional injected parameters work like for CDI observers
    void onPremiumOrder(@ObservesDisruptor @Premium OrderPlaced order, AuditService auditService) {
        // invoked on a disruptor thread
    }

    // opt-in: new request-context per event (needed for @RequestScoped beans, costs throughput)
    void onOrder(@ObservesDisruptor(requestContext = true) OrderPlaced order, RequestScopedAudit audit) {
    }
}
```

Requirements: Java 17+, a CDI 4 container with support for portable extensions (CDI Full - e.g. Weld or
OpenWebBeans), Disruptor 4 (`com.lmax:disruptor`, transitive dependency), MicroProfile Config is optional.

## How it works

```mermaid
flowchart LR
    subgraph producers["Producers (any thread)"]
        orderService["OrderService<br/>@Inject DisruptorEvent#lt;OrderPlaced#gt;"]
        auditService["AuditService<br/>@Inject @RingBuffer(name = #quot;audit#quot;)<br/>DisruptorEvent#lt;AuditEntry#gt;"]
    end

    subgraph ringBuffers["Ring-buffers (one disruptor each)"]
        defaultRingBuffer[("ring-buffer<br/>default")]
        auditRingBuffer[("ring-buffer<br/>audit")]
    end

    subgraph observers["Observer methods"]
        orderObserver["@ObservesDisruptor<br/>OrderPlaced"]
        premiumObserver["@ObservesDisruptor @Premium<br/>OrderPlaced"]
        auditObserver["@ObservesDisruptor @RingBuffer(name = #quot;audit#quot;)<br/>AuditEntry"]
    end

    config["Configuration per ring-buffer<br/>1. defaults<br/>2. @RingBuffer attributes<br/>3. MicroProfile Config<br/>4. DisruptorConfig beans"]

    orderService -- "fire / tryFire" --> defaultRingBuffer
    auditService -- "fire / tryFire" --> auditRingBuffer
    defaultRingBuffer --> orderObserver
    defaultRingBuffer --> premiumObserver
    auditRingBuffer --> auditObserver
    config -. "applied at startup" .-> ringBuffers
```

* `fire` publishes the event into the ring-buffer of its `DisruptorEvent` and returns -
  the observer methods which match the event-type and qualifiers are resolved once per event-type
* every observer method of a ring-buffer has its own event-handler thread, which skips events it doesn't observe
* a ring-buffer slot gets reused once all observer methods of the ring-buffer processed it

Lifecycle of the extension:

```mermaid
sequenceDiagram
    participant Container as CDI container
    participant Extension as DisruptorExtension
    participant Disruptors as Disruptor per ring-buffer

    Container->>Extension: ProcessInjectionPoint (DisruptorEvent)
    Extension-->>Container: bean per ring-buffer and qualifiers
    Container->>Extension: ProcessManagedBean (methods with @ObservesDisruptor)
    Container->>Extension: AfterDeploymentValidation
    Extension->>Disruptors: resolve configuration and start
    Note over Container,Disruptors: application fires events
    Container->>Extension: @BeforeDestroyed(ApplicationScoped.class)
    Extension->>Disruptors: process pending events and shut down
```

## Semantics

* every observer method is an own event-handler (thread) of the disruptor:
  different observer methods run in parallel, a single observer method receives events in publishing order
* an observer gets notified if the runtime type of the event is assignable to the observed (raw) type
  and its qualifiers are a subset of the event qualifiers (`@Default` on the observer = only unqualified events)
* the request-context is only active if requested via `@ObservesDisruptor(requestContext = true)` -
  activating it per event reduces the throughput by ~10x (measured end-to-end with `YieldingWaitStrategy`:
  ~5.5M vs. ~60M events/s)
* `fire` doesn't allocate and doesn't block unless the ring-buffer is full (backpressure) - `tryFire` never blocks
* every named ring-buffer is a bulkhead: a slow observer can only fill its own ring-buffer
  (but it slows down all observers of the same ring-buffer, once the ring-buffer is full)
* observers of `@Dependent` beans get a new instance per event, which gets destroyed afterwards
* pending events get processed before the application-context gets destroyed
  (max. the shutdown-timeout of the ring-buffer, see `RingBufferSettings#shutdownTimeout`)
* exceptions of observer methods get logged and don't affect other observers

## Ring-buffers

By default all events use the ring-buffer `default` (`@RingBuffer(size = 4096)` without a name configures it).
`@RingBuffer(name = "...")` selects a named ring-buffer with an own configuration and own threads.
There is exactly one ring-buffer per name - all injection points and observers with the same name share it.

```java
@Inject
@RingBuffer(name = "audit", size = 4096, waitStrategy = YieldingWaitStrategy.class)
private DisruptorEvent<AuditEntry> auditEvent;

void onAuditEntry(@ObservesDisruptor @RingBuffer(name = "audit") AuditEntry entry) {
    // only receives events of the ring-buffer "audit"
}
```

The configuration of every ring-buffer gets resolved once during the bootstrapping
(later entries override former ones):

1. defaults: size 1024, `BlockingWaitStrategy`, `ProducerType.MULTI`, 10s shutdown-timeout, daemon threads
2. `@Nonbinding` attributes of `@RingBuffer` (`size`, `waitStrategy`) -
   all usages with the same name have to specify the same values (or none)
3. MicroProfile Config (optional, used if available):
   ```properties
   cdi.disruptor.audit.size=8192
   cdi.disruptor.audit.wait-strategy=BusySpinWaitStrategy
   cdi.disruptor.audit.producer-type=SINGLE
   cdi.disruptor.audit.shutdown-timeout-ms=5000
   ```
4. beans implementing `DisruptorConfig` (e.g. for custom thread-factories or wait-strategies with parameters):
   ```java
   @ApplicationScoped
   public class AuditRingBufferConfig implements DisruptorConfig {
       @Override
       public void configure(RingBufferSettings settings) {
           if ("audit".equals(settings.name())) {
               settings.waitStrategy(new TimeoutBlockingWaitStrategy(1, TimeUnit.MILLISECONDS));
           }
       }
   }
   ```

## Restrictions

* `DisruptorEvent` is supported for injection points processed by the container (fields, constructors,
  initializers, producers) and as additional parameter of `@ObservesDisruptor` methods -
  not via programmatic lookup (`Instance<DisruptorEvent<...>>`)
* `fire` within an observer method can dead-lock if the ring-buffer is full - use `tryFire` there
* ring-buffers are created during the bootstrapping - selecting a ring-buffer at runtime
  (`select(RingBuffer.Literal.of("x"))`) only reaches observers if the name is used by an observer,
  the configuration attributes of `@RingBuffer` are only evaluated at injection points and observer methods
* events are matched against the raw type of the observed type (e.g. `List<String>` observes all lists)
* private observer methods aren't supported for normal-scoped beans

## Build

```
mvn verify          # tests with Weld (with and without MicroProfile Config)
mvn verify -Powb    # tests with OpenWebBeans (with and without MicroProfile Config)
mvn test -Dtest=ThroughputTest -Dperf=true -Dsurefire.failIfNoSpecifiedTests=false   # manual throughput check
```

Apache RAT (license headers) and Checkstyle (`src/build/checkstyle.xml`) run in the `validate` phase.

## License

[Apache License, Version 2.0](LICENSE)

LMAX is a trademark of its respective owner. This project is an independent integration of the
Apache-2.0 licensed [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor) and isn't affiliated with,
sponsored or endorsed by LMAX.
