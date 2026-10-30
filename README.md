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
# lasync-cdi

Portable CDI extension (CDI 4 / Jakarta EE 10+) which dispatches events via the
[LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor) - used like CDI events.

```java
@ApplicationScoped
public class OrderService {
    @Inject
    private DisruptorEvent<OrderPlaced> orderPlaced;

    @Inject
    @Premium
    private DisruptorEvent<OrderPlaced> premiumOrderPlaced;

    public void place(OrderPlaced order) {
        orderPlaced.fire(order);                 // blocks if the ring-buffer is full
        boolean published = orderPlaced.tryFire(order); // never blocks
    }
}

@ApplicationScoped
public class OrderObserver {
    // qualifiers and additional injected parameters work like for CDI observers
    void onPremiumOrder(@ObservesDisruptor @Premium OrderPlaced order, AuditService auditService) {
        // invoked on a disruptor thread with an active request-context
    }
}
```

## Semantics

* every observer method is an own event-handler (thread) of the disruptor:
  different observer methods run in parallel, a single observer method receives events in publishing order
* an observer gets notified if the runtime type of the event is assignable to the observed (raw) type
  and its qualifiers are a subset of the event qualifiers (`@Default` on the observer = only unqualified events)
* observers of `@Dependent` beans get a new instance per event, which gets destroyed afterwards
* pending events get processed before the application-context gets destroyed
  (max. `DisruptorConfig#shutdownTimeout`)
* exceptions of observer methods get logged and don't affect other observers

## Configuration

Provide a bean implementing `org.os890.cdi.disruptor.DisruptorConfig` to change the defaults
(ring-buffer size 1024, `BlockingWaitStrategy`, `ProducerType.MULTI`, 10s shutdown timeout, daemon threads).

## Restrictions

* `DisruptorEvent` is supported for injection points processed by the container (fields, constructors,
  initializers, producers) and as additional parameter of `@ObservesDisruptor` methods -
  not via programmatic lookup (`Instance<DisruptorEvent<...>>`)
* `fire` within an observer method can dead-lock if the ring-buffer is full - use `tryFire` there

## Build

```
mvn verify          # tests with Weld
mvn verify -Powb    # tests with OpenWebBeans
```

Apache RAT (license headers) and Checkstyle (`src/build/checkstyle.xml`) run in the `validate` phase.

## License

[Apache License, Version 2.0](LICENSE)

LMAX is a trademark of its respective owner. This project is an independent integration of the
Apache-2.0 licensed [LMAX Disruptor](https://github.com/LMAX-Exchange/disruptor) and isn't affiliated with,
sponsored or endorsed by LMAX.
