# service-bus (Python) — Design

A port of [`service-bus-java`](https://github.com/resolvingarchitecture/service-bus-java)'s
design onto [`seda-bus-python`](https://github.com/resolvingarchitecture/seda-bus-python).
Same model, idiomatic to Python.

## Role

`seda-bus` is a transport: named channels, a shared `ThreadPoolExecutor`, bounded
queues, the routing-slip engine. It has no notion of a "service".

`service-bus` adds that notion and the management that comes with it:

- **composition** — a `Service` is the unit; each becomes one channel + its
  consumer;
- **lifecycle** — register, start, stop, pause, restart, per service and for the
  whole bus;
- **discovery** — look services up by name, by type, or by predicate;
- **health** — per-service `ServiceStatus` tracked, published to listeners, and an
  `UNSTABLE` service stopped and restarted;
- **remote control** — an `Envelope` carrying `headers["command"]` acts on a service;
- **a reusable `Daemon`** host.

## Model

    ServiceBus
      _seda                SEDABus (created, or supplied)
      _registered          name -> Service
      _running             name -> Service
      _statuses            name -> ServiceStatus
      _svc_listeners / _bus_listeners

    register(service):
      bind a ServiceContext (config + send) and a status observer onto the service
      _seda.channel(service.name)
      _seda.subscribe(service.name, service.handle)

    start_service(name):  threading.Thread -> service.start() -> _running[name] = service
    send(env):            if headers["command"] in CONTROL_COMMANDS -> _process_command;
                          _seda.publish(env)

"name" is the registration key, the channel name, and the value a routing slip
carries (`Envelope.to` / `Envelope.slip`).

## Differences from `service-bus-java` (and why)

| java | here | reason |
|------|------|--------|
| reflective `Class.forName` registration | pass a `Service` instance | the instance carries its own `name` |
| dependency-ordered auto-registration | `depends_on()` is advisory (warns) | can't build an unknown dependency without a factory; caller orders `register_and_start_services` |
| `AppThread` per start/stop | `threading.Thread` per start/stop | same "async, join later" shape; `await_running` joins |
| `findRunningServices(Class)` | `find_running_services(type_or_predicate)` | Python takes a class *or* a callable |
| `ControlCommand` enum on the envelope | `headers["command"]` + `headers["service"]` | seda-bus envelopes carry a `headers` dict, not a typed command path |
| `PersistDeadLetter` file with rotation | `seda.set_dead_letter_channel(source, dlq)` | seda-bus-python already models a dead-letter channel |
| routing slip is a LIFO stack | seda-bus-python slip is FIFO | property of the underlying bus; the router pattern is unchanged |

## Threading

`start_service` / `stop_service` run `service.start()` / `.stop()` on a daemon
`threading.Thread` (services may block on network or disk). `await_running(timeout,
*names)` polls `_running` until the targets appear. On a free-threaded (PEP 703)
build the services' `handle` work also runs in parallel across the seda-bus pool.

State maps are plain dicts guarded by an `RLock` on the registration path; reads
(`is_running`, `get_service`) are lock-free snapshots.

## Status and self-healing

A `BaseService` calls `_update_status(status)`, which reaches
`ServiceBus._on_service_status(name, status)`. The bus records it, forwards it to
every service-status listener, and on `ServiceStatus.UNSTABLE` spawns a thread that
stops and restarts the service.

## Daemon

`Daemon` has `config_name` / `load_config` / `before_start` / `on_bus_started` /
`on_stopping` hooks and a `launch(argv)` that loads config, runs the hooks, installs
SIGINT/SIGTERM handlers, and blocks on a `threading.Event` until `shutdown()`.

## Not here

- No priority between services (that is seda-bus per-stage config).
- No distributed registry — one process, one bus.
- No hot reload — restart re-runs `start()` on the same instance.
