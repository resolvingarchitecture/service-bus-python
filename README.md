# service-bus (Python)

Service lifecycle management and discovery over a
[`seda-bus`](https://github.com/resolvingarchitecture/seda-bus-python).

seda-bus moves envelopes between named channels and walks their routing slips;
`service-bus` gives you **services** as the unit of composition &mdash; register
them, start/stop/pause them, let them find each other, and watch their health.
Each service becomes one seda-bus channel keyed by its name, with the service as
that channel's consumer.

This is a Python port of the design in
[`service-bus-java`](https://github.com/resolvingarchitecture/service-bus-java).

```python
from seda_bus import Envelope
from service_bus import ServiceBus, BaseService


class EchoService(BaseService):
    name = "echo"

    def handle(self, env: Envelope) -> bool:
        print(env.payload)
        return True


with ServiceBus() as bus:                 # start() on enter, graceful_shutdown() on exit
    bus.register_and_start_services(EchoService())
    bus.await_running(5.0, "echo")

    echo = bus.get_service("echo")
    transports = bus.find_running_services(MyProtocolService)   # by type
    net = bus.find_running_services(lambda s: s.name.startswith("net-"))  # by predicate

    bus.send(Envelope(to="echo", payload="hello"))
    bus.send(Envelope(to="echo", payload="hello"), on_complete=lambda e: print("done", e.id))
```

### As a daemon

```python
from service_bus import Daemon, ServiceBus


class MyDaemon(Daemon):
    def config_name(self) -> str:
        return "my.config"

    def on_bus_started(self, bus: ServiceBus, config: dict[str, str]) -> None:
        bus.register_and_start_services(FooService(), BarService())
        bus.await_running(10.0)


if __name__ == "__main__":
    MyDaemon().launch()
```

`launch` loads `key=value` config (file + argv), runs the hooks, installs
SIGINT/SIGTERM handlers, then blocks until the bus stops.

## API

| area       | methods                                                                          |
|------------|--------------------------------------------------------------------------------|
| register   | `register(service)`, `register_and_start_service(s)`, `register_and_start_services(*s)` |
| lifecycle  | `start_service`, `stop_service`, `start_all_registered`, `pause`/`unpause`, `restart`, `shutdown`/`graceful_shutdown` |
| discovery  | `get_service(name)`, `find_running_services(type_or_predicate)`, `running_services()`, `is_registered`/`is_running` |
| wait       | `await_running(timeout, *names)`                                                |
| health     | `get_service_status(name)`, `get_service_statuses()`, `add_service_status_listener` |
| bus        | `status`, `add_bus_status_listener`                                             |
| control    | `send` an `Envelope` with `headers["command"]` (`start`/`stop`/`pause`/...) and `headers["service"]` |

## Behaviour

- **Threaded start/stop** &mdash; `start_service` returns immediately; the service
  starts on its own thread. Join with `await_running`.
- **Advisory dependencies** &mdash; `Service.depends_on()` is checked at registration
  (warns); order your `register_and_start_services(...)` call.
- **UNSTABLE self-healing** &mdash; a service that reports `ServiceStatus.UNSTABLE`
  (via `report_unstable()` / `_update_status`) is stopped and restarted.
- **Dead letters** &mdash; use the underlying seda-bus:
  `bus.seda_bus.set_dead_letter_channel(source, dlq)`.

## Develop

```sh
python3.13 -m venv .venv
.venv/bin/pip install -e ../seda-bus-python -e ".[test]"
.venv/bin/python -m pytest
```

## Reference

- [`DESIGN.md`](DESIGN.md)
- [`TODO.md`](TODO.md)
