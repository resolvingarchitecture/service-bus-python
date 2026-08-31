# service-bus (Python) — TODO

## Done (0.1.0)

- [x] `Service` ABC + `BaseService` (status tracking, `send`, `on_start`/`on_stop`
      hooks); `ServiceStatus` enum.
- [x] `ServiceBus`: register / `register_and_start_service(s)` / `start_all_registered`;
      `start_service` / `stop_service` / `unregister_service` (threaded).
- [x] Discovery: `get_service(name)`, `find_running_services(type_or_predicate)`,
      `running_services()` / `registered_services()`, `is_registered` / `is_running`.
- [x] `await_running(timeout, *names)`.
- [x] `pause` / `unpause` / `restart` / `shutdown` / `graceful_shutdown`; context
      manager.
- [x] Per-service `ServiceStatus` tracking + listeners; `UNSTABLE` -> restart.
- [x] Control commands via `headers["command"]` + `headers["service"]`.
- [x] Reusable `Daemon` base (hooks + `launch`).
- [x] `python -m service_bus` demo; test suite; `README.md`, `DESIGN.md`.

## Next

- [ ] `register_and_start_service_sync(service, timeout)` — register + start + await.
- [ ] Real dependency ordering for **start** using `depends_on()` (topological sort).
- [ ] Factory registration (`register(name, lambda: S())`) so `depends_on()` can
      auto-register and control-command `register` works over the bus.
- [ ] Readiness gate: hold delivery to a service until it reports `RUNNING`.
- [ ] Health policy beyond "UNSTABLE -> restart": restart backoff, give-up threshold.
- [ ] Control-command responses (ack/nack back to the sender).
- [ ] Surface seda-bus `stats()` per service.
- [ ] Free-threaded (`python3.14t`) test run in CI alongside the GIL build.
- [ ] Publish to PyPI (currently a path dependency on `../seda-bus-python`).
- [ ] `[tool.hatch.metadata] allow-direct-references` + a `seda-bus @ file://` pin so
      a fresh `pip install -e .` resolves the sibling without a separate step.
