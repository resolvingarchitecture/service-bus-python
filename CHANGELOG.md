# Changelog

## 0.1.0

Initial release. A Python port of `service-bus-java`'s design onto
`seda-bus-python`.

- `Service` / `BaseService` / `ServiceStatus`.
- `ServiceBus`: register, start/stop/pause/restart (threaded starts), discovery
  (`get_service`, `find_running_services` by type or predicate), `await_running`,
  per-service status + listeners, `UNSTABLE` -> restart, control commands via
  envelope headers, context-manager support.
- Reusable `Daemon` base (`config_name` / `before_start` / `on_bus_started` /
  `on_stopping` + `launch`).
- `python -m service_bus` demo.
- Depends on `seda-bus` (`seda-bus-python`).
