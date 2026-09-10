# Changelog

## 0.2.0 — unreleased

- **`ServiceStatus`, `ServiceReport` and the envelope now come from `ra-common`**
  (as `service-bus-java` gets them from `ra-common-java`).
  - New dependency: `ra-common`.
  - `ServiceStatus` is ra-common's 19-state enum (was an 8-state local enum).
    `ServiceStatus.RUNNING` / `PAUSED` / `UNSTABLE` / … are unchanged names.
  - `BaseService` holds a `ra_common.ServiceCore` for status, observer
    notification and `report()`; the no-arg `start()` / `stop()` API is unchanged.
  - The bus carries `ra_common.Envelope`; construct with `make_envelope` (also
    re-exported from `service_bus`).
- Public `ServiceBus` API unchanged.

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
