"""Service lifecycle management and discovery over a :class:`seda_bus.SEDABus`.

seda-bus is a transport: named channels, a shared worker pool, bounded queues,
the routing-slip engine. It has no notion of a "service". ``ServiceBus`` adds
that: register services, start / stop / pause them, let them find each other,
and watch their health. Each service becomes one seda-bus channel keyed by its
name, with the service as that channel's consumer.

Mirrors the design of ``service-bus-java``.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable

from seda_bus import Envelope, SEDABus

from .service import BaseService, OnComplete, Service, ServiceContext, ServiceStatus

log = logging.getLogger("service_bus")

#: lifecycle commands carried on ``envelope.headers["command"]``
CONTROL_COMMANDS = frozenset(
    {"register", "unregister", "start", "stop", "pause", "unpause"}
)

ServiceStatusListener = Callable[[str, ServiceStatus], None]
BusStatusListener = Callable[[str], None]

TypeOrPredicate = type | Callable[[Service], bool]


class ServiceBus:
    def __init__(
        self,
        *,
        seda: SEDABus | None = None,
        config: dict[str, str] | None = None,
        workers: int | None = None,
    ) -> None:
        self._seda = seda if seda is not None else SEDABus(workers=workers)
        self._own_seda = seda is None
        self._config: dict[str, str] = dict(config or {})

        self._registered: dict[str, Service] = {}
        self._running: dict[str, Service] = {}
        self._statuses: dict[str, ServiceStatus] = {}
        self._lock = threading.RLock()

        self._svc_listeners: list[ServiceStatusListener] = []
        self._bus_listeners: list[BusStatusListener] = []
        self._status = "stopped"

    # -- lifecycle ------------------------------------------------------

    def start(self, config: dict[str, str] | None = None) -> None:
        self._set_status("starting")
        if config:
            self._config.update(config)
        self._seda.start()
        self._seda.resume()
        self._set_status("running")

    def pause(self) -> bool:
        if self._status != "running":
            return False
        for svc in list(self._running.values()):
            svc.pause()
        self._seda.pause()
        self._set_status("paused")
        return True

    def unpause(self) -> bool:
        if self._status != "paused":
            return False
        self._seda.resume()
        for svc in list(self._running.values()):
            svc.unpause()
        self._set_status("running")
        return True

    def restart(self) -> bool:
        saved = dict(self._config)
        return self.shutdown() and (self.start(saved) or True)

    def shutdown(self, timeout: float = 5.0) -> bool:
        return self._do_shutdown(graceful=False, timeout=timeout)

    def graceful_shutdown(self, timeout: float = 30.0) -> bool:
        return self._do_shutdown(graceful=True, timeout=timeout)

    def _do_shutdown(self, *, graceful: bool, timeout: float) -> bool:
        self._set_status("stopping")
        threads = []
        for name in list(self._running):
            svc = self._running.get(name)
            if svc is None:
                continue

            def _stop(n: str = name, s: Service = svc) -> None:
                try:
                    if s.stop():
                        self._running.pop(n, None)
                except Exception:
                    log.exception("%s.stop() raised", n)

            t = threading.Thread(target=_stop, name=f"{name}-stop", daemon=True)
            t.start()
            threads.append(t)

        deadline = time.monotonic() + timeout
        for t in threads:
            t.join(max(0.0, deadline - time.monotonic()))

        if self._own_seda:
            bus_ok = (
                self._seda.shutdown(timeout)
                if graceful
                else (self._seda.shutdown_now() or True)
            )
        else:
            bus_ok = True
        self._set_status("stopped")
        return bool(bus_ok) and not self._running

    @property
    def status(self) -> str:
        return self._status

    # -- registration -------------------------------------------------

    def register(self, service: Service) -> bool:
        with self._lock:
            if service.name in self._registered:
                return True
            for dep in service.depends_on():
                if dep not in self._registered:
                    log.warning(
                        "%s depends on unregistered %r; register it first",
                        service.name,
                        dep,
                    )
            ctx = ServiceContext(config=self._config, send=self._send_for_service)
            if isinstance(service, BaseService):
                service.bind(ctx, self._on_service_status)
            self._seda.channel(service.name)
            self._seda.subscribe(service.name, service.handle)
            self._registered[service.name] = service
            self._statuses[service.name] = ServiceStatus.NOT_INITIALIZED
            return True

    def start_service(self, name: str) -> bool:
        svc = self._registered.get(name)
        if svc is None:
            log.warning("not registered, cannot start: %s", name)
            return False
        if name in self._running:
            return True

        def _run() -> None:
            try:
                if svc.start():
                    self._running[name] = svc
                else:
                    log.warning("failed to start: %s", name)
            except Exception:
                log.exception("%s.start() raised", name)

        threading.Thread(target=_run, name=f"{name}-start", daemon=True).start()
        return True

    def stop_service(self, name: str) -> bool:
        svc = self._running.get(name)
        if svc is None:
            return True

        def _run() -> None:
            try:
                if svc.stop():
                    self._running.pop(name, None)
            except Exception:
                log.exception("%s.stop() raised", name)

        threading.Thread(target=_run, name=f"{name}-stop", daemon=True).start()
        return True

    def unregister_service(self, name: str) -> bool:
        self.stop_service(name)
        with self._lock:
            self._registered.pop(name, None)
            self._statuses.pop(name, None)
        return True

    def register_and_start_service(self, service: Service) -> bool:
        return self.register(service) and self.start_service(service.name)

    def register_and_start_services(self, *services: Service) -> None:
        for svc in services:
            self.register_and_start_service(svc)

    def start_all_registered(self) -> None:
        for name in list(self._registered):
            if name not in self._running:
                self.start_service(name)

    def await_running(self, timeout: float, *names: str) -> bool:
        targets = list(names) if names else list(self._registered)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if all(n in self._running for n in targets):
                return True
            time.sleep(0.02)
        return all(n in self._running for n in targets)

    # -- discovery --------------------------------------------------

    def registered_service_names(self) -> list[str]:
        return list(self._registered)

    def running_service_names(self) -> list[str]:
        return list(self._running)

    def registered_services(self) -> list[Service]:
        return list(self._registered.values())

    def running_services(self) -> list[Service]:
        return list(self._running.values())

    def get_service(self, name: str) -> Service | None:
        return self._registered.get(name)

    def find_running_services(self, type_or_predicate: TypeOrPredicate) -> list[Service]:
        """Running services matching a type or a predicate.

        ``bus.find_running_services(ProtocolService)`` or
        ``bus.find_running_services(lambda s: s.name.startswith("net-"))``.
        """
        if isinstance(type_or_predicate, type):
            cls = type_or_predicate
            return [s for s in self._running.values() if isinstance(s, cls)]
        pred = type_or_predicate
        return [s for s in self._running.values() if pred(s)]

    def is_registered(self, name: str) -> bool:
        return name in self._registered

    def is_running(self, name: str) -> bool:
        return name in self._running

    def get_service_status(self, name: str) -> ServiceStatus | None:
        return self._statuses.get(name)

    def get_service_statuses(self) -> dict[str, ServiceStatus]:
        return dict(self._statuses)

    # -- status observation ---------------------------------------

    def add_bus_status_listener(self, listener: BusStatusListener) -> None:
        self._bus_listeners.append(listener)

    def add_service_status_listener(self, listener: ServiceStatusListener) -> None:
        self._svc_listeners.append(listener)

    def _on_service_status(self, name: str, status: ServiceStatus) -> None:
        self._statuses[name] = status
        for listener in list(self._svc_listeners):
            try:
                listener(name, status)
            except Exception:  # pragma: no cover - defensive
                log.exception("service-status listener raised")
        if status is ServiceStatus.UNSTABLE:
            svc = self._registered.get(name)
            if svc is not None:
                log.warning("%s UNSTABLE; restarting...", name)

                def _restart() -> None:
                    try:
                        svc.stop()
                        if svc.start():
                            self._running[name] = svc
                    except Exception:
                        log.exception("restart of %s failed", name)

                threading.Thread(
                    target=_restart, name=f"{name}-restart", daemon=True
                ).start()

    def _set_status(self, status: str) -> None:
        self._status = status
        for listener in list(self._bus_listeners):
            try:
                listener(status)
            except Exception:  # pragma: no cover - defensive
                log.exception("bus-status listener raised")

    # -- messaging -----------------------------------------------

    def send(
        self,
        envelope: Envelope,
        *,
        timeout: float | None = None,
        on_complete: OnComplete | None = None,
    ) -> bool:
        command = envelope.headers.get("command")
        if command in CONTROL_COMMANDS:
            self._process_command(command, envelope)
        return self._seda.publish(envelope, timeout=timeout, on_complete=on_complete)

    def _send_for_service(
        self, envelope: Envelope, *, on_complete: OnComplete | None = None
    ) -> bool:
        return self.send(envelope, on_complete=on_complete)

    @property
    def seda_bus(self) -> SEDABus:
        return self._seda

    def _process_command(self, command: str, envelope: Envelope) -> None:
        name = envelope.headers.get("service")
        if not name:
            log.warning("control command %r with no headers['service']", command)
            return
        if command == "start":
            self.start_service(name)
        elif command == "stop":
            self.stop_service(name)
        elif command == "unregister":
            self.unregister_service(name)
        elif command == "pause":
            svc = self._registered.get(name)
            if svc:
                svc.pause()
        elif command == "unpause":
            svc = self._registered.get(name)
            if svc:
                svc.unpause()
        elif command == "register":
            log.warning("'register' over the bus needs a factory; ignored")

    # -- context manager --------------------------------------

    def __enter__(self) -> "ServiceBus":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.graceful_shutdown()
