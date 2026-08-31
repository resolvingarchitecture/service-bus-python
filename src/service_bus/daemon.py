"""A reusable headless host for a :class:`~service_bus.bus.ServiceBus`.

Subclass and override the hooks::

    class MyDaemon(Daemon):
        def config_name(self) -> str:
            return "my.config"

        def on_bus_started(self, bus: ServiceBus, config: dict[str, str]) -> None:
            bus.register_and_start_services(FooService(), BarService())
            bus.await_running(10.0)

    if __name__ == "__main__":
        MyDaemon().launch()

Mirrors ``ra.servicebus.Daemon`` in ``service-bus-java``.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
from pathlib import Path

from .bus import ServiceBus

log = logging.getLogger("service_bus")


class Daemon:
    def __init__(self) -> None:
        self._bus: ServiceBus | None = None
        self._stop = threading.Event()
        self._shutting_down = threading.Lock()
        self._down = False

    def launch(self, argv: list[str] | None = None) -> None:
        argv = sys.argv[1:] if argv is None else argv
        config = self.load_config(argv)
        self.before_start(config)

        self._bus = ServiceBus(config=config)
        self._bus.start(config)
        self.on_bus_started(self._bus, config)

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, lambda *_: self.shutdown())
            except ValueError:  # pragma: no cover - not on main thread
                pass

        log.info("%s running.", type(self).__name__)
        self._stop.wait()

    def shutdown(self) -> None:
        with self._shutting_down:
            if self._down:
                return
            self._down = True
        log.info("%s shutting down...", type(self).__name__)
        try:
            self.on_stopping()
        except Exception:
            log.exception("on_stopping() raised")
        if self._bus is not None:
            ok = self._bus.graceful_shutdown()
            log.info("bus stopped=%s", ok)
        self._stop.set()

    @property
    def bus(self) -> ServiceBus | None:
        return self._bus

    # -- hooks --------------------------------------------------------

    def config_name(self) -> str:
        """Config file name looked up in the working directory."""
        return "service-bus.config"

    def load_config(self, argv: list[str]) -> dict[str, str]:
        """Parse ``key=value`` lines from the config file, overlaid with argv."""
        config: dict[str, str] = {}
        path = Path(self.config_name())
        if path.is_file():
            for line in path.read_text().splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                key, sep, value = stripped.partition("=")
                if sep:
                    config[key.strip()] = value.strip()
        for arg in argv:
            key, sep, value = arg.partition("=")
            if sep:
                config[key] = value
        return config

    def before_start(self, config: dict[str, str]) -> None:
        """Runs before the bus is created."""

    def on_bus_started(self, bus: ServiceBus, config: dict[str, str]) -> None:
        """Runs after the bus is running - register and start services here."""

    def on_stopping(self) -> None:
        """Runs at the start of shutdown, before the bus is stopped."""
