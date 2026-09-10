"""The unit of composition on the bus: a service.

Status, reports and the envelope come from ``ra-common`` (as ``service-bus-java``
gets them from ``ra-common-java``). ``ServiceStatus`` is ra-common's 19-state
enum; ``BaseService`` holds a :class:`ra_common.ServiceCore` for status
tracking, observer notification and reporting.
"""

from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable

from ra_common import Envelope, ServiceCore, ServiceReport, ServiceStatus

log = logging.getLogger("service_bus")

OnComplete = Callable[[Envelope], None]

__all__ = [
    "Service",
    "BaseService",
    "ServiceStatus",
    "ServiceReport",
    "ServiceContext",
    "StatusObserver",
]


@dataclass
class ServiceContext:
    """Handed to each service so it can read config and send envelopes."""

    config: dict[str, str]
    send: Callable[..., bool]


StatusObserver = Callable[[str, ServiceStatus], None]


class Service(ABC):
    """A service on the :class:`~service_bus.bus.ServiceBus`.

    Each service becomes one seda-bus channel keyed by :attr:`name`, with the
    service as that channel's consumer.
    """

    #: unique name - the registration key, the channel name, the routing target
    name: str

    def depends_on(self) -> list[str]:
        """Names of services that must start before this one (advisory)."""
        return []

    @abstractmethod
    def handle(self, envelope: Envelope) -> bool:
        """Handle an envelope routed to this service. Return False to nack."""

    @abstractmethod
    def start(self) -> bool: ...

    @abstractmethod
    def stop(self) -> bool: ...

    def pause(self) -> None: ...

    def unpause(self) -> None: ...

    @abstractmethod
    def get_status(self) -> ServiceStatus: ...


class BaseService(Service):
    """Tracks status via a :class:`ra_common.ServiceCore`, exposes ``send``,
    turns lifecycle calls into transitions.

    Override :meth:`handle` (and optionally :meth:`on_start` / :meth:`on_stop`).
    """

    name: str = "base-service"

    def __init__(self) -> None:
        self.core = ServiceCore(type(self).__name__)
        self._ctx: ServiceContext | None = None
        self._observer: StatusObserver | None = None
        self._status_lock = threading.Lock()

    def bind(self, ctx: ServiceContext, observer: StatusObserver) -> None:
        """Wired by :meth:`ServiceBus.register`."""
        self._ctx = ctx
        self._observer = observer
        self.core.config = dict(ctx.config)
        self.core.service_class_name = self.name

    def depends_on(self) -> list[str]:
        return list(self.core.services_dependent_upon)

    def start(self) -> bool:
        self._update_status(ServiceStatus.STARTING)
        ok = bool(self.on_start())
        self._update_status(ServiceStatus.RUNNING if ok else ServiceStatus.ERROR)
        return ok

    def stop(self) -> bool:
        self._update_status(ServiceStatus.SHUTTING_DOWN)
        ok = bool(self.on_stop())
        self._update_status(ServiceStatus.SHUTDOWN)
        return ok

    def pause(self) -> None:
        self._update_status(ServiceStatus.PAUSED)

    def unpause(self) -> None:
        self._update_status(ServiceStatus.RUNNING)

    def get_status(self) -> ServiceStatus:
        return self.core.status

    def report(self) -> ServiceReport:
        return self.core.report()

    # -- for subclasses ------------------------------------------------

    def on_start(self) -> bool:
        """Override for startup work. Return False to fail the start."""
        return True

    def on_stop(self) -> bool:
        """Override for shutdown work."""
        return True

    def send(self, envelope: Envelope, *, on_complete: OnComplete | None = None) -> bool:
        assert self._ctx is not None, "service not registered"
        return self._ctx.send(envelope, on_complete=on_complete)

    @property
    def config(self) -> dict[str, str]:
        return self._ctx.config if self._ctx else {}

    def _update_status(self, status: ServiceStatus) -> None:
        with self._status_lock:
            if self.core.status is status:
                return
            self.core.status = status
        if self._observer is not None:
            try:
                self._observer(self.name, status)
            except Exception:  # pragma: no cover - defensive
                log.exception("status observer raised for %s", self.name)

    #: ask the bus for a restart without importing the enum path
    def report_unstable(self) -> None:
        self._update_status(ServiceStatus.UNSTABLE)
