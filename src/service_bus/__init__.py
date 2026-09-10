"""service-bus: service lifecycle management and discovery over a seda-bus.

A Python port of ``service-bus-java``'s design onto ``seda-bus-python``. The
envelope, ``ServiceStatus`` and ``ServiceReport`` come from ``ra-common``, as
``service-bus-java`` gets them from ``ra-common-java``.
"""

from seda_bus import Envelope, make_envelope

from .bus import ServiceBus
from .daemon import Daemon
from .service import BaseService, Service, ServiceContext, ServiceReport, ServiceStatus

__version__ = "0.2.0"

__all__ = [
    "ServiceBus",
    "Service",
    "BaseService",
    "ServiceStatus",
    "ServiceReport",
    "ServiceContext",
    "Daemon",
    "Envelope",
    "make_envelope",
    "__version__",
]
