"""service-bus: service lifecycle management and discovery over a seda-bus.

A Python port of ``service-bus-java``'s design onto ``seda-bus-python``.
"""

from seda_bus import Envelope

from .bus import ServiceBus
from .daemon import Daemon
from .service import BaseService, Service, ServiceContext, ServiceStatus

__version__ = "0.1.0"

__all__ = [
    "ServiceBus",
    "Service",
    "BaseService",
    "ServiceStatus",
    "ServiceContext",
    "Daemon",
    "Envelope",
    "__version__",
]
