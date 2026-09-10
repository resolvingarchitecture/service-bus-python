"""Demo: two services, a routing slip between them, and discovery.

    python -m service_bus
"""

from __future__ import annotations

import logging
import threading

from seda_bus import Envelope, make_envelope

from .bus import ServiceBus
from .service import BaseService


class Uppercase(BaseService):
    name = "uppercase"

    def handle(self, env: Envelope) -> bool:
        env.add_content(str(env.content()).upper())
        return True


class Printer(BaseService):
    name = "printer"

    def handle(self, env: Envelope) -> bool:
        print(f"  [{env.headers.get('from', '?')}] {env.content()}")
        return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")
    with ServiceBus() as bus:
        bus.register_and_start_services(Uppercase(), Printer())
        bus.await_running(5.0)

        print("running:", bus.running_service_names())
        print("discovered:", [type(s).__name__ for s in bus.find_running_services(BaseService)])

        done = threading.Event()
        for word in ("alpha", "bravo", "charlie"):
            bus.send(
                make_envelope("uppercase", word, slip=["printer"],
                              headers={"from": "demo"}),
                on_complete=lambda _e: done.set(),
            )
        done.wait(2.0)


if __name__ == "__main__":
    main()
