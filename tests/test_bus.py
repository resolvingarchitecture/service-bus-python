import threading
import time

from seda_bus import Envelope

from service_bus import BaseService, ServiceBus, ServiceStatus


class RecordingService(BaseService):
    name = "recorder"

    def __init__(self, name: str = "recorder") -> None:
        super().__init__()
        self.name = name
        self.seen: list[str] = []

    def handle(self, envelope: Envelope) -> bool:
        self.seen.append(envelope.id)
        return True


class Transport(BaseService):
    def __init__(self, name: str) -> None:
        super().__init__()
        self.name = name
        self.sent: list[str] = []

    def handle(self, envelope: Envelope) -> bool:
        self.sent.append(envelope.id)
        return True


def test_register_start_discover_await():
    with ServiceBus() as bus:
        rec = RecordingService()
        assert bus.register_and_start_service(rec)
        assert bus.await_running(2.0, "recorder")

        assert bus.is_registered("recorder")
        assert bus.is_running("recorder")
        assert bus.get_service("recorder") is rec
        assert bus.get_service_status("recorder") is ServiceStatus.RUNNING
        assert bus.find_running_services(RecordingService) == [rec]


def test_routes_envelope_and_fires_callback():
    with ServiceBus() as bus:
        rec = RecordingService()
        bus.register_and_start_service(rec)
        bus.await_running(2.0, "recorder")

        done = threading.Event()
        env = Envelope(to="recorder", payload="hi")
        bus.send(env, on_complete=lambda _e: done.set())
        assert done.wait(3.0)
        assert env.id in rec.seen


def test_routing_slip_walks_services_in_order():
    with ServiceBus() as bus:
        a = RecordingService("a")
        b = RecordingService("b")
        bus.register_and_start_services(a, b)
        bus.await_running(2.0, "a", "b")

        done = threading.Event()
        env = Envelope(to="a", payload="x", slip=["b"])
        bus.send(env, on_complete=lambda _e: done.set())
        assert done.wait(3.0)
        assert env.id in a.seen
        assert env.id in b.seen


def test_typed_discovery():
    with ServiceBus() as bus:
        bus.register_and_start_services(Transport("i2p"), Transport("tor"), RecordingService())
        bus.await_running(2.0)

        transports = bus.find_running_services(Transport)
        assert sorted(t.name for t in transports) == ["i2p", "tor"]


def test_pause_unpause():
    with ServiceBus() as bus:
        rec = RecordingService()
        bus.register_and_start_service(rec)
        bus.await_running(2.0, "recorder")

        assert bus.pause()
        assert bus.status == "paused"
        assert rec.get_status() is ServiceStatus.PAUSED
        assert bus.unpause()
        assert bus.status == "running"


def test_unstable_service_is_restarted():
    starts = {"n": 0}

    class Flaky(BaseService):
        name = "flaky"

        def on_start(self) -> bool:
            starts["n"] += 1
            return True

        def handle(self, envelope: Envelope) -> bool:
            return True

    with ServiceBus() as bus:
        flaky = Flaky()
        bus.register_and_start_service(flaky)
        bus.await_running(2.0, "flaky")
        assert starts["n"] == 1

        flaky.report_unstable()
        time.sleep(0.2)
        assert starts["n"] == 2


def test_control_command_over_the_bus():
    with ServiceBus() as bus:
        rec = RecordingService()
        bus.register(rec)  # registered but not started
        assert not bus.is_running("recorder")

        env = Envelope(to="recorder", payload=None,
                       headers={"command": "start", "service": "recorder"})
        bus.send(env)
        assert bus.await_running(2.0, "recorder")
