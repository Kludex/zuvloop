from __future__ import annotations

import asyncio
import socket
import ssl
import subprocess
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import TypedDict

import pytest
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import HistogramDataPoint, InMemoryMetricReader, NumberDataPoint
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.util.types import AttributeValue

import zuvloop


class ExceptionContext(TypedDict):
    """The keys of asyncio's exception-handler context that the tests inspect."""

    message: str
    exception: BaseException | None


def collect_contexts(loop: asyncio.AbstractEventLoop) -> list[ExceptionContext]:
    """Install an exception handler that records every reported context."""
    seen: list[ExceptionContext] = []
    loop.set_exception_handler(
        lambda _loop, context: seen.append(
            ExceptionContext(
                message=message if isinstance(message := context.get("message"), str) else "",
                exception=exception if isinstance(exception := context.get("exception"), BaseException) else None,
            )
        )
    )
    return seen


@pytest.fixture(scope="session")
def _telemetry() -> tuple[InMemorySpanExporter, InMemoryMetricReader]:
    """Install real OpenTelemetry providers once, so tests read what is exported."""
    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(tracer_provider)

    reader = InMemoryMetricReader()
    metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
    return exporter, reader


@pytest.fixture
def telemetry(_telemetry: tuple[InMemorySpanExporter, InMemoryMetricReader]) -> Telemetry:
    exporter, reader = _telemetry
    exporter.clear()
    return Telemetry(exporter, reader)


class Telemetry:
    """Reads back what the instrumentation layer actually exported."""

    def __init__(self, exporter: InMemorySpanExporter, reader: InMemoryMetricReader) -> None:
        self._exporter = exporter
        self._reader = reader
        self._collected: dict[str, float] | None = None

    def spans(self, name: str | None = None) -> list[ReadableSpan]:
        found = self._exporter.get_finished_spans()
        return [span for span in found if name is None or span.name == name]

    def metric(self, name: str) -> float | None:
        # Collection drains the reader, so everything is read in one pass and
        # the result reused for the rest of the test.
        if self._collected is None:
            self._collected = {}
            data = self._reader.get_metrics_data()
            for resource in data.resource_metrics if data else ():
                for scope in resource.scope_metrics:
                    for metric in scope.metrics:
                        points: Sequence[NumberDataPoint | HistogramDataPoint] = getattr(metric.data, "data_points", ())
                        for point in points:
                            # Histograms report a count rather than a single value.
                            if isinstance(point, HistogramDataPoint):
                                self._collected[metric.name] = point.count
                            else:
                                self._collected[metric.name] = point.value
        return self._collected.get(name)

    def counted(self, name: str) -> float:
        """A metric that must be present."""
        value = self.metric(name)
        assert value is not None, f"{name} was never recorded"
        return value


def attribute(span: ReadableSpan, key: str) -> AttributeValue:
    """A span attribute, without OpenTelemetry's value union getting in the way."""
    assert span.attributes is not None
    return span.attributes[key]


def numeric_attribute(span: ReadableSpan, key: str) -> float:
    """A span attribute that the test compares as a number."""
    value = attribute(span, key)
    assert isinstance(value, (int, float))
    return value


def running_loop() -> zuvloop.EventLoop:
    """The running loop, typed - `asyncio.get_running_loop` widens to the ABC."""
    loop = asyncio.get_running_loop()
    assert isinstance(loop, zuvloop.EventLoop)
    return loop


@pytest.fixture
def anyio_backend() -> tuple[str, dict[str, Callable[[], asyncio.AbstractEventLoop]]]:
    return "asyncio", {"loop_factory": zuvloop.new_event_loop}


@pytest.fixture
def loop() -> Iterator[zuvloop.EventLoop]:
    """A loop that is *not* running, for driving lifecycle behaviour by hand."""
    instance = zuvloop.new_event_loop()
    try:
        yield instance
    finally:
        instance.close()


@pytest.fixture(scope="session")
def certificate(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    directory = tmp_path_factory.mktemp("certs")
    cert, key = directory / "localhost.pem", directory / "localhost.key"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "3650",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    return cert, key


@pytest.fixture
def server_context(certificate: tuple[Path, Path]) -> ssl.SSLContext:
    cert, key = certificate
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    return context


@pytest.fixture
def client_context(certificate: tuple[Path, Path]) -> ssl.SSLContext:
    cert, _key = certificate
    context = ssl.create_default_context(cafile=str(cert))
    context.check_hostname = True
    return context


@pytest.fixture
def closed_port() -> int:
    """A port nothing is listening on, for exercising connection failures."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
