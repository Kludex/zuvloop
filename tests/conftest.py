from __future__ import annotations

import asyncio
import socket
import ssl
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

import zuv


def running_loop() -> zuv.EventLoop:
    """The running loop, typed - `asyncio.get_running_loop` widens to the ABC."""
    loop = asyncio.get_running_loop()
    assert isinstance(loop, zuv.EventLoop)
    return loop


@pytest.fixture
def anyio_backend() -> tuple[str, dict[str, object]]:
    return "asyncio", {"loop_factory": zuv.new_event_loop}


@pytest.fixture
def loop() -> Iterator[zuv.EventLoop]:
    """A loop that is *not* running, for driving lifecycle behaviour by hand."""
    instance = zuv.new_event_loop()
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
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "3650",
            "-keyout", str(key), "-out", str(cert),
            "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
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
