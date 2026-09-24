from __future__ import annotations

import asyncio
import concurrent.futures
import socket
import threading

import pytest

import zuvloop


@pytest.mark.parametrize("debug", [False, True])
def test_close_is_rejected_until_final_flushing_finishes(debug: bool) -> None:
    loop = zuvloop.new_event_loop()
    loop.set_debug(debug)
    flushing = threading.Event()
    release = threading.Event()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="retained-executor")
    loop.set_default_executor(executor)

    class FlushProtocol(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            assert isinstance(transport, asyncio.Transport)
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            self.transport.write(data)
            self.transport.set_write_buffer_limits(high=0, low=0)
            loop.stop()

        def resume_writing(self) -> None:
            assert not loop.is_running()
            with pytest.raises(RuntimeError, match="Cannot close a running event loop"):
                loop.close()
            flushing.set()
            assert release.wait(5)
            self.transport.write(b"alive")

    local, peer = socket.socketpair()
    peer.settimeout(5)
    transport, _ = loop.run_until_complete(loop.create_connection(FlushProtocol, sock=local))
    thread = threading.Thread(target=loop.run_forever)
    thread.start()
    try:
        peer.sendall(b"flush")
        assert flushing.wait(5)
        with pytest.raises(RuntimeError, match="Cannot close a running event loop"):
            loop.close()
        assert not loop.is_closed()
        assert not transport.is_closing()
        release.set()
        thread.join(5)
        assert not thread.is_alive()
        with peer.makefile("rb") as stream:
            assert stream.read(10) == b"flushalive"
        worker = loop.run_until_complete(loop.run_in_executor(None, threading.current_thread))
        assert worker.name.startswith("retained-executor")
    finally:
        release.set()
        thread.join(5)
        assert not thread.is_alive()
        transport.close()
        peer.close()
        loop.close()
        executor.shutdown()
