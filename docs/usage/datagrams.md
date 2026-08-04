# Datagrams

```python
transport, protocol = await loop.create_datagram_endpoint(
    Protocol, local_addr=("127.0.0.1", 9999)
)
```

`create_datagram_endpoint` is implemented natively over `uv_udp_t`, and takes the
full asyncio signature: `local_addr`, `remote_addr`, `family`, `proto`, `flags`,
`reuse_port`, `allow_broadcast` and `sock`.

The returned transport is an `asyncio.DatagramTransport` and hands out a real
`TransportSocket` through `get_extra_info("socket")`.

## Sending

```python
transport.sendto(b"payload", ("127.0.0.1", 9999))  # unconnected
transport.sendto(b"payload")                       # connected, via remote_addr
```

An endpoint created with `remote_addr` is connected: `sendto()` takes no address,
and passing one raises `ValueError`. An endpoint without it requires an address
on every send.

A datagram the kernel accepts outright goes out through `uv_udp_try_send` without
allocating a request.

## Errors

A failed send does **not** raise. asyncio's contract is that the endpoint stays
usable, so the error reaches your protocol:

```python
class Protocol(asyncio.DatagramProtocol):
    def error_received(self, exc: Exception) -> None: ...
```

A datagram too large for the receive buffer is reported the same way rather than
delivered truncated. A silent prefix is worse than a reported loss.

## Unix datagrams

```python
transport, protocol = await loop.create_datagram_endpoint(
    Protocol, local_addr="/tmp/server.sock", family=socket.AF_UNIX
)
```
