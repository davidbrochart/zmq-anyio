[![Build Status](https://github.com/davidbrochart/zmq-anyio/actions/workflows/test.yml/badge.svg?query=branch%3Amain++)](https://github.com/davidbrochart/zmq-anyio/actions/workflows/test.yml/badge.svg?query=branch%3Amain++)

# zmq-anyio

Asynchronous API for ZMQ using AnyIO.

## Usage

`zmq_anyio.Socket` is a subclass of `zmq.Socket`. Here is how it must be used:
- Create a blocking ZMQ socket `sock` using a `zmq.Context`.
- Create an async `zmq_anyio.Socket(sock)`, passing the `sock`.
- Use the `zmq_anyio.Socket` with an async context manager.
- Use `arecv()` for the async API, `recv()` for the blocking API, etc.

```py
import anyio
import zmq
import zmq_anyio

ctx = zmq.Context()
sock1 = ctx.socket(zmq.PAIR)
port = sock1.bind("tcp://127.0.0.1:1234")
sock2 = ctx.socket(zmq.PAIR)
sock2.connect("tcp://127.0.0.1:1234")

# wrap the `zmq.Socket` with `zmq_anyio.Socket`:
sock1 = zmq_anyio.Socket(sock1)
sock2 = zmq_anyio.Socket(sock2)

async def main():
    async with sock1, sock2:  # use an async context manager
        await sock1.asend(b"Hello")  # use `asend` instead of `send`
        assert await sock2.arecv() == b"Hello"  # use `arecv` instead of `recv`

anyio.run(main)
```
