import pytest
import zmq
from anyio import create_task_group, move_on_after, sleep, to_thread
from zmq_anyio import Socket

pytestmark = pytest.mark.anyio


async def test_arecv_multipart(context, create_bound_pair):
    a, b = create_bound_pair(zmq.PUSH, zmq.PULL)
    a, b = Socket(a), Socket(b)
    async with b, a, create_task_group() as tg:

        async def recv(messages):
            for message in messages:
                assert await b.arecv_multipart() == [message]

        tg.start_soon(recv, [b"Hello", b", World!"])
        await a.asend(b"Hello")
        await a.asend(b", World!")


async def test_arecv(context, create_bound_pair):
    a, b = create_bound_pair(zmq.PUSH, zmq.PULL)
    a, b = Socket(a), Socket(b)
    async with b, a, create_task_group() as tg:

        async def recv(messages):
            for message in messages:
                assert await b.arecv() == message

        tg.start_soon(recv, [b"Hello", b", World!"])
        await a.asend(b"Hello")
        await a.asend(b", World!")


async def test_arecv_json(context, create_bound_pair):
    a, b = create_bound_pair(zmq.PUSH, zmq.PULL)
    a, b = Socket(a), Socket(b)
    async with b, a, create_task_group() as tg:

        async def recv(messages):
            for message in messages:
                assert await b.arecv_json() == message

        tg.start_soon(recv, [{"Hello": ", World!"}])
        await a.asend_json({"Hello": ", World!"})


async def test_arecv_send(context, create_bound_pair):
    a, b = create_bound_pair(zmq.REQ, zmq.REP)
    a, b = Socket(a), Socket(b)
    async with a, b, create_task_group() as tg:

        async def recv(messages):
            for message in messages:
                assert await b.arecv() == message
                b.send(b", World!")

        tg.start_soon(recv, [b"Hello"])
        a.send(b"Hello")
        assert await a.arecv() == b", World!"


async def test_inproc(sockets):
    ctx = zmq.Context()
    url = "inproc://test"
    a = ctx.socket(zmq.PUSH)
    b = ctx.socket(zmq.PULL)
    a.linger = 0
    b.linger = 0
    sockets.extend([a, b])
    a.connect(url)
    b.bind(url)
    b = Socket(b)
    async with b, create_task_group() as tg:

        async def recv():
            assert await b.arecv() == b"hi"

        tg.start_soon(recv)
        await sleep(0.1)
        a.send(b"hi")


@pytest.mark.parametrize("total_threads", [1, 2])
async def test_start_socket(total_threads, create_bound_pair):
    to_thread.current_default_thread_limiter().total_tokens = total_threads

    a, b = map(Socket, create_bound_pair(zmq.REQ, zmq.REP))
    a_started = False
    b_started = False

    with pytest.raises(BaseException):
        async with b:
            b_started = True
            with move_on_after(0.1):
                async with a:
                    a_started = True
                    raise RuntimeError

    assert b_started
    if total_threads == 1:
        assert not a_started
    else:
        assert a_started
    
    to_thread.current_default_thread_limiter().total_tokens = 40
