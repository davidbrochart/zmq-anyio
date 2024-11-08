from __future__ import annotations

import select
import selectors
import threading
import warnings
from collections import deque
from contextlib import AsyncExitStack
from functools import partial
from itertools import chain
from socket import socketpair
from typing import (
    Any,
    Awaitable,
    Callable,
    NamedTuple,
    TypeVar,
    cast,
)

from anyio import Event, TASK_STATUS_IGNORED, create_task_group, from_thread, sleep, to_thread, wait_socket_readable
from anyio.abc import TaskGroup, TaskStatus
from anyioutils import Future, Task, create_task

import zmq
from zmq import EVENTS, POLLIN, POLLOUT
from zmq.utils import jsonapi


class _FutureEvent(NamedTuple):
    future: Future
    kind: str
    kwargs: dict
    msg: Any
    timer: Any


class _AsyncPoller(zmq.Poller):
    """Poller that returns a Future on poll, instead of blocking."""

    _socket_class: type[Socket]
    raw_sockets: list[Any]

    def _watch_raw_socket(self, socket: Any, evt: int, f: Callable) -> None:
        """Schedule callback for a raw socket"""
        raise NotImplementedError()

    def _unwatch_raw_sockets(self, *sockets: Any) -> None:
        """Unschedule callback for a raw socket"""
        raise NotImplementedError()

    async def poll(self, timeout=-1) -> list[tuple[Any, int]]:  # type: ignore
        """Return a poll event"""
        async with create_task_group() as tg:
            future = Future()
            if timeout == 0:
                try:
                    result = super().poll(0)
                except Exception as e:
                    future.set_exception(e)
                else:
                    future.set_result(result)
                return await future.wait()

            # register Future to be called as soon as any event is available on any socket
            watcher = Future()

            # watch raw sockets:
            raw_sockets: list[Any] = []

            def wake_raw(*args):
                if not watcher.done():
                    watcher.set_result(None)

            watcher.add_done_callback(lambda f: self._unwatch_raw_sockets(*raw_sockets))

            wrapped_sockets: list[Socket] = []

            def _clear_wrapper_io(f):
                for s in wrapped_sockets:
                    s._clear_io_state()

            for socket, mask in self.sockets:
                if isinstance(socket, zmq.Socket):
                    if not isinstance(socket, self._socket_class):
                        # it's a blocking zmq.Socket, wrap it in async
                        socket = self._socket_class(socket)
                        wrapped_sockets.append(socket)
                    if mask & zmq.POLLIN:
                        create_task(
                            socket._add_recv_event(tg, "poll", future=watcher), tg
                        )
                    if mask & zmq.POLLOUT:
                        create_task(
                            socket._add_send_event(tg, "poll", future=watcher), tg
                        )
                else:
                    raw_sockets.append(socket)
                    evt = 0
                    if mask & zmq.POLLIN:
                        evt |= selectors.EVENT_READ
                    if mask & zmq.POLLOUT:
                        evt |= selectors.EVENT_WRITE
                    self._watch_raw_socket(socket, evt, wake_raw)

            def on_poll_ready(f):
                if future.done():
                    return
                if watcher.cancelled():
                    try:
                        future.cancel(raise_exception=False)
                    except RuntimeError:
                        # RuntimeError may be called during teardown
                        pass
                    return
                if watcher.exception():
                    future.set_exception(watcher.exception())
                else:
                    try:
                        result = super(_AsyncPoller, self).poll(0)
                    except Exception as e:
                        future.set_exception(e)
                    else:
                        future.set_result(result)

            watcher.add_done_callback(on_poll_ready)

            if wrapped_sockets:
                watcher.add_done_callback(_clear_wrapper_io)

            if timeout is not None and timeout > 0:
                # schedule cancel to fire on poll timeout, if any
                async def trigger_timeout():
                    await sleep(1e-3 * timeout)
                    if not watcher.done():
                        watcher.set_result(None)

                timeout_handle = create_task(trigger_timeout(), tg)

                def cancel_timeout(f):
                    timeout_handle.cancel(raise_exception=False)

                future.add_done_callback(cancel_timeout)

            def cancel_watcher(f):
                if not watcher.done():
                    watcher.cancel(raise_exception=False)

            future.add_done_callback(cancel_watcher)

            return await future.wait()


class _NoTimer:
    @staticmethod
    def cancel(raise_exception=True):
        pass


class Socket(zmq.Socket):
    _recv_futures = None
    _send_futures = None
    _state = 0
    _shadow_sock: zmq.Socket
    _poller_class = _AsyncPoller
    _fd = None
    _exit_stack = None
    _task_group = None
    _select_socket_r = None
    _select_socket_w = None
    _stopped = None
    started = None

    def __init__(
        self,
        context_or_socket: zmq.Context | zmq.Socket,
        socket_type: int = -1,
        **kwargs,
    ) -> None:
        """
        Args:
            context: The context to create the socket with.
            socket_type: The socket type to create.
        """
        if isinstance(context_or_socket, zmq.Socket):
            super().__init__(shadow=context_or_socket.underlying)  # type: ignore
            self._shadow_sock = context_or_socket
            self.context = context_or_socket.context
        else:
            super().__init__(context_or_socket, socket_type, **kwargs)
            self._shadow_sock = zmq.Socket.shadow(self.underlying)

        self._recv_futures = deque()
        self._send_futures = deque()
        self._state = 0
        self._fd = self._shadow_sock.FD
        self._select_socket_r, self._select_socket_w = socketpair()
        self._select_socket_r.setblocking(False)
        self._select_socket_w.setblocking(False)
        self.started = Event()
        self._stopped = threading.Event()

    def close(self, linger: int | None = None) -> None:
        assert self._stopped is not None
        assert self._select_socket_w is not None
        self._stopped.set()
        self._select_socket_w.send(b"a")
        if not self.closed and self._fd is not None:
            event_list: list[_FutureEvent] = list(
                chain(self._recv_futures or [], self._send_futures or [])
            )
            for event in event_list:
                if not event.future.done():
                    try:
                        event.future.cancel(raise_exception=False)
                    except RuntimeError:
                        # RuntimeError may be called during teardown
                        pass
            self._clear_io_state()
        super().close(linger=linger)

    close.__doc__ = zmq.Socket.close.__doc__

    def get(self, key):
        result = super().get(key)
        # if key == EVENTS:
        #     self._schedule_remaining_events(result)
        return result

    get.__doc__ = zmq.Socket.get.__doc__

    async def arecv(
        self,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> bytes | zmq.Frame:
        async with create_task_group() as tg:
            return await self._add_recv_event(
                tg, 'recv', dict(flags=flags, copy=copy, track=track)
            )

    async def arecv_json(
        self,
        flags: int = 0,
        **kwargs,
    ):
        msg = await self.arecv(flags)
        return self._deserialize(msg, lambda buf: jsonapi.loads(buf, **kwargs))

    async def arecv_multipart(
        self,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
    ) -> list[bytes] | list[zmq.Frame]:
        async with create_task_group() as tg:
            return await self._add_recv_event(
                tg, 'recv_multipart', dict(flags=flags, copy=copy, track=track)
            )

    async def asend(
        self,
        data: bytes,
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
        **kwargs: Any,
    ) -> zmq.MessageTracker | None:
        kwargs['flags'] = flags
        kwargs['copy'] = copy
        kwargs['track'] = track
        kwargs.update(dict(flags=flags, copy=copy, track=track))
        async with create_task_group() as tg:
            return await self._add_send_event(tg, 'send', msg=data, kwargs=kwargs)

    async def asend_json(
        self,
        obj: Any,
        flags: int = 0,
        **kwargs,
    ):
        send_kwargs = {}
        for key in ("routing_id", "group"):
            if key in kwargs:
                send_kwargs[key] = kwargs.pop(key)
        msg = jsonapi.dumps(obj, **kwargs)
        return await self.asend(msg, flags=flags, **send_kwargs)

    async def asend_multipart(
        self,
        msg_parts: list[bytes],
        flags: int = 0,
        copy: bool = True,
        track: bool = False,
        **kwargs,
    ) -> zmq.MessageTracker | None:
        kwargs["flags"] = flags
        kwargs["copy"] = copy
        kwargs["track"] = track
        async with create_task_group() as tg:
            return await self._add_send_event(
                tg, "send_multipart", msg=msg_parts, kwargs=kwargs
            )

    def _deserialize(self, recvd, load):
        """Deserialize with Futures"""
        return load(recvd)
        # f = Future()

        # def _chain(_):
        #    """Chain result through serialization to recvd"""
        #    if f.done():
        #        # chained future may be cancelled, which means nobody is going to get this result
        #        # if it's an error, that's no big deal (probably zmq.Again),
        #        # but if it's a successful recv, this is a dropped message!
        #        if not recvd.cancelled() and recvd.exception() is None:
        #            warnings.warn(
        #                # is there a useful stacklevel?
        #                # ideally, it would point to where `f.cancel()` was called
        #                f"Future {f} completed while awaiting {recvd}. A message has been dropped!",
        #                RuntimeWarning,
        #            )
        #        return
        #    if recvd.exception():
        #        f.set_exception(recvd.exception())
        #    else:
        #        buf = recvd.result()
        #        try:
        #            loaded = load(buf)
        #        except Exception as e:
        #            f.set_exception(e)
        #        else:
        #            f.set_result(loaded)

        # recvd.add_done_callback(_chain)

        # def _chain_cancel(_):
        #    """Chain cancellation from f to recvd"""
        #    if recvd.done():
        #        return
        #    if f.cancelled():
        #        recvd.cancel()

        # f.add_done_callback(_chain_cancel)

        # return await f.wait()

    async def poll(self, timeout=None, flags=zmq.POLLIN) -> int:  # type: ignore
        """poll the socket for events

        returns a Future for the poll results.
        """

        if self.closed:
            raise zmq.ZMQError(zmq.ENOTSUP)

        async with create_task_group() as tg:
            p = self._poller_class()
            p.register(self, flags)
            poll_future = cast(Task, create_task(p.poll(timeout), tg))

            future = Future()

            def unwrap_result(f):
                if future.done():
                    return
                if poll_future.cancelled():
                    try:
                        future.cancel()
                    except RuntimeError:
                        # RuntimeError may be called during teardown
                        pass
                    return
                if f.exception():
                    future.set_exception(poll_future.exception())
                else:
                    evts = dict(poll_future.result())
                    future.set_result(evts.get(self, 0))

            if poll_future.done():
                # hook up result if already done
                unwrap_result(poll_future)
            else:
                poll_future.add_done_callback(unwrap_result)

            def cancel_poll(future):
                """Cancel underlying poll if request has been cancelled"""
                if not poll_future.done():
                    try:
                        poll_future.cancel()
                    except RuntimeError:
                        # RuntimeError may be called during teardown
                        pass

            future.add_done_callback(cancel_poll)

            return await future.wait()

    def _add_timeout(self, task_group, future, timeout):
        """Add a timeout for a send or recv Future"""

        def future_timeout():
            if future.done():
                # future already resolved, do nothing
                return

            # raise EAGAIN
            future.set_exception(zmq.Again())

        return self._call_later(task_group, timeout, future_timeout)

    def _call_later(self, task_group, delay, callback):
        """Schedule a function to be called later

        Override for different IOLoop implementations

        Tornado and asyncio happen to both have ioloop.call_later
        with the same signature.
        """

        async def call_later():
            await sleep(delay)
            callback()

        return create_task(call_later(), task_group)

    @staticmethod
    def _remove_finished_future(future, event_list, event=None):
        """Make sure that futures are removed from the event list when they resolve

        Avoids delaying cleanup until the next send/recv event,
        which may never come.
        """
        # "future" instance is shared between sockets, but each socket has its own event list.
        if not event_list:
            return
        # only unconsumed events (e.g. cancelled calls)
        # will be present when this happens
        try:
            event_list.remove(event)
        except ValueError:
            # usually this will have been removed by being consumed
            return

    async def _add_recv_event(self, task_group, kind, kwargs=None, future=None):
        """Add a recv event, returning the corresponding Future"""
        f = future or Future()
        if kind.startswith("recv") and kwargs.get("flags", 0) & zmq.DONTWAIT:
            # short-circuit non-blocking calls
            recv = getattr(self._shadow_sock, kind)
            try:
                r = recv(**kwargs)
            except Exception as e:
                f.set_exception(e)
            else:
                f.set_result(r)
            return await f.wait()

        timer = _NoTimer
        if hasattr(zmq, "RCVTIMEO"):
            timeout_ms = self._shadow_sock.rcvtimeo
            if timeout_ms >= 0:
                timer = self._add_timeout(task_group, f, timeout_ms * 1e-3)

        # we add it to the list of futures before we add the timeout as the
        # timeout will remove the future from recv_futures to avoid leaks
        _future_event = _FutureEvent(f, kind, kwargs, msg=None, timer=timer)
        self._recv_futures.append(_future_event)

        if self._shadow_sock.get(EVENTS) & POLLIN:
            # recv immediately, if we can
            self._handle_recv(task_group)
        if self._recv_futures and _future_event in self._recv_futures:
            # Don't let the Future sit in _recv_events after it's done
            # no need to register this if we've already been handled
            # (i.e. immediately-resolved recv)
            f.add_done_callback(
                partial(
                    self._remove_finished_future,
                    event_list=self._recv_futures,
                    event=_future_event,
                )
            )
            self._add_io_state(task_group, POLLIN)
        return await f.wait()

    async def _add_send_event(
        self, task_group, kind, msg=None, kwargs=None, future=None
    ):
        """Add a send event, returning the corresponding Future"""
        f = future or Future()
        # attempt send with DONTWAIT if no futures are waiting
        # short-circuit for sends that will resolve immediately
        # only call if no send Futures are waiting
        if kind in ('send', 'send_multipart') and not self._send_futures:
            flags = kwargs.get('flags', 0)
            nowait_kwargs = kwargs.copy()
            nowait_kwargs['flags'] = flags | zmq.DONTWAIT

            # short-circuit non-blocking calls
            send = getattr(self._shadow_sock, kind)
            # track if the send resolved or not
            # (EAGAIN if DONTWAIT is not set should proceed with)
            finish_early = True
            try:
                r = send(msg, **nowait_kwargs)
            except zmq.Again as e:
                if flags & zmq.DONTWAIT:
                    f.set_exception(e)
                else:
                    # EAGAIN raised and DONTWAIT not requested,
                    # proceed with async send
                    finish_early = False
            except Exception as e:
                f.set_exception(e)
            else:
                f.set_result(r)

            if finish_early:
                # short-circuit resolved, return finished Future
                # schedule wake for recv if there are any receivers waiting
                if self._recv_futures:
                    self._schedule_remaining_events(task_group)
                return await f.wait()

        timer = _NoTimer
        if hasattr(zmq, 'SNDTIMEO'):
            timeout_ms = self._shadow_sock.get(zmq.SNDTIMEO)
            if timeout_ms >= 0:
                timer = self._add_timeout(task_group, f, timeout_ms * 1e-3)

        # we add it to the list of futures before we add the timeout as the
        # timeout will remove the future from recv_futures to avoid leaks
        _future_event = _FutureEvent(f, kind, kwargs=kwargs, msg=msg, timer=timer)
        self._send_futures.append(_future_event)
        # Don't let the Future sit in _send_futures after it's done
        f.add_done_callback(
            partial(
                self._remove_finished_future,
                event_list=self._send_futures,
                event=_future_event,
            )
        )

        self._add_io_state(task_group, POLLOUT)
        return await f.wait()

    def _handle_recv(self, task_group):
        """Handle recv events"""
        if not self._shadow_sock.get(EVENTS) & POLLIN:
            # event triggered, but state may have been changed between trigger and callback
            return
        f = None
        while self._recv_futures:
            f, kind, kwargs, _, timer = self._recv_futures.popleft()
            # skip any cancelled futures
            if f.done():
                f = None
            else:
                break

        if not self._recv_futures:
            self._drop_io_state(task_group, POLLIN)

        if f is None:
            return

        timer.cancel(raise_exception=False)

        if kind == 'poll':
            # on poll event, just signal ready, nothing else.
            f.set_result(None)
            return
        elif kind == 'recv_multipart':
            recv = self._shadow_sock.recv_multipart
        elif kind == 'recv':
            recv = self._shadow_sock.recv
        else:
            raise ValueError(f"Unhandled recv event type: {kind!r}")

        kwargs['flags'] |= zmq.DONTWAIT
        try:
            result = recv(**kwargs)
        except Exception as e:
            f.set_exception(e)
        else:
            f.set_result(result)

    def _handle_send(self, task_group):
        if not self._shadow_sock.get(EVENTS) & POLLOUT:
            # event triggered, but state may have been changed between trigger and callback
            return
        f = None
        while self._send_futures:
            f, kind, kwargs, msg, timer = self._send_futures.popleft()
            # skip any cancelled futures
            if f.done():
                f = None
            else:
                break

        if not self._send_futures:
            self._drop_io_state(task_group, POLLOUT)

        if f is None:
            return

        timer.cancel()

        if kind == 'poll':
            # on poll event, just signal ready, nothing else.
            f.set_result(None)
            return
        elif kind == 'send_multipart':
            send = self._shadow_sock.send_multipart
        elif kind == 'send':
            send = self._shadow_sock.send
        else:
            raise ValueError(f"Unhandled send event type: {kind!r}")

        kwargs['flags'] |= zmq.DONTWAIT
        try:
            result = send(msg, **kwargs)
        except Exception as e:
            f.set_exception(e)
        else:
            f.set_result(result)

    # event masking from ZMQStream
    async def _handle_events(self):
        """Dispatch IO events to _handle_recv, etc."""
        if self._shadow_sock.closed:
            return

        async with create_task_group() as tg:
            zmq_events = self._shadow_sock.get(EVENTS)
            if zmq_events & zmq.POLLIN:
                self._handle_recv(tg)
            if zmq_events & zmq.POLLOUT:
                self._handle_send(tg)
            self._schedule_remaining_events(tg)

    def _schedule_remaining_events(self, task_group, events=None):
        """Schedule a call to handle_events next loop iteration

        If there are still events to handle.
        """
        # edge-triggered handling
        # allow passing events in, in case this is triggered by retrieving events,
        # so we don't have to retrieve it twice.
        if self._state == 0:
            # not watching for anything, nothing to schedule
            return
        if events is None:
            events = self._shadow_sock.get(EVENTS)
        if events & self._state:
            create_task(self._handle_events(), task_group)

    def _add_io_state(self, task_group, state):
        """Add io_state to poller."""
        if self._state != state:
            state = self._state = self._state | state
        self._update_handler(task_group, self._state)

    def _drop_io_state(self, task_group, state):
        """Stop poller from watching an io_state."""
        if self._state & state:
            self._state = self._state & (~state)
        self._update_handler(task_group, self._state)

    def _update_handler(self, task_group, state):
        """Update IOLoop handler with state.

        zmq FD is always read-only.
        """
        self._schedule_remaining_events(task_group)

    async def __aenter__(self) -> Socket:
        async with AsyncExitStack() as exit_stack:
            self._task_group = await exit_stack.enter_async_context(create_task_group())
            self._exit_stack = exit_stack.pop_all()
            await self._task_group.start(self.start)

        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        try:
            self.close()
        except BaseException:
            pass
        self._task_group.cancel_scope.cancel()
        return await self._exit_stack.__aexit__(exc_type, exc_value, exc_tb)

    async def start(self, *, task_status: TaskStatus[None] = TASK_STATUS_IGNORED) -> None:
        assert self._task_group is not None
        assert self.started is not None
        self._task_group.start_soon(partial(to_thread.run_sync, self._reader, abandon_on_cancel=True))
        await self.started.wait()
        task_status.started()

    def _reader(self):
        from_thread.run_sync(self.started.set)
        while True:
            try:
                rs, ws, xs = select.select([self._shadow_sock, self._select_socket_r.fileno()], [], [self._shadow_sock, self._select_socket_r.fileno()])
            except OSError as e:
                return
            if self._stopped.is_set():
                return
            self._read()

    def _read(self):
        from_thread.run(self._handle_events)

    def _clear_io_state(self):
        """unregister the ioloop event handler

        called once during close
        """
        fd = self._shadow_sock
        if self._shadow_sock.closed:
            fd = self._fd
        #if self._current_loop is not None:
        #    self._current_loop.remove_handler(fd)
