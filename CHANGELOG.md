# Version history

## 0.2.3

- Check if socket is started when calling async methods.

## 0.2.2

- Allow starting a socket multiple times.

## 0.2.1

- Update README.

## 0.2.0

- Use root task group instead of creating new ones.
- Rename `Poller.poll` to `Poller.apoll`.
- Add `arecv_string`, `arecv_pyobj`, `arecv_serialized`, and equivalent send methods.
- Add more tests and fixes.

## 0.1.3

- Use `anyio.wait_socket_readable(sock)` with a ThreadSelectorEventLoop on Windows with ProactorEventLoop.

## 0.1.2

- Block socket startup if no thread is available.

## 0.1.1

- Add `CHANGELOG.md`.
- Automatically create a GitHub release after publishing to PyPI.

## 0.1.0
