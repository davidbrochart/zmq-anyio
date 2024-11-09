# Version history

## 0.1.3

- Use `anyio.wait_socket_readable(sock)` with a ThreadSelectorEventLoop on Windows with ProactorEventLoop.

## 0.1.2

- Block socket startup if no thread is available.

## 0.1.1

- Add `CHANGELOG.md`.
- Automatically create a GitHub release after publishing to PyPI.

## 0.1.0
