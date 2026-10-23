#!/usr/bin/env python3
"""Bounded TCP readiness probe; independent of workload/backend."""
import argparse
import math
import socket
import time


def wait_for_tcp(host, port, timeout=15, interval=0.1):
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    if not math.isfinite(interval) or interval <= 0:
        raise ValueError("interval must be finite and positive")
    deadline = time.monotonic() + timeout
    last_error = None
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            with socket.create_connection((host, port), timeout=min(1, remaining)):
                return
        except OSError as error:
            last_error = error
        time.sleep(min(interval, max(0, deadline - time.monotonic())))
    raise TimeoutError(f"{host}:{port} not ready after {timeout}s: {last_error}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("--timeout", type=float, default=15)
    args = parser.parse_args()
    try:
        wait_for_tcp(args.host, args.port, args.timeout)
    except (OSError, ValueError) as error:
        parser.exit(1, f"readiness failed: {error}\n")


if __name__ == "__main__":
    main()
