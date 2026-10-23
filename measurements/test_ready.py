import socket
import time
import unittest
from unittest.mock import patch

from measurements.ready import wait_for_tcp


class ReadinessTest(unittest.TestCase):
    def test_open_listener(self):
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            server.listen()
            wait_for_tcp(*server.getsockname(), timeout=1)

    def test_bounded_failure(self):
        start = time.monotonic()
        with patch("measurements.ready.socket.create_connection", side_effect=ConnectionRefusedError("closed")):
            with self.assertRaisesRegex(TimeoutError, "not ready"):
                wait_for_tcp("127.0.0.1", 8500, timeout=.02, interval=.005)
        self.assertLess(time.monotonic() - start, 1)

    def test_invalid_timeout(self):
        for value in (0, -1, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                wait_for_tcp("localhost", 8500, timeout=value)

    def test_transient_failure(self):
        with socket.socket() as connection:
            with patch("measurements.ready.socket.create_connection", side_effect=[ConnectionRefusedError(), connection]) as connect:
                wait_for_tcp("localhost", 8500, interval=.001)
            self.assertEqual(connect.call_count, 2)


if __name__ == "__main__":
    unittest.main()
