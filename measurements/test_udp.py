import csv
import json
import tempfile
import unittest
from pathlib import Path

from measurements.udp import check_common, owd_summary, parse_rows, parse_timestamp


HEADER = ["seq", "ts_sent", "ts_rcvd", "owd_ms", "size", "lost", "dup_count"]


class UDPTests(unittest.TestCase):
    def parse(self, data, header=HEADER):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "udp.csv"
            with path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(data)
            errors = []
            rows = parse_rows(str(path), errors)
            check_common(str(path), rows, 1, errors)
            return rows, errors

    def row(self, seq=0, sent="2026-01-01T00:00:00.000000001Z",
            received="2026-01-01T00:00:00.010000002Z", owd="10.000001"):
        return [seq, sent, received, owd, 100, "false", 0]

    def test_nanoseconds_and_timezones(self):
        a, ns = parse_timestamp("2026-01-01T00:00:00.000000001Z")
        b, other = parse_timestamp("2026-01-01T01:00:00.000000001+01:00")
        self.assertEqual(a, b)
        self.assertEqual(ns, other)
        self.assertEqual(parse_timestamp("1970-01-01T00:00:00.000000001Z")[1], 1)
        self.assertEqual(parse_timestamp("1969-12-31T23:59:59.999999999Z")[1], -1)
        rows, errors = self.parse([self.row()])
        self.assertFalse(errors)
        self.assertEqual(rows[0].ts_rcvd_ns - rows[0].ts_sent_ns, 10_000_001)

    def test_timezone_errors(self):
        for zone in ("", "+24:00", "+00:60", "+garbage", "+01:00:30"):
            with self.subTest(zone=zone):
                _, errors = self.parse([self.row(sent="2026-01-01T00:00:00" + zone)])
                self.assertTrue(errors)

    def test_invalid_numeric_and_missing_values(self):
        for value in ("NaN", "inf", "-inf", "-1", "9", "", "ten"):
            with self.subTest(value=value):
                _, errors = self.parse([self.row(owd=value)])
                self.assertTrue(errors)
        for index, value in ((0, -1), (4, -1), (5, "False"), (6, -1), (6, 1)):
            row = self.row()
            row[index] = value
            self.assertTrue(self.parse([row])[1])

    def test_sequence_and_ns_order(self):
        self.assertTrue(self.parse([self.row(seq=1)])[1])
        self.assertTrue(self.parse([self.row(), self.row(seq=2)])[1])
        # Equal timestamps are legitimate, but a 1 ns regression must fail.
        self.assertFalse(self.parse([self.row(), self.row(seq=1)])[1])
        later = self.row(sent="2026-01-01T00:00:00.000000002Z", owd="10")
        self.assertTrue(self.parse([later, self.row(seq=1)])[1])
        self.assertFalse(self.parse([self.row(), self.row(seq=1, sent=later[1], owd="10")])[1])

    def test_negative_timestamp_difference(self):
        self.assertTrue(self.parse([self.row(received="2025-12-31T23:59:59Z", owd="0")])[1])

    def test_duplicates_ordered_by_instant(self):
        header = HEADER + ["ts_rcvd_1"]
        row = self.row()
        row[-1] = 1
        row.append("2026-01-01T01:00:00.010000003+01:00")
        self.assertFalse(self.parse([row], header)[1])
        row[-1] = "2026-01-01T00:00:00.010000001Z"
        self.assertTrue(self.parse([row], header)[1])
        row[-1] = ""
        self.assertTrue(self.parse([row], header)[1])

    def test_empty_and_bad_header_and_width(self):
        self.assertTrue(self.parse([])[1])
        self.assertTrue(self.parse([self.row()], HEADER[:-1])[1])
        self.assertTrue(self.parse([self.row()[:-1]])[1])

    def test_lost_and_empty_summaries(self):
        rows, errors = self.parse([[0, "2026-01-01T00:00:00Z", "", "", 100, "true", 0]])
        self.assertFalse(errors)
        result = owd_summary(rows, 10)
        self.assertEqual(result["loss_percent"], 100)
        self.assertEqual(result["received"], 0)
        self.assertTrue(all(v is None for v in result["owd_ms"].values()))
        self.assertTrue(all(v is None for v in result["error_ms"].values()))
        self.assertIsNone(owd_summary([])["loss_percent"])
        json.dumps(result, allow_nan=False)
        bad = self.row()
        bad[5] = "true"
        self.assertTrue(self.parse([bad])[1])

    def test_summary_received(self):
        rows, errors = self.parse([self.row()])
        self.assertFalse(errors)
        result = owd_summary(rows, 10)
        self.assertEqual(result["packets"], 1)
        self.assertEqual(result["lost"], 0)
        self.assertAlmostEqual(result["error_ms"]["median"], .000001)
        self.assertEqual(result["owd_ms"]["p99"], 10.000001)
        json.dumps(result, allow_nan=False)


if __name__ == "__main__":
    unittest.main()
