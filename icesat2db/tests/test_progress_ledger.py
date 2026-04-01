# SPDX-License-Identifier: EUPL-1.2
# Contact: felixd@gfz.de, besnard@gfz.de, urbazaev@gfz.de and amelia.holcomb@gmail.com.
# SPDX-FileCopyrightText: 2026 Felix Dombrowski
# SPDX-FileCopyrightText: 2026 Mikhail Urbazaev
# SPDX-FileCopyrightText: 2026 Simon Besnard
# SPDX-FileCopyrightText: 2026 Amelia Holcomb
# SPDX-FileCopyrightText: 2026 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import csv
import os
import tempfile
import time
import unittest

from icesat2db.utils.progress_ledger import ProgressLedger, Row


def _make_row(granule_id="G001", status="ok", n_records=100, offset=0.0):
    now = time.time() + offset
    return Row(
        granule_id=granule_id,
        timeframe="2020-W01",
        submitted_ts=now - 2,
        started_ts=now - 1,
        finished_ts=now,
        duration_s=1.0,
        status=status,
        n_records=n_records,
        bytes_downloaded=1024,
        products="ATL08",
        error_msg=None,
    )


class TestProgressLedgerInit(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_creates_out_dir(self):
        ledger = ProgressLedger(os.path.join(self.tmp, "ledger_out"), "2020-W01")
        self.assertTrue(os.path.isdir(ledger.out_dir))

    def test_creates_errors_subdir(self):
        ledger = ProgressLedger(os.path.join(self.tmp, "ledger_out2"), "2020-W01")
        self.assertTrue(os.path.isdir(os.path.join(ledger.out_dir, "errors")))

    def test_creates_csv_with_header(self):
        ledger = ProgressLedger(os.path.join(self.tmp, "ledger_out3"), "2020-W01")
        self.assertTrue(os.path.exists(ledger.csv_path))
        with open(ledger.csv_path) as f:
            reader = csv.reader(f)
            header = next(reader)
        self.assertIn("granule_id", header)
        self.assertIn("status", header)
        self.assertIn("duration_s", header)

    def test_does_not_overwrite_existing_csv(self):
        out_dir = os.path.join(self.tmp, "ledger_out4")
        ledger = ProgressLedger(out_dir, "2020-W01")
        ledger.append(_make_row("G001"))
        # Re-open same dir — should NOT truncate existing CSV
        ProgressLedger(out_dir, "2020-W01")
        with open(ledger.csv_path) as f:
            rows = list(csv.reader(f))
        # header + 1 data row
        self.assertEqual(len(rows), 2)


class TestProgressLedgerAppend(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ledger = ProgressLedger(os.path.join(self.tmp, "out"), "2020-W01")

    def test_append_ok_increments_ok_count(self):
        self.ledger.append(_make_row("G001", status="ok"))
        self.assertEqual(self.ledger.ok_count, 1)
        self.assertEqual(self.ledger.fail_count, 0)

    def test_append_fail_increments_fail_count(self):
        self.ledger.append(_make_row("G002", status="fail"))
        self.assertEqual(self.ledger.fail_count, 1)
        self.assertEqual(self.ledger.ok_count, 0)

    def test_append_writes_to_csv(self):
        self.ledger.append(_make_row("G003", status="ok", n_records=42))
        with open(self.ledger.csv_path) as f:
            rows = list(csv.reader(f))
        # header + 1 data row
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1][0], "G003")
        self.assertEqual(rows[1][6], "ok")

    def test_append_none_n_records_writes_empty_string(self):
        row = _make_row("G004", n_records=None)
        self.ledger.append(row)
        with open(self.ledger.csv_path) as f:
            rows = list(csv.reader(f))
        self.assertEqual(rows[1][7], "")

    def test_append_with_error_msg(self):
        row = _make_row("G005", status="fail")
        row.error_msg = "something broke"
        self.ledger.append(row)
        with open(self.ledger.csv_path) as f:
            rows = list(csv.reader(f))
        self.assertEqual(rows[1][10], "something broke")

    def test_multiple_appends_accumulate(self):
        for i in range(5):
            self.ledger.append(_make_row(f"G{i:03d}"))
        self.assertEqual(self.ledger.ok_count, 5)


class TestProgressLedgerNoteSubmit(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ledger = ProgressLedger(os.path.join(self.tmp, "out"), "2020-W01")

    def test_note_submit_tracked(self):
        self.ledger.note_submit("G001")
        self.assertIn("G001", self.ledger._submits)

    def test_total_uses_submits_when_larger(self):
        for i in range(10):
            self.ledger.note_submit(f"G{i:03d}")
        self.ledger.append(_make_row("G000"))
        self.assertEqual(self.ledger.total, 10)

    def test_total_uses_row_count_when_larger(self):
        self.ledger.note_submit("G000")
        for i in range(5):
            self.ledger.append(_make_row(f"G{i:03d}"))
        self.assertEqual(self.ledger.total, 5)


class TestProgressLedgerEta(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ledger = ProgressLedger(os.path.join(self.tmp, "out"), "2020-W01")

    def test_eta_no_rows_returns_none(self):
        self.assertIsNone(self.ledger.eta_minutes())

    def test_eta_with_rows_returns_float(self):
        # Submit 10, process 5
        for i in range(10):
            self.ledger.note_submit(f"G{i:03d}")
        for i in range(5):
            self.ledger.append(_make_row(f"G{i:03d}", offset=float(i)))
        eta = self.ledger.eta_minutes()
        self.assertIsNotNone(eta)
        self.assertGreaterEqual(eta, 0.0)


class TestProgressLedgerWriteMd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ledger = ProgressLedger(os.path.join(self.tmp, "out"), "2020-W01")

    def test_write_status_md_creates_file(self):
        self.ledger.append(_make_row("G001"))
        self.ledger.write_status_md()
        self.assertTrue(os.path.exists(self.ledger.md_path))

    def test_write_status_md_contains_timeframe(self):
        self.ledger.append(_make_row("G001"))
        self.ledger.write_status_md()
        with open(self.ledger.md_path) as f:
            content = f.read()
        self.assertIn("2020-W01", content)

    def test_write_status_md_contains_counts(self):
        self.ledger.append(_make_row("G001", status="ok"))
        self.ledger.append(_make_row("G002", status="fail"))
        self.ledger.write_status_md()
        with open(self.ledger.md_path) as f:
            content = f.read()
        self.assertIn("Success", content)
        self.assertIn("Failed", content)

    def test_write_status_md_empty_ledger(self):
        # Should not raise even with no rows
        self.ledger.write_status_md()
        self.assertTrue(os.path.exists(self.ledger.md_path))


class TestProgressLedgerWriteHtml(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ledger = ProgressLedger(os.path.join(self.tmp, "out"), "2020-W01")

    def test_write_html_creates_file(self):
        self.ledger.append(_make_row("G001"))
        self.ledger.write_html()
        self.assertTrue(os.path.exists(self.ledger.html_path))

    def test_write_html_contains_granule_id(self):
        self.ledger.append(_make_row("GRANULE_XYZ"))
        self.ledger.write_html()
        with open(self.ledger.html_path) as f:
            content = f.read()
        self.assertIn("GRANULE_XYZ", content)

    def test_write_html_escapes_special_chars(self):
        row = _make_row("<script>")
        self.ledger.append(row)
        self.ledger.write_html()
        with open(self.ledger.html_path) as f:
            content = f.read()
        self.assertNotIn("<script>", content)

    def test_write_html_zero_total_no_division_error(self):
        # No submits, no rows — pct = 0, should not raise ZeroDivisionError
        self.ledger.write_html()
        self.assertTrue(os.path.exists(self.ledger.html_path))


class TestProgressLedgerWriteError(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.ledger = ProgressLedger(os.path.join(self.tmp, "out"), "2020-W01")

    def test_write_error_creates_log_file(self):
        self.ledger.write_error("G001", "Traceback (most recent call last):\n  ...")
        log_path = os.path.join(self.ledger.out_dir, "errors", "G001.log")
        self.assertTrue(os.path.exists(log_path))

    def test_write_error_content(self):
        tb = "Some traceback text"
        self.ledger.write_error("G002", tb)
        log_path = os.path.join(self.ledger.out_dir, "errors", "G002.log")
        with open(log_path) as f:
            self.assertEqual(f.read(), tb)


if __name__ == "__main__":
    unittest.main()
