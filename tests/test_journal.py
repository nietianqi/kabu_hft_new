import csv
import tempfile
import time
import unittest
from pathlib import Path

from kabu_hft.execution import RoundTrip
from kabu_hft.journal import TradeJournal
from kabu_hft.signals import SignalPacket


def _make_round_trip(**kwargs) -> RoundTrip:
    defaults = dict(
        symbol="9984",
        side=1,
        qty=100,
        entry_price=9980.0,
        exit_price=10030.0,
        entry_ts_ns=int(1_700_000_000 * 1e9),
        exit_ts_ns=int(1_700_000_000 * 1e9) + 15_000_000_000,
        realized_pnl=5000.0,
        exit_reason="signal_reverse",
    )
    defaults.update(kwargs)
    return RoundTrip(**defaults)


def _make_signal(**kwargs) -> SignalPacket:
    defaults = dict(
        ts_ns=0,
        obi_raw=0.1,
        lob_ofi_raw=0.2,
        tape_ofi_raw=0.05,
        micro_momentum_raw=0.3,
        microprice_tilt_raw=0.15,
        microprice=9990.0,
        mid=9990.0,
        obi_z=1.2,
        lob_ofi_z=0.8,
        tape_ofi_z=0.5,
        micro_momentum_z=0.7,
        microprice_tilt_z=0.3,
        composite=0.75,
    )
    defaults.update(kwargs)
    return SignalPacket(**defaults)


class TestTradeJournal(unittest.TestCase):
    def test_log_trade_writes_csv_row(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "trades.csv"
            journal = TradeJournal(path, markout_seconds=0)
            journal.open()

            trade = _make_round_trip()
            signal = _make_signal()
            journal.log_trade(trade, signal)
            journal.close()

            rows = list(csv.DictReader(path.open()))
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["symbol"], "9984")
            self.assertEqual(row["side"], "1")
            self.assertEqual(row["qty"], "100")
            self.assertAlmostEqual(float(row["realized_pnl"]), 5000.0, places=1)
            self.assertEqual(row["exit_reason"], "signal_reverse")
            self.assertAlmostEqual(float(row["composite"]), 0.75, places=3)

    def test_log_trade_without_signal(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "trades.csv"
            journal = TradeJournal(path, markout_seconds=0)
            journal.open()
            journal.log_trade(_make_round_trip(), None)
            journal.close()

            rows = list(csv.DictReader(path.open()))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["composite"], "")

    def test_appends_to_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "trades.csv"

            journal = TradeJournal(path, markout_seconds=0)
            journal.open()
            journal.log_trade(_make_round_trip(exit_price=10010.0, realized_pnl=3000.0), None)
            journal.close()

            journal2 = TradeJournal(path, markout_seconds=0)
            journal2.open()
            journal2.log_trade(_make_round_trip(exit_price=10020.0, realized_pnl=4000.0), None)
            journal2.close()

            rows = list(csv.DictReader(path.open()))
            self.assertEqual(len(rows), 2)
            self.assertAlmostEqual(float(rows[0]["realized_pnl"]), 3000.0, places=1)
            self.assertAlmostEqual(float(rows[1]["realized_pnl"]), 4000.0, places=1)

    def test_hold_ms_calculated_correctly(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "trades.csv"
            journal = TradeJournal(path, markout_seconds=0)
            journal.open()

            entry_ns = 1_000_000_000_000_000_000
            exit_ns = entry_ns + 5_000_000_000  # 5 seconds
            trade = _make_round_trip(entry_ts_ns=entry_ns, exit_ts_ns=exit_ns)
            journal.log_trade(trade, None)
            journal.close()

            rows = list(csv.DictReader(path.open()))
            hold_ms = float(rows[0]["hold_ms"])
            self.assertAlmostEqual(hold_ms, 5000.0, places=0)

    def test_markout_csv_created(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "trades.csv"
            markout_path = Path(tmpdir) / "trades.markout.csv"
            journal = TradeJournal(path, markout_seconds=30)
            journal.open()
            journal.close()

            self.assertTrue(markout_path.exists())

    def test_log_trade_before_open_does_not_raise(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "trades.csv"
            journal = TradeJournal(path, markout_seconds=0)
            # Not calling open() — should silently do nothing
            journal.log_trade(_make_round_trip(), None)


if __name__ == "__main__":
    unittest.main()
