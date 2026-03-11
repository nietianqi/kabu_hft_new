import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kabu_hft.replay import ReplayConfig, run_replay


class ReplayTests(unittest.TestCase):
    def test_replay_runner_generates_summary(self) -> None:
        jst = timezone(timedelta(hours=9))
        start = datetime(2026, 3, 11, 9, 0, 0, tzinfo=jst)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "events.jsonl"
            cum_volume = 0
            lines: list[str] = []

            for index in range(80):
                ts = start + timedelta(milliseconds=100 * index)
                positive_phase = index < 40
                best_bid = 100.0 + 0.01 * index
                best_ask = best_bid + 1.0
                buy_qty = 1200 if positive_phase else 200
                sell_qty = 200 if positive_phase else 1200
                last_price = best_ask if positive_phase else best_bid
                cum_volume += 100
                payload = {
                    "Symbol": "9984",
                    "Exchange": 1,
                    "AskPrice": best_bid,  # kabu reversed field
                    "AskQty": buy_qty,
                    "BidPrice": best_ask,  # kabu reversed field
                    "BidQty": sell_qty,
                    "Buy1": {"Price": best_bid, "Qty": buy_qty},
                    "Sell1": {"Price": best_ask, "Qty": sell_qty},
                    "CurrentPrice": last_price,
                    "CurrentPriceTime": ts.isoformat(),
                    "TradingVolume": cum_volume,
                    "VWAP": best_bid + 0.5,
                }
                lines.append(json.dumps(payload, ensure_ascii=True))

            path.write_text("\n".join(lines), encoding="utf-8")

            summary = run_replay(
                str(path),
                ReplayConfig(
                    symbol="9984",
                    tick_size=1.0,
                    qty=100,
                    entry_threshold=0.05,
                    exit_threshold=0.02,
                    max_hold_events=12,
                    strong_threshold=0.2,
                ),
            )
            self.assertEqual(summary.symbol, "9984")
            self.assertGreater(summary.num_events, 0)
            self.assertGreater(summary.num_signals, 0)
            self.assertGreaterEqual(summary.num_entries, 1)
            self.assertGreaterEqual(summary.num_round_trips, 1)


if __name__ == "__main__":
    unittest.main()
