import tempfile
import unittest
from pathlib import Path

from kabu_hft.telemetry.latency_report import analyze_log, attach_trade_stale_exit_rate


class LatencyReportTests(unittest.TestCase):
    def test_analyze_log_extracts_disconnects_latency_and_stale(self) -> None:
        log_text = "\n".join(
            [
                "14:00:00.000 [kabu.gateway] INFO websocket connected: ws://localhost:18080/kabusapi/websocket",
                "14:00:01.000 [kabu.gateway] WARNING market data latency 1200.0ms for 7269 (source=bid_time bid=1200.0ms ask=1300.0ms current=5000.0ms)",
                "14:00:02.000 [kabu.gateway] INFO latency stats symbol=7269 samples=100 p50=300.0ms p90=1200.0ms p99=3200.0ms max=8000.0ms",
                "14:00:03.000 [kabu.execution] INFO entry order sent symbol=7269 side=+1 qty=100 price=2035.000 market=False mode=QUEUE_DEFENSE dry_run=True",
                "14:00:04.000 [kabu.execution] INFO cancel requested symbol=7269 order_id=PAPER-7269-1 reason=abnormal_stale_quote",
                "14:00:05.000 [kabu.gateway] WARNING websocket disconnected: sent 1011 (internal error) keepalive ping timeout; no close frame received",
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "run.log"
            log_path.write_text(log_text, encoding="utf-8")
            run = analyze_log(log_path, label="local")

        self.assertEqual(run.connects, 1)
        self.assertEqual(run.disconnects, 1)
        self.assertEqual(run.total_entries(), 1)
        self.assertEqual(run.total_cancels(), 1)
        self.assertEqual(run.total_stale_cancels(), 1)
        self.assertGreater(run.duration_seconds(), 0.0)

        symbol = run.symbols["7269"]
        est = symbol.est_latency()
        self.assertEqual(est["method"], "weighted_window")
        self.assertEqual(est["samples"], 100)
        self.assertEqual(est["p50_ms"], 300.0)
        self.assertEqual(est["p99_ms"], 3200.0)

    def test_attach_trade_stale_exit_rate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "run.log"
            log_path.write_text(
                "14:00:00.000 [kabu.gateway] INFO websocket connected: ws://localhost:18080/kabusapi/websocket\n",
                encoding="utf-8",
            )
            trades_path = Path(tmp) / "trades.csv"
            trades_path.write_text(
                "\n".join(
                    [
                        "ts_jst,symbol,side,qty,entry_price,exit_price,realized_pnl,hold_ms,exit_reason",
                        "2026-03-12T11:00:00+09:00,7269,1,100,2000,2001,100,1000,signal_reverse",
                        "2026-03-12T11:01:00+09:00,7269,1,100,2002,2001,-100,1000,abnormal_stale_quote",
                    ]
                ),
                encoding="utf-8",
            )
            run = analyze_log(log_path, label="local")
            attach_trade_stale_exit_rate(run, trades_path)

        self.assertEqual(run.total_trade_rows, 2)
        self.assertEqual(run.stale_trade_exits, 1)
        self.assertEqual(run.stale_trade_exit_rate, 50.0)


if __name__ == "__main__":
    unittest.main()

