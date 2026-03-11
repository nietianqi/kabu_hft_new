from __future__ import annotations

import argparse
import asyncio
import glob
import json
import sys
from contextlib import suppress


def _run_live(args: argparse.Namespace) -> None:
    from kabu_hft.app import run_async
    asyncio.run(run_async(args.config))


def _run_replay(args: argparse.Namespace) -> None:
    from kabu_hft.config import load_config
    from kabu_hft.replay.loader import ReplayLoader
    from kabu_hft.replay.runner import ReplayRunner

    config = load_config(args.config)

    # Find strategy config for the requested symbol
    symbol = args.replay_symbol
    strategy_cfg = None
    if symbol:
        for cfg in config.strategies:
            if cfg.symbol == symbol:
                strategy_cfg = cfg
                break
        if strategy_cfg is None:
            print(f"ERROR: symbol {symbol!r} not found in config {args.config}", file=sys.stderr)
            sys.exit(1)
    elif config.strategies:
        strategy_cfg = config.strategies[0]
        print(f"No --replay-symbol given; using first symbol: {strategy_cfg.symbol}", file=sys.stderr)
    else:
        print("ERROR: no strategies in config", file=sys.stderr)
        sys.exit(1)

    # Resolve path glob
    paths = sorted(glob.glob(args.replay)) if "*" in args.replay or "?" in args.replay else [args.replay]
    if not paths:
        print(f"ERROR: no files matching {args.replay!r}", file=sys.stderr)
        sys.exit(1)

    print(f"Replaying {len(paths)} file(s) for symbol={strategy_cfg.symbol}", flush=True)

    loader = ReplayLoader(paths)
    runner = ReplayRunner(
        config=strategy_cfg,
        latency_us=config.replay_latency_us,
    )
    result = runner.run(loader)
    summary = result.metrics.summary()

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(
        f"\nBoards={result.board_count}  Trades={len(result.trades)}"
        f"  Fills={result.fill_count}  TradePrints={result.trade_print_count}",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="kabu microstructure HFT scaffold")
    parser.add_argument("--config", default="config.json", help="Path to JSON config file")

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--replay",
        metavar="PATH",
        help="JSONL board recording to replay (supports glob patterns, e.g. data/board_*.jsonl)",
    )
    parser.add_argument(
        "--replay-symbol",
        metavar="SYMBOL",
        help="Symbol code to replay (required when config has multiple symbols)",
    )

    args = parser.parse_args()

    if args.replay:
        _run_replay(args)
    else:
        _run_live(args)


if __name__ == "__main__":
    main()
