from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from contextlib import suppress

from kabu_hft.config import AppConfig, load_config
from kabu_hft.core import HFTStrategy
from kabu_hft.gateway import BoardSnapshot, KabuApiError, KabuRestClient, KabuWebSocket, TradePrint
from kabu_hft.journal import TradeJournal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(name)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger("kabu.app")


class KabuHFTApp:
    def __init__(self, config: AppConfig):
        self.config = config
        self.rest = KabuRestClient(config.base_url, rate_per_sec=config.rate_limit_per_second)
        self.websocket: KabuWebSocket | None = None
        self.strategies: dict[str, HFTStrategy] = {}
        self.journal: TradeJournal | None = None
        self.running = False
        self._stopping = False
        self.status_task: asyncio.Task | None = None

    async def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.journal = TradeJournal(
            trade_path=self.config.journal_path,
            markout_seconds=self.config.markout_seconds,
        )
        self.journal.open()

        await self.rest.start()
        await self.rest.get_token(self.config.api_password)
        logger.info("token acquired")

        if not self.config.dry_run:
            existing_positions = await self._check_existing_positions()
            if existing_positions and self.config.fail_on_startup_positions:
                raise RuntimeError(
                    "startup blocked: existing broker positions detected while fail_on_startup_positions=true"
                )

        for strategy_config in self.config.strategies:
            strategy = HFTStrategy(
                config=strategy_config,
                order_profile=self.config.order_profile,
                rest_client=self.rest,
                dry_run=self.config.dry_run,
                journal=self.journal,
                markout_seconds=self.config.markout_seconds,
            )
            await strategy.start()
            self.strategies[strategy_config.symbol] = strategy

        await self._register_symbols()
        logger.info("registered %d symbols", len(self.strategies))

        self.websocket = KabuWebSocket(
            url=self.config.ws_url,
            on_board=self._on_board,
            on_trade=self._on_trade,
            on_reconnect=self._reregister_symbols,
            api_token=self.rest.token,
        )
        self.running = True
        self.status_task = asyncio.create_task(self._status_loop(), name="status-loop")
        await self.websocket.run()

    async def stop(self) -> None:
        if self._stopping:
            return
        self._stopping = True
        self.running = False

        try:
            if self.websocket is not None:
                self.websocket.stop()
                self.websocket = None

            if self.status_task is not None:
                self.status_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self.status_task
                self.status_task = None

            if not self.config.dry_run:
                await self._emergency_close_all()

            for strategy in self.strategies.values():
                await strategy.stop()
            self.strategies.clear()

            await self.rest.stop()

            if self.journal is not None:
                self.journal.close()
                self.journal = None
        finally:
            self._stopping = False

    def _on_board(self, snapshot: BoardSnapshot) -> None:
        strategy = self.strategies.get(snapshot.symbol)
        if strategy is not None:
            strategy.on_board(snapshot)

    def _on_trade(self, trade: TradePrint) -> None:
        strategy = self.strategies.get(trade.symbol)
        if strategy is not None:
            strategy.on_trade(trade)

    async def _register_symbols(self) -> None:
        symbols = self._build_register_symbols()
        if not symbols:
            logger.warning("no symbols to register")
            return
        await self.rest.register_symbols(symbols)

    async def _reregister_symbols(self) -> None:
        if not self.running:
            return
        try:
            await self._register_symbols()
            logger.info("symbols re-registered after WebSocket reconnect")
        except KabuApiError as exc:
            if exc.status not in {401, 403}:
                logger.warning("re-registration failed after reconnect: %s", exc)
                return
            logger.warning("re-registration unauthorized, refreshing token and retrying")
            await self.rest.get_token(self.config.api_password)
            if self.websocket is not None:
                self.websocket.set_api_token(self.rest.token)
            await self._register_symbols()
            logger.info("token refreshed and symbols re-registered after reconnect")
        except Exception as exc:
            logger.warning("re-registration failed after reconnect: %s", exc)

    def _build_register_symbols(self) -> list[dict[str, int | str]]:
        symbols: list[dict[str, int | str]] = []
        seen: set[tuple[str, int]] = set()
        for strategy in self.strategies.values():
            key = (strategy.config.symbol, strategy.config.exchange)
            if key in seen:
                logger.warning(
                    "duplicate symbol config skipped symbol=%s exchange=%s",
                    strategy.config.symbol,
                    strategy.config.exchange,
                )
                continue
            seen.add(key)
            symbols.append({"Symbol": key[0], "Exchange": key[1]})

        if len(symbols) > 50:
            raise ValueError(
                f"kabu PUSH supports at most 50 registered symbols; got {len(symbols)}"
            )
        return symbols

    async def _check_existing_positions(self) -> list[dict]:
        open_positions: list[dict] = []
        seen_keys: set[str] = set()
        for product in self.config.startup_position_products:
            try:
                raw_positions = await self.rest.get_positions(product=product)
            except Exception as exc:
                logger.warning("startup position check failed for product=%s: %s", product, exc)
                continue

            for position in raw_positions:
                if not isinstance(position, dict):
                    continue
                if self._position_qty(position) <= 0:
                    continue
                key = str(
                    position.get("ExecutionID")
                    or position.get("HoldID")
                    or position.get("ID")
                    or f"{product}:{position.get('Symbol')}:{position.get('Side')}:{self._position_qty(position)}"
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                open_positions.append(position)

        if open_positions:
            logger.warning(
                "EXISTING OPEN POSITIONS DETECTED (%d). fail_on_startup_positions=%s positions=%s",
                len(open_positions),
                self.config.fail_on_startup_positions,
                open_positions,
            )
        return open_positions

    async def _emergency_close_all(self) -> None:
        tasks = [
            asyncio.create_task(strategy.emergency_close())
            for strategy in self.strategies.values()
            if strategy.execution.has_inventory
        ]
        if tasks:
            logger.warning("emergency closing %d open positions on shutdown", len(tasks))
            done, pending = await asyncio.wait(
                tasks, timeout=self.config.shutdown_emergency_timeout_s
            )
            for pending_task in pending:
                pending_task.cancel()
            for done_task in done:
                with suppress(asyncio.CancelledError, Exception):
                    _ = done_task.result()

    async def _status_loop(self) -> None:
        while self.running:
            await asyncio.sleep(self.config.status_interval_s)
            for symbol, strategy in self.strategies.items():
                status = strategy.status()
                logger.info(
                    "status symbol=%s state=%s inventory=%s pnl=%.2f alpha=%.3f",
                    symbol,
                    status["state"],
                    status["execution"]["inventory_qty"],
                    status["risk"]["daily_pnl"],
                    status["signal"]["composite"],
                )

    @staticmethod
    def _position_qty(position: dict) -> int:
        for key in ("LeavesQty", "HoldQty", "Qty"):
            raw_value = position.get(key)
            if raw_value in (None, ""):
                continue
            try:
                return int(float(raw_value))
            except (TypeError, ValueError):
                continue
        return 0


async def run_async(config_path: str) -> None:
    config = load_config(config_path)
    app = KabuHFTApp(config)
    stop_event = asyncio.Event()

    def request_stop(*_: object) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, request_stop)
        except NotImplementedError:
            signal.signal(signum, request_stop)

    runner = asyncio.create_task(app.start(), name="kabu-hft-app")
    waiter = asyncio.create_task(stop_event.wait(), name="stop-waiter")
    try:
        done, _ = await asyncio.wait({runner, waiter}, return_when=asyncio.FIRST_COMPLETED)
        if runner in done and runner.exception() is not None:
            raise runner.exception()
    finally:
        waiter.cancel()
        with suppress(asyncio.CancelledError):
            await waiter
        await app.stop()
        runner.cancel()
        with suppress(asyncio.CancelledError):
            await runner


def main() -> None:
    parser = argparse.ArgumentParser(description="kabu microstructure HFT scaffold")
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()
    asyncio.run(run_async(args.config))
