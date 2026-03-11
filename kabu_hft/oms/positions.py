from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class PositionState:
    symbol: str
    side: int = 0
    qty: int = 0
    avg_price: float = 0.0
    realized_pnl: float = 0.0


class PositionLedger:
    def __init__(self) -> None:
        self._positions: dict[str, PositionState] = {}

    def _get(self, symbol: str) -> PositionState:
        if symbol not in self._positions:
            self._positions[symbol] = PositionState(symbol=symbol)
        return self._positions[symbol]

    def apply_fill(self, symbol: str, side: int, qty: int, price: float) -> PositionState:
        position = self._get(symbol)
        if qty <= 0:
            return position

        if position.qty == 0:
            position.side = side
            position.qty = qty
            position.avg_price = price
            return position

        if position.side == side:
            total_qty = position.qty + qty
            total_value = position.qty * position.avg_price + qty * price
            position.qty = total_qty
            position.avg_price = total_value / max(total_qty, 1)
            return position

        close_qty = min(position.qty, qty)
        position.realized_pnl += position.side * (price - position.avg_price) * close_qty
        remaining_open_qty = position.qty - close_qty
        residual_qty = qty - close_qty

        if remaining_open_qty > 0:
            position.qty = remaining_open_qty
            return position

        if residual_qty > 0:
            position.side = side
            position.qty = residual_qty
            position.avg_price = price
            return position

        position.side = 0
        position.qty = 0
        position.avg_price = 0.0
        return position

    def get(self, symbol: str) -> PositionState:
        return self._get(symbol)

    def snapshot(self) -> dict[str, dict]:
        return {
            symbol: {
                "side": state.side,
                "qty": state.qty,
                "avg_price": state.avg_price,
                "realized_pnl": state.realized_pnl,
            }
            for symbol, state in self._positions.items()
        }
