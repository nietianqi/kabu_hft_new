import unittest
from types import SimpleNamespace

from kabu_hft.app import KabuHFTApp
from kabu_hft.config import load_config


def _fake_strategy(symbol: str, exchange: int):
    return SimpleNamespace(config=SimpleNamespace(symbol=symbol, exchange=exchange))


class AppRegistrationTests(unittest.TestCase):
    def test_register_payload_deduplicates_same_symbol_exchange(self) -> None:
        app = KabuHFTApp(load_config(None))
        app.strategies = {
            ("9984", 1): _fake_strategy("9984", 1),
            ("9984", 3): _fake_strategy("9984", 3),
            ("4568", 1): _fake_strategy("4568", 1),
        }
        payload = app._build_register_symbols()
        self.assertEqual(
            payload,
            [
                {"Symbol": "9984", "Exchange": 1},
                {"Symbol": "9984", "Exchange": 3},
                {"Symbol": "4568", "Exchange": 1},
            ],
        )

    def test_register_payload_rejects_more_than_50_symbols(self) -> None:
        app = KabuHFTApp(load_config(None))
        app.strategies = {
            (f"{7000 + index}", 1): _fake_strategy(f"{7000 + index}", 1)
            for index in range(51)
        }
        with self.assertRaises(ValueError):
            app._build_register_symbols()

    def test_register_payload_normalizes_tse_plus_and_sor_to_tse(self) -> None:
        app = KabuHFTApp(load_config(None))
        app.strategies = {
            ("7269", 27): _fake_strategy("7269", 27),
            ("7269", 1): _fake_strategy("7269", 1),
            ("9616", 9): _fake_strategy("9616", 9),
        }
        payload = app._build_register_symbols()
        self.assertEqual(
            payload,
            [
                {"Symbol": "7269", "Exchange": 1},
                {"Symbol": "9616", "Exchange": 1},
            ],
        )

    def test_find_strategy_uses_exact_symbol_exchange(self) -> None:
        app = KabuHFTApp(load_config(None))
        exact = _fake_strategy("9984", 3)
        app.strategies = {
            ("9984", 1): _fake_strategy("9984", 1),
            ("9984", 3): exact,
        }
        self.assertIs(app._find_strategy("9984", 3), exact)

    def test_find_strategy_fallback_only_when_symbol_unique(self) -> None:
        app = KabuHFTApp(load_config(None))
        only = _fake_strategy("7269", 1)
        app.strategies = {
            ("7269", 1): only,
        }
        self.assertIs(app._find_strategy("7269", 27), only)
        app.strategies = {
            ("9984", 1): _fake_strategy("9984", 1),
            ("9984", 3): _fake_strategy("9984", 3),
        }
        self.assertIsNone(app._find_strategy("9984", 27))

    def test_summarize_positions_groups_by_symbol_exchange_side(self) -> None:
        positions = [
            {"Symbol": "4568", "Exchange": 27, "Side": "2", "LeavesQty": 100},
            {"Symbol": "4568", "Exchange": 27, "Side": "2", "LeavesQty": 200},
            {"Symbol": "9984", "Exchange": 1, "Side": "1", "LeavesQty": 300},
        ]
        summary = KabuHFTApp._summarize_positions(positions)
        self.assertEqual(
            summary,
            "4568@27 side=2 count=2 qty=300; 9984@1 side=1 count=1 qty=300",
        )


if __name__ == "__main__":
    unittest.main()
