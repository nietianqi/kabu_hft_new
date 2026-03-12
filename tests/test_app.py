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
            "s1": _fake_strategy("9984", 1),
            "s2": _fake_strategy("9984", 1),
            "s3": _fake_strategy("4568", 1),
        }
        payload = app._build_register_symbols()
        self.assertEqual(
            payload,
            [
                {"Symbol": "9984", "Exchange": 1},
                {"Symbol": "4568", "Exchange": 1},
            ],
        )

    def test_register_payload_rejects_more_than_50_symbols(self) -> None:
        app = KabuHFTApp(load_config(None))
        app.strategies = {
            f"s{index}": _fake_strategy(f"{7000 + index}", 1)
            for index in range(51)
        }
        with self.assertRaises(ValueError):
            app._build_register_symbols()


if __name__ == "__main__":
    unittest.main()
