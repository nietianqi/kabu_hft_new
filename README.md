# kabu_hft

面向 `kabu station` 的自研微结构高频骨架，不依赖 vn.py。

## 核心约束

- kabu PUSH 的 `Bid/Ask` 含义和国际通行相反，内部必须先标准化为 `best_bid / best_ask`。
- PUSH 最多注册 50 个标的，且午休与收盘后不推送，代码内置了 stale quote 防护。
- kabu PUSH 没有完整逐笔成交流，`Tape-OFI` 只能基于 `TradingVolume` 增量与 quote rule 做近似。
- 同标的双边同时挂单可能遇到 cross-trade / 仮装売買 风险，因此默认只保留单边工作单。

参考文档：

- [PUSH 文档](https://kabucom.github.io/kabusapi/ptal/push.html)
- [REST 参考](https://kabucom.github.io/kabusapi/reference/index.html)
- [错误码说明](https://kabucom.github.io/kabusapi/ptal/error.html)

## 当前实现

- `kabu_hft/gateway`: token / register / websocket / sendorder / cancelorder / orders / positions 适配。
- `kabu_hft/signals`: 加权盘口不平衡、LOB-OFI、Tape-OFI、micro-momentum、microprice tilt。
- `kabu_hft/execution`: 单向被动执行器、价格优势判断、撤改单节流、订单状态机、纸面撮合。
- `kabu_hft/risk`: stale quote、最大持仓、最大名义金额、日内亏损、冷却期、持仓超时、session 过滤。
- `kabu_hft/core`: 研究和实盘共用的策略主循环，对账与超时检查都在同一条链路里。

## 运行

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python main.py --config config.json
```

默认 `dry_run=true`，不会向 kabu station 发送真实订单。

## 配置说明

- `order_profile.mode`
  - `cash`: 现金买入/卖出，默认不允许裸卖空。
  - `margin_daytrade`: 信用新规 / 返済模式，支持做空，但需要 kabu 账户参数与可用持仓匹配。
- `symbols[*]`
  - `tick_size`: 标的最小价位。
  - `base_qty`: 基础下单股数。
  - `max_qty`: 单笔最大股数。
  - `max_inventory_qty`: 最大库存股数。
  - `max_notional`: 单标的最大名义敞口。

## 测试

```bash
python -m unittest discover -s tests -v
```
