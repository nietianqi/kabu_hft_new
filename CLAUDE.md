# CLAUDE.md — kabu_hft 项目统一契约

所有 AI 助手（Claude Code、ChatGPT Codex 等）在本项目工作时**必须**遵守本文件。
本文件优先级高于任何会话内的临时指令。

---

## 一、字段约定

### kabu Station API 字段反转语义
kabu API 的 bid/ask 字段名与业界惯例**相反**，务必注意：

| kabu API 字段 | 实际含义 |
|--------------|---------|
| `AskPrice` | 买方最优价（bid） |
| `BidPrice` | 卖方最优价（ask） |
| `AskQty` | 买方最优量（bid size） |
| `BidQty` | 卖方最优量（ask size） |
| `AskSign` | 买方气配标志（bid sign） |
| `BidSign` | 卖方气配标志（ask sign） |

归一化逻辑已在 `kabu_hft/gateway/kabu_gateway.py` 的 `KabuAdapter` 中处理。
**不要在其他地方再次反转。**

### 内部 side 约定
```
side = +1  →  多头 / 买方 (long / buy)
side = -1  →  空头 / 卖方 (short / sell)
```

### 时间戳
- 所有时间戳单位：**纳秒（ns）**，使用 `time.time_ns()`
- 日志中的可读时间使用 JST（Asia/Tokyo，UTC+9）

### 持仓字段
```python
@dataclass(slots=True)
class Inventory:
    qty: int          # 当前净持仓手数（API sync 后可被修正）
    side: int         # +1 多头 / -1 空头
    avg_price: float  # 开仓均价（加权）
    entry_qty: int    # 本轮开仓累计成交量（用于均价加权，不用于 P&L）
    exit_qty: int     # 本轮平仓累计成交量（P&L 基准量）
    exit_value: float # 平仓累计成交金额（= sum(price * qty)）
    opened_ts_ns: int # 首次开仓时间戳（ns）
```
**P&L 计算必须用 `exit_qty`，不得用 `entry_qty`。**
原因：`_sync_inventory_from_api()` 可能修正 `qty` 和 `entry_qty` 不一致。

### OrderSnapshot（broker 推送）
```python
@dataclass(slots=True)
class OrderSnapshot:
    order_id: str
    status: str        # "filled" / "cancelled" / "partial" / "working"
    cum_qty: int       # 累计成交量（只增不减；若倒退须记 WARNING）
    avg_fill_price: float
    fill_ts_ns: int
    price: float       # 委托价（fallback）
    qty: int           # 委托量
    is_final: bool
```

### kabu Exchange ID
```
1  = 東証（TSE 普通单）
27 = 東証+（TSE SOR 单，sendorder exchange=1 失败后的回退）
```

---

## 二、禁止改动的文件

未经用户明确授权，**不得修改**以下文件：

```
kabu_hft/adapter/normalizer.py   # 已废弃，等待删除，禁止修改
config.json                       # 生产配置，禁止修改
tests/fixtures/                   # 测试 fixture 数据，禁止修改
```

**每次 session 必须新增对应测试**（若修改了业务逻辑）：

| 修改模块 | 对应测试文件 |
|---------|------------|
| `execution/engine.py` | `tests/test_execution.py` |
| `gateway/kabu_gateway.py` | `tests/test_gateway.py` |
| `signals/microstructure.py` | `tests/test_signals.py` |
| `oms/` | `tests/test_oms.py` |
| `risk/guard.py` | `tests/test_risk.py` |
| `core/strategy.py` | `tests/test_strategy_adaptive.py` |
| `core/market_state.py` | `tests/test_market_state.py` |

---

## 三、改动预算（单次 session 强制限制）

- **Bug 数量**：最多 1–3 个，超出**必须拆分 session**
- **行数上限**：业务代码 ≤ 200 LOC，超出拆分
- **文件范围**：不得改动 Assumptions 之外的模块（禁止"顺手重构"）

---

## 四、每次 session 固定输出模板

```
## Assumptions
<本次改动依赖的假设：字段语义、调用顺序、边界条件>

## Changed files
- kabu_hft/xxx.py (line N–M): <一句话说明改了什么>
- tests/test_xxx.py: <新增/修改了哪个测试>

## Tests run
python -m unittest discover -s tests -p "test_*.py" -v
# 预期：XX passed, 0 failed（已知预存失败除外，见第六节）

## Residual risks
<未覆盖的边界情况或已知局限>
```

---

## 五、给 AI 助手的提示规范

向 Claude Code 或 Codex 提交任务时，prompt 中**必须包含**：

1. **相关 dataclass 全定义**（直接粘贴，不要说"参考 xxx 文件"）
2. **问题的精确行号**（例如：`engine.py:511`）
3. **改动约束**（例如："只改这一处逻辑，不要动其他函数"）
4. **验收标准**（例如："HoldQty=0 时 qty 必须为 0，不得回退到 LeavesQty"）

示例：
```
文件：kabu_hft/gateway/kabu_gateway.py，第 454 行
问题：`raw.get("HoldQty") or raw.get("LeavesQty")` 当 HoldQty=0 时会
      错误回退到 LeavesQty，导致已平仓的仓位被误计为有仓。
相关定义：
  @dataclass(slots=True)
  class PositionLot:
      hold_id: str
      symbol: str
      qty: int   # 0 = 合法值，表示空仓
约束：只修改 position_lot() 方法，不改其他逻辑。
验收：HoldQty=0 时 qty=0，HoldQty=None 时 fallback 到 LeavesQty。
```

---

## 六、门禁测试命令

**合并前必须通过，不得跳过：**

```bash
# 标准门禁（unittest，与 CI 保持一致）
python -m unittest discover -s tests -p "test_*.py" -v

# 等价的 pytest 写法
python -m pytest tests/ -v
```

### 已知预存测试失败（不属于任何人引入，需单独排查）
```
tests/test_signals.py::SignalStackTests::test_positive_order_flow_produces_positive_composite
```
此测试在 commit `030f6f9` 之前就已失败。修复前，门禁命令可用：
```bash
python -m pytest tests/ -v --deselect \
  tests/test_signals.py::SignalStackTests::test_positive_order_flow_produces_positive_composite
```

---

## 七、并行开发规范

- **一人一分支**：命名格式 `fix/<描述>` 或 `claude/<任务名>`
- **worktree 隔离**：`git worktree add .worktrees/<branch> <branch>`，禁止多人同时编辑同一文件
- **合并前**：必须通过门禁测试，且 `git diff main --stat` 核查改动范围
- **禁止**：在同一 session 内同时修改被并行任务占用的文件
