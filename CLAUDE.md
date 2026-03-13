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
- `inventory.qty`：当前净持仓手数
- `inventory.entry_qty`：本轮开仓累计成交量（用于均价计算）
- `inventory.exit_qty`：本轮平仓累计成交量（用于 P&L 计算）
- P&L 计算使用 `exit_qty`，不用 `entry_qty`

### kabu Exchange ID
- `1` = 東証（TSE 普通单）
- `27` = 東証+（TSE SOR 单，sendorder 失败后的回退）

---

## 二、禁止自动改动的文件

未经用户明确授权，**不得修改**以下内容：

```
kabu_hft/adapter/normalizer.py   # 已废弃，等待删除，不要修
config.json                       # 生产配置，不要改
tests/fixtures/                   # 测试 fixture 数据，不要改
```

---

## 三、改动预算（单次 session 限制）

- **Bug 数量**：单次 session 最多修复 1–3 个 bug
- **行数上限**：单次改动 ≤ 200 LOC（不含测试）
- 超出限制时**拆分任务**，不要合并到一次提交

---

## 四、每次 session 输出模板

```
## Assumptions
<列出本次改动依赖的假设，例如字段语义、调用顺序>

## Changed files
- kabu_hft/xxx.py (line N–M): <一句话说明改了什么>

## Tests run
python -m pytest tests/test_xxx.py::TestClass::test_method -v
# 输出：X passed

## Residual risks
<未处理的边界情况或已知局限>
```

---

## 五、禁止行为

- 不得修改 `config.json` 或任何运行产物
- 不得自动扩散改动到未授权的模块
- 不得只说"已测试"——必须附可复现命令
- 不得跨多个不相关模块做"顺手重构"
- 不得在未读文件的情况下编辑文件

---

## 六、验收命令

```bash
# 全量测试（门禁）
python -m pytest tests/ -v

# 快速回归（排除已知预存失败）
python -m pytest tests/ -v --deselect tests/test_signals.py::SignalStackTests::test_positive_order_flow_produces_positive_composite

# 检查残留日语字符（注释/字符串）
grep -rn --include="*.py" "[ぁ-ん\|ァ-ン\|一-龥]" kabu_hft/
```

### 已知预存测试失败（不属于本次改动引入）
```
tests/test_signals.py::SignalStackTests::test_positive_order_flow_produces_positive_composite
```
此测试在 commit `1a7e35a` 之前就已失败，需单独排查。

---

## 七、并行开发规范

- 每个任务独立分支，命名格式：`fix/<简短描述>` 或 `claude/<任务名>`
- 合并前必须通过门禁测试（见第六节）
- 使用 git worktree 隔离并行任务，避免互相干扰
