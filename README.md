# 候选快照归并器（merger.py）

对两个地理编码供应商的候选结果做**版本化快照归并**：每个原始地址（稳定 `source_row`）
保存各供应商在指定版本下的候选集合、可解释的选择结果与未决冲突；人工选择只作为
随规则版本声明的“保留（hold）”，永不固化为永久真值。

## 核心不变量

1. **候选身份** = `(provider, response_version, rank)`；地址文本相同绝不合并 `source_row`。
2. **归并请求** = `(batch_id, provider_versions, rule_version)`。指纹（批次内容 + 供应商
   版本 + 规则版本的 SHA-256）相同 → 幂等返回旧快照；任一版本变化 → 新快照。
3. **历史快照只读**：SQLite 触发器禁止对快照/选择/响应/规则/隔离记录 UPDATE、DELETE；
   同指纹伪造快照被部分唯一索引拒绝。
4. **失败快照不替换可读版本**：失败仅留痕（`status='failed'`），所有读取只认 `status='ok'`；
   快照 id 持久自增（`AUTOINCREMENT`），进程重启后不复用身份。
5. **人工保留**随规则版本声明，只针对明确 `source_row` + 候选身份；供应商新版本到达后
   保留身份缺席 → 不静默沿用，按规则重算并标记 `hold_superseded` 进入待人工清单；
   保留身份仍在 → 标记 `inherited`（跨快照继承）。

## 隔离（quarantine）

| 类型 | 阶段 | 范围 | 后果 |
|---|---|---|---|
| `incomplete_response` | ingest / merge | 响应级 | 整个响应不可用 |
| `duplicate_rank` | ingest | 响应级 | 整个响应不可用 |
| `invalid_coordinate` | ingest | 候选级 | 仅该候选被剔除 |
| `rule_conflict` | merge | 源行级 | 选择未决（pending），待人工 |

## 归并规则链（每个源行）

1. 人工保留身份存在于本次请求版本 → `decision=hold`
2. 仅一家可用 → `single_provider`（附 `provider_unavailable` 未决原因）
3. 多家 top 候选两两距离 ≤ `distance_threshold_m` → `providers_agree`，选优先供应商
4. 距离冲突但置信度差 ≥ `confidence_diff_threshold` → `confidence_override`
5. 否则 → `rule_conflict`：隔离 + 未决，不产生选择

每条选择都记录：使用的候选身份、比较字段（距离/置信度差/阈值）、被淘汰候选及原因、
未决原因、相对上一可读快照的变化标记（`added/unchanged/inherited/changed`）与原因。

## 接口

Python API（`merger.py`）与同名 CLI 子命令：

| 接口 | 函数 / CLI |
|---|---|
| 接收 | `ingest_source_rows` / `ingest_response`（`add-rows` / `ingest`） |
| 校验 | `validate_response_payload` / `validate_response`（`validate`） |
| 归并 | `create_snapshot`（`snapshot`） |
| 比较 | `compare_snapshots`（`compare`） |
| 查询源行 | `get_source_row`（`get`） |
| 导出待人工清单 | `export_pending`（`export`，含来源与规则证据，非仅经纬度） |

```bash
python3 merger.py init geo.db
python3 merger.py add-rows geo.db B1 rows.json
python3 merger.py add-rule geo.db rule.json
python3 merger.py ingest geo.db resp.json
python3 merger.py snapshot geo.db B1 r1 provA=v1,provB=v1
python3 merger.py compare geo.db 1 2
python3 merger.py get geo.db S6 --snapshot 1
python3 merger.py export geo.db --snapshot 2
```

## 测试

```bash
python3 scenario_test.py
```

手工构造：接近/冲突坐标、版本升级只改第二候选、缺响应、重复 rank、无效坐标、
人工保留（适用/失效/重新确认/继承）、同文本不同 source_row；用独立期望表逐源行
核对选择、冲突与变化原因（437 项），并交错接收与查询验证旧快照可读、新快照不可
冒充旧结果、失败快照不替换可读版本、重启后快照身份不重复。
