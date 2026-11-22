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

---

# 派单名册（dispatch.py）

在归并器所在的**同一个 SQLite 数据库**中，把固定快照的每个地址（`source_row`）
派给负责的配送区。首次建册后，运营只交本次改动的区，调度端即可读到改区后
**完整、同一版**的逐地址归属；局部改区只重算受影响地址，不顺序扫描整批地址、
不重建全表。merger 的原有表与只读触发器一律不改动、不写入。

## 数据约定

- **地址全集固定**：建册时固定一个 `status='ok'` 的成功快照（默认最新可读快照，
  也可用 `--snapshot` 指定），以该快照 `selections` 的全部 `source_row` 为全集；
  只使用其中已选经纬度 `sel_lon/sel_lat`，不回查供应商，也不跟随之后的新快照。
- **三种记录状态**：`assigned`（有坐标且匹配到区，带 `zone_id`）、`uncovered`
  （有坐标但无区覆盖）、`no_coordinate`（无选定坐标）。有待决原因但已有选定坐标
  的行照常分区；相同坐标的不同源行仍是互相独立的派单记录。
- **坐标与几何尺度**：多边形顶点是整数**微度**（microdegree，1e-6 度），顺序为
  `[经度, 纬度]`；地址点用已存 REAL 坐标的 Python 十进制字符串值经 `Decimal`
  精确换算到微度尺度，不四舍五入到格点、不用容差吞掉贴边点。在普通经纬度平面内
  做直线判定，不支持跨日期变更线的边界。
- **环规则**：环首尾闭合、不得自交或退化；孔须严格处于外环内部，孔之间不能接触、
  相交或嵌套。外环边线算覆盖；孔内部与孔边线均不算覆盖；环绕向不影响结果。
- **重叠裁决**：优先级（整数）数值最大者胜；同级按区 ID 字符串升序最前者胜。
- **版本**：建册产生修订号 1；每个成功批次只增加一个修订号。读取名册带快照 ID、
  名册修订号及按 `source_row` 排序的完整记录。

## 区表 / 批次 JSON 格式

完整区表（建册）：

```json
{
  "Z1": {
    "priority": 5,
    "geometry": {
      "outer": [[lon, lat], [lon, lat], ..., [lon, lat]],
      "holes": [[[lon, lat], ..., [lon, lat]]]
    }
  }
}
```

改区批次（`holes` 可省略；区值为 `null` 表示删除，否则是新增/替换的完整区）：

```json
{
  "expected_revision": 3,
  "changes": {
    "Z1": { "priority": 7, "geometry": {"outer": [[...]]} },
    "Z9": null
  }
}
```

## 局部维护与一致性

- 改区重算范围 = 每个改动区各自**旧外环 bbox ∪ 新外环 bbox** 的并集（逐区独立
  闭合外包矩形，不合并成横跨两处的大矩形）；范围内按批次最终区表整体重新判定，
  未编辑的区也可能接手或仍然获胜；范围外记录不重算、不覆写（`last_revision`
  留痕可核）。定位走 `roster_addresses(x_floor, y_floor)` 索引区间查询。
- 区表、定位状态、派单记录、修订号在同一事务（WAL + `BEGIN IMMEDIATE`）一起
  生效；读连接只能看到完整旧版或完整新版；同一预期修订号的并发写至多一个成功
  （其余以过期号拒绝）；写进程提交前退出，事务随连接关闭回滚，可从上一版继续。
- 空批次、删除不存在的区、过期修订号、同批重复区、非法几何均**整批拒绝**，
  当前名册不变。

## 接口

| 操作 | CLI | Python API |
|---|---|---|
| 首次建册（完整区表 → 修订号 1） | `dispatch.py build DB zones.json [--snapshot N]` | `build_roster` |
| 应用改区批次 | `dispatch.py apply DB batch.json` | `apply_changes` |
| 读取当前版完整名册 | `dispatch.py read DB [--out f]` | `read_roster` |
| 只校验几何不写库 | `dispatch.py validate-geometry f.json` | `validate_polygon` / `normalize_zones` |

```bash
python3 dispatch.py build geo.db zones.json
python3 dispatch.py apply geo.db batch.json
python3 dispatch.py read geo.db
```

数据库表：`dz_meta`（固定快照 ID、当前修订号）、`dz_zones`（区表 + 外环 bbox）、
`roster_addresses`（地址全集、精确微度坐标与 floor 定位列）、`roster_records`
（逐地址归属与 `last_revision`）、`roster_revisions`（修订台账）。

## 测试

```bash
python3 dispatch_roster_test.py
```

人工判定表覆盖贴边与孔、同级重叠、优先级裁决、重复坐标、缺坐标、有待决原因但
有坐标照常分区；区从街区甲移到远处乙后甲地址由未编辑区接手、乙原未覆盖地址获派，
随后改优先级与删区。甲乙之间 40 个不受影响地址通过 SQLite 语句追踪、
`EXPLAIN QUERY PLAN` 与 `last_revision` 留痕核实无全表扫描、无合并大矩形、
无区外重算。另验证旧修订写入、同批有效+无效项整批回滚、未提交更新时另开连接
读旧版并在写进程结束后重开续提、同预期号并发写至多一个成功（164 项）。
并发/崩溃的独立 OS 进程辅助脚本为 `dispatch_worker.py`（以 `python3 -I` 运行）。
