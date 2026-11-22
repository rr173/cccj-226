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

---

# 配送区覆盖审计（coverage_audit.py，只读）

派单名册能查某个地址归哪个区，但判断不了**地图上没有地址样本的地方是否漏划**，
也看不出**优先级掩盖了哪块重叠**。覆盖审计命令补上这一面：运营指定一个矩形
检查窗口，命令报告窗口内实际存在的每一片漏划或重叠区域的精确面积与定位点。
审计不取地址点作样本、不改库、不追溯旧版。

## 用法

```bash
python3 dispatch.py audit-coverage DB --window MIN_LON MIN_LAT MAX_LON MAX_LAT [--out F]
```

- 窗口四至是**整数微度**（与区表同尺度），要求严格正宽高
  （`MIN_LON < MAX_LON`、`MIN_LAT < MAX_LAT`）且在经纬度范围内
  （经度 ±180000000、纬度 ±90000000）；`min < max` 的矩形天然不跨日期变更线。
- 没有已建名册或窗口无效时**非零退出**，标准输出不出现貌似成功的报告
  （错误信息走标准错误）。
- 成功时退出码 0，输出一份 JSON 报告（`--out` 写文件，默认标准输出）。

## 报告字段

```json
{
  "window": {"min_lon": 0, "min_lat": 0, "max_lon": 9000000, "max_lat": 8000000},
  "revision": 4,
  "snapshot_id": 1,
  "zone_count": 13,
  "region_count": 2,
  "regions": [
    {"zones": [], "kind": "gap", "area": "25/4", "point": ["25/4", "7/3"]},
    {"zones": ["A", "B"], "kind": "overlap", "area": "27/4", "point": ["9/4", "5/1"]}
  ]
}
```

| 字段 | 含义 |
|---|---|
| `window` | 回显检查窗口四至（整数微度） |
| `revision` | 所读区表的**名册修订号** |
| `snapshot_id` | 名册固定的快照 ID（上下文） |
| `zone_count` | 所读区表的区数 |
| `regions[]` | 异常区域列表，顺序稳定 |
| `regions[].zones` | 覆盖该区域的完整区 ID 列表，按字符串升序；漏划为空 |
| `regions[].kind` | `gap`（漏划，签名为空）/ `overlap`（重叠，签名 ≥2 个区） |
| `regions[].area` | 区域精确面积（平方微度），约分后的 `分子/分母` 字符串，分母为正 |
| `regions[].point` | 严格位于该区域内部、具有该签名的定位点 `[经度, 纬度]`，同样的分数串 |

## 语义与一致性

- **覆盖签名** = 在经纬度平面中实际覆盖该处的所有区 ID，**忽略派单优先级**。
  只报签名为空的漏划和含两个或更多 ID 的重叠；单区覆盖不报告。
- **区域** = 窗口内签名相同且沿正长度公共边相连的极大面积片：穿过人为切分
  的边不多报，仅在一点相碰不合并；不同签名各报各的；边界线与孤立点本身
  不计面积、不单独报异常。**先按窗口截取**：同一片区域若只能从窗口外绕行
  相通，在窗口内仍分开计。
- 几何直接取自数据库里**已提交**的配送区（含孔洞）；几何与修订号在同一个
  只读事务（一致性快照）中读取——即使审计期间有人提交改区，报告也不会把
  旧修订号和新区表配在一起。CLI 以 `mode=ro` 只读连接打开数据库。
- 全程精确有理数运算（`fractions.Fraction`）：斜边在非整数坐标相交也算准，
  不用栅格估算，不把交点舍入到整数微度。同一窗口与同一版区表重复审计，
  定位点与列表顺序逐字节稳定。
- 算法：区边裁剪到窗口 → 切分/去重为原子线段 → 竖直梯形分解 → 沿正长度
  公共边合并出签名恒定的面 → 面积求和、规范梯形中点作定位点。线段两两求
  交为 O(边数²)，适合审计窗口量级。

## 测试

```bash
python3 coverage_audit_test.py
```

全部期望面积与区域个数从几何本身手工推导（推导写在测试注释里），不拿程序
输出当预期：斜边交于非整数坐标 (9/2, 7/2) 时重叠面积恰为 27/4、孔被另一区
精确填满与贴合边不制造伪缺口、两片只有一个角相碰的漏划不被错误合并、窗口
外才相通的漏划在窗内分开计；定位点用手写严格不等式验证落在应有区域内部。
另验证改区提交与读取交错（未提交写、读事务中插入提交）时报告始终配对同一
已提交版本的修订号与几何，以及无名册/非法窗口时 CLI 非零退出（151 项）。


---

# 站点同步（sync_center.py / sync_site.py）

后场断网时站点仍要用本机 SQLite 查单。中心端把派单名册**按站点切成增量**：
站点先取走自己负责的地址（初装包），之后只接收自己与上一版的**净变化**（补丁），
在一次事务里把地址表与版本游标推进到补丁指明的名册版本。

- 站点由**派单结果里的区 ID** 确定（区 → 站点映射），与多边形相交/覆盖签名无关。
  一个地址属于哪个站，只看它 `roster_records.zone_id` 落在哪个区的站点。
- 中心端与站点端各一个 CLI；不做网络传输（包就是 JSON 文件）。

## 中心端：启用、初装、补丁

```bash
# 1) 建册时顺手给出区 -> 站点静态映射（也可建成后由 enable --site-zones 整理）
python3 dispatch.py build geo.db zones.json --site-zones site_zones.json

# 2) 在已有名册上启用同步（幂等；重复启用返回原身份与起点）
python3 sync_center.py enable geo.db
python3 sync_center.py sites geo.db                 # 列出发布源 ID 与已知站点

# 3) 初装包：站点在目标修订当前已提交的全部派单地址
python3 sync_center.py export-initial geo.db SITE_A --target 1 --out a_init.json

# 4) 补丁：站点 + 起始修订 + 目标修订（两端都须是启用以来已提交的版本）
python3 sync_center.py export-patch geo.db SITE_A --from 1 --to 4 --out a_1_4.json
```

启用（`enable_sync`）：

| 行为 | 说明 |
|---|---|
| 最早同步版本 | 记下启用当时的名册修订号为 `enabled_revision`；更早的版本**不编造历史**、不可导出 |
| 发布源 ID | 存库的随机 `SRC-…`，进程重启不变；独立启用的另一个中心库分配到的值不同 |
| 重复启用 | 幂等返回原 `source_id` / `enabled_revision`；映射已锁定，若再次传入不同映射则拒绝 |
| 一次性整理 | 只整理区 → 站点映射（`dz_zone_sites` → 发布映射 `sync_zone_site`），**不改名册记录与修订号** |

启用后继续用原来的 `dispatch.py build / apply` 改区。改区成功提交时，
站点净变化与派单记录、修订号在**同一事务**里生效：

- 只比较本批重算过的地址的旧/新站点，不为写同步记录而遍历整册；
- 未实际换站的地址不写事件；同一批内转了又回（净零）不写事件；
- 被拒或未提交的改区不产生任何可导出的变化；
- 删区时先写净变化、再移除该区的发布映射，所以**原站一定收到删除**，
  即使它最后一个区已被删除（站点登记在 `sync_sites`，仍可导出）。

覆盖审计（`audit-coverage`）与地理编码快照（merger 各表）的行为不变。

## 同步包格式（JSON，字段固定）

初装包：

```json
{
  "protocol": "dispatch-site-sync/1",
  "package_type": "initial",
  "source_id": "SRC-3F9C...",
  "site_id": "SITE_A",
  "target_revision": 1,
  "items": [
    {"source_row": "R_A_1", "lon": 100.5, "lat": 40.5}
  ]
}
```

补丁：

```json
{
  "protocol": "dispatch-site-sync/1",
  "package_type": "patch",
  "source_id": "SRC-3F9C...",
  "site_id": "SITE_A",
  "from_revision": 1,
  "target_revision": 4,
  "items": [
    {"op": "add", "source_row": "R_NEW", "lon": 100.3, "lat": 40.3, "zone_id": "M"},
    {"op": "del", "source_row": "R_OLD"}
  ]
}
```

| 字段 | 含义 |
|---|---|
| `protocol` | 固定 `dispatch-site-sync/1`，站点端不认别的值 |
| `package_type` | `initial` 或 `patch` |
| `source_id` | 中心库的发布源 ID（站点初装后固定，不得改变） |
| `site_id` | 本包的目标站点（站点初装后固定） |
| `from_revision` | 仅补丁：补丁起点，必须等于站点当前修订号 |
| `target_revision` | 应用后站点应到达的名册修订号 |
| `items[].op` | 仅补丁：`add` 或 `del`；初装包省略（全部视为新增） |
| `items[].source_row` | 地址稳定 ID，全包严格升序 |
| `items[].lon` / `lat` | 名册中的选定经纬度（度数，REAL）；初装包只给这两个字段 |
| `items[].zone_id` | 仅补丁的 `add` 项：中心端当前归属，仅供核对，不作收件依据 |

补丁只交付**两个版本之间的净差**：

- 起点不在本站、终点在 → 完整地址行（`add`）；
- 起点在、终点不在 → 删除项（`del`）；
- 两端都在或都不在（含中途转走又调回）→ 不发；
- 无变化也给可应用、能推进版本的空补丁（`items: []`）。

导出实现按 `(site_id, source_row, sync_revision)` / `(site_id, sync_revision)`
索引把区间内**本站**事件折叠成起点/终点状态：不扫完整名册、不遍历其他站点，
也不随空修订或无关修订的个数增长。地址项一律按 `source_row` 排序，同一请求对
同一版本重复导出逐字节一致；三个版本之后仍可按原目标版本重导同一补丁。

## 站点端：应用与查看

```bash
python3 sync_site.py init site_a.db                      # 建空站点库
python3 sync_site.py apply site_a.db a_init.json         # 初装（固定发布源与站点）
python3 sync_site.py apply site_a.db a_1_4.json          # 应用补丁
python3 sync_site.py apply site_a.db a_1_4.json --dry-run # 只校验不写库

# 查看本站当前修订号与当前地址（含经纬度），
# 或 --out 把地址表单独写文件（剩余摘要仍打印到标准输出）
python3 sync_site.py show site_a.db
python3 sync_site.py show site_a.db --out addresses.json
```

站点库的表：

| 表 | 内容 |
|---|---|
| `ss_meta` | `source_id`、`site_id`（初装后固定）、`revision`（当前名册版本）、`last_digest`（最近成功应用包的内容摘要） |
| `ss_addresses` | 本站当前派单地址：`source_row`、`lon`、`lat` |
| `ss_applied` | 应用历史：包类型、起止版本、条数、内容摘要、原包 JSON |

直接用 SQLite 查阅：

```bash
sqlite3 site_a.db "SELECT value FROM ss_meta WHERE key='revision';"
sqlite3 site_a.db "SELECT source_row, lon, lat FROM ss_addresses ORDER BY source_row;"
```

应用规则（全部在任何写操作之前校验，失败不改库）：

1. **身份**：`source_id` / `site_id` 必须与本站固定身份一致；
2. **版本**：补丁 `from_revision` 必须等于本机当前修订号；缺版、跳版、倒序一律拒绝；
3. **包类型**：初装包只能应用到空库；已初装后再次初装被拒（不会重置本站）；
4. **项自洽**：地址项必须严格升序；`add` 的地址不得已存在，`del` 的地址必须存在；
   任一冲突整包拒绝，**不留下部分结果**；
5. **原子性**：地址表与版本游标、应用历史在同一个事务提交；
6. **幂等重放**：原样重送**最近一次成功应用**的包 → 成功返回 `replayed`、无任何副作用；
   版本区间相同但内容不同的另一份包 → 拒绝（不能借重送偷换内容）。

## 表结构（中心库新增）

`sync_meta`（发布源 ID、启用版本、singleton）、`sync_zone_site`（启用后随改区演进的
发布映射，可删区）、`sync_zone_sites`（启用时的一次性整理快照）、
`sync_sites`（已知站点登记）、`sync_changes`（按 `(sync_revision, site_id, source_row)`
记录的站点净变化，`op` 为 `add`/`del`）。merger 各表与只读触发器不变。

## 示例与测试

```bash
python3 sync_example.py
```

可复跑示例（临时目录建库）构造一条修订链：地址 `R_MOVE_1` 由甲站转到乙站、
又转回甲站、再转回乙站后不再回来；另一个地址 `R_DEL` 因区被删除而离开原站。
手写 rev1/2/3/4/5/6/7/13 各站地址集，逐版本核对；核对旧目标版本的补丁、
跨版净差、空补丁、删除项、转了又回不发、重复导出一致；核对站点应用结果与
错序、缺版、身份不符、重复项、冲突项整包拒绝后本地版本与地址逐字节不变；
另让一次改区停在提交前，在独立连接上确认目标版本不可导出；对一个只涉及少量
本站变化的长区间，用 `EXPLAIN QUERY PLAN`（`SEARCH … USING INDEX
idx_sync_changes_site_rev`）与 SQL 追踪（折叠只发一条 `sync_changes` 区间查询、
不扫 `roster_addresses`/`roster_records`）说明导出不随其他站点记录或空修订增长；
并覆盖另一中心库累积 25 个无关修订后同一区间仍为空补丁、可逐个应用推进版本。
CLI 冒烟覆盖 `enable` / `export-initial` / `export-patch` / `apply` / `show`
以及倒序导出的非零退出。
