#!/usr/bin/env python3
"""派单名册场景测试（可复跑）：人工判定表 + 局部改区可观测验证 + 并发/崩溃。

运行：``python3 dispatch_roster_test.py``（子进程辅助以 ``python3 -I`` 运行）。

覆盖需求
--------
1. 贴边与孔（外边线覆盖、孔边/孔内不覆盖）、同级重叠按 ID 升序、
   优先级高者胜、重复坐标独立派单、缺坐标、有待决原因但有坐标照常分区。
2. 区 M 从街区甲移到远处乙：甲的地址由未编辑的区接手，乙原先未覆盖地址获派单；
   随后改优先级、删区。
3. 甲乙之间 40 个不受影响地址：通过 SQLite 语句追踪 + EXPLAIN QUERY PLAN +
   last_revision 留痕，核实改区没有全表扫描、没有合并大矩形、没有区外重算。
4. 旧修订号写入被拒；同批有效项与无效项混在一起时整批拒绝、名册不变。
5. 更新未提交时另开连接只能读到旧版；写进程结束（未提交）后重开仍是旧版，
   再正常提交同一批成功。
6. 同一预期号的并发写至多一个成功。
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import dispatch
import merger

FAILURES = []
N_CHECKS = [0]


def check(name, cond, detail=""):
    N_CHECKS[0] += 1
    if not cond:
        FAILURES.append(name)
        print(f"[FAIL] {name} -- {detail}")


def section(title):
    print(f"\n=== {title} ===")


# ---------------------------------------------------------------- 几何小工具

def ring(minx, miny, maxx, maxy):
    return [[minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny]]


def zone(priority, outer, holes=None):
    return {"priority": priority, "geometry": {"outer": outer, "holes": holes or []}}


def md(deg):
    """度 -> 该尺度下的整数微度（测试坐标均为 1e-6 的整数倍，换算精确）。"""
    return int(round(deg * 1_000_000))


# ---------------------------------------------------------------- 测试数据

# 街区甲（lon 100..101, lat 40..41）的区。各区几何互不覆盖对方的判定点，
# 除明确设计的 ZA/ZB 同级重叠与 M/ZT、M/S 优先级重叠外。
ZONES_REV1 = {
    # 带孔的区（偏甲西南）：外环边线覆盖，孔内/孔边不覆盖
    "ZH": zone(8, ring(md(100.00), md(40.00), md(100.10), md(40.10)),
               holes=[ring(md(100.04), md(40.04), md(100.06), md(40.06))]),
    # 同级重叠对（甲中部）：重叠区取字符串升序最前的 ID
    "ZA": zone(5, ring(md(100.20), md(40.20), md(100.32), md(40.32))),
    "ZB": zone(5, ring(md(100.28), md(40.28), md(100.40), md(40.40))),
    # 与 M 在甲东北重叠：M 优先级 10 > ZT 优先级 6
    "ZT": zone(6, ring(md(100.70), md(40.70), md(100.90), md(40.90))),
    "M":  zone(10, ring(md(100.70), md(40.70), md(100.95), md(40.95))),
    # 不搬家的区：M 移走后接手甲处地址
    "S":  zone(4, ring(md(100.60), md(40.60), md(101.00), md(41.00))),
}

# 人工判定点（坐标写成度，存储后精确换成微度；点都放在整数微度上）
POINTS = {
    # name: (lon 度, lat 度, rev1 期望 (status, zone_id))
    "T_OUTER_EDGE":  (100.050000, 40.000000, ("assigned", "ZH")),   # 外环边线 -> 覆盖
    "T_HOLE_EDGE":   (100.050000, 40.040000, ("uncovered", None)),  # 孔边线 -> 不算覆盖
    "T_HOLE_INSIDE": (100.050000, 40.050000, ("uncovered", None)),  # 孔内部 -> 不覆盖
    "T_IN_RING_OUTSIDE_HOLE": (100.050000, 40.020000, ("assigned", "ZH")),  # 环内孔外 -> 覆盖
    "T_ZA_ONLY":     (100.220000, 40.220000, ("assigned", "ZA")),
    "T_TIE":         (100.300000, 40.300000, ("assigned", "ZA")),   # ZA/ZB 同级 -> ID 升序
    "T_ZB_ONLY":     (100.380000, 40.380000, ("assigned", "ZB")),
    "T_M_WINS":      (100.800000, 40.800000, ("assigned", "M")),    # M(10) 压过 ZT(6)/S(4)
    "T_ZT_ONLY":     (100.720000, 40.720000, ("assigned", "M")),    # 仍在 M 内
    "T_S_ONLY":      (100.980000, 40.980000, ("assigned", "S")),    # 只 S 覆盖
    "T_UNCOVERED":   (100.500000, 40.500000, ("uncovered", None)),  # 不在任何区
    "T_DUP_A":       (100.850000, 40.850000, ("assigned", "M")),    # 与 T_DUP_B 同坐标
    "T_DUP_B":       (100.850000, 40.850000, ("assigned", "M")),    # 不同 source_row 独立记录
    # 待决原因 + 有选定坐标 -> 照常分区（点落在 ZA 独占处）
    "T_PENDING_WITH_COORD": (100.240000, 40.240000, ("assigned", "ZA")),
}
# 乙街区（lon 120..130, lat 10..20）rev1 时无区覆盖
POINTS["T_EAST_1"] = (125.000000, 15.000000, ("uncovered", None))
POINTS["T_EAST_2"] = (126.000000, 16.000000, ("uncovered", None))
# 缺坐标行（不含在 POINTS，单独构造）
NO_COORD_ROWS = ["T_NO_COORD_1", "T_NO_COORD_2_PENDING"]

# 甲乙之间的一批不受影响地址（lon 20..60, lat 45 一带，与任何区 bbox 都不相交）
MIDDLE_ROWS = {f"T_MID_{i:02d}": (20.0 + i, 45.0001 + (i % 7) * 0.0001)
               for i in range(40)}


def cand(addr, lat, lon, conf=0.9):
    return {"rank": 1, "normalized_address": addr, "lat": lat, "lon": lon,
            "confidence": conf}


def build_snapshot(db):
    """用 merger 构造一个含 ok 快照的现有数据库（快照 selections 即地址全集）。"""
    conn = merger.init_db(db)
    rows = []
    for sr in list(POINTS) + list(MIDDLE_ROWS):
        rows.append((sr, f"addr-{sr}"))
    for sr in NO_COORD_ROWS:
        rows.append((sr, f"addr-{sr}"))
    merger.ingest_source_rows(conn, "B1", rows)
    merger.register_rule(conn, "r1", preferred_provider="provA",
                         distance_threshold_m=50.0, confidence_diff_threshold=0.2)
    # 有坐标的行：单供应商自动选择
    coord_points = {sr: (lon, lat) for sr, (lon, lat, _e) in POINTS.items()}
    coord_points.update(MIDDLE_ROWS)
    for sr, (lon, lat) in coord_points.items():
        merger.ingest_response(
            conn, sr, "provA", "v1", "2026-11-22T08:00:00+00:00",
            [cand(f"addr-{sr}", lat, lon)])
    # T_PENDING_WITH_COORD：provB 响应因重复 rank 被隔离而不可用 -> provA 自动选定
    # 坐标，同时挂 provider_unavailable 未决原因；名册必须无视未决原因按选定坐标分区
    merger.ingest_response(
        conn, "T_PENDING_WITH_COORD", "provB", "v1", "2026-11-22T08:00:00+00:00",
        [{"rank": 1, "normalized_address": "x", "lat": 40.16, "lon": 100.16,
          "confidence": 0.8},
         {"rank": 1, "normalized_address": "y", "lat": 40.17, "lon": 100.17,
          "confidence": 0.5}])
    # 缺坐标行 1：只有无效坐标候选 -> 选择无坐标
    merger.ingest_response(
        conn, "T_NO_COORD_1", "provA", "v1", "2026-11-22T08:00:00+00:00",
        [{"rank": 1, "normalized_address": "bad", "lat": 999.0, "lon": 0.0,
          "confidence": 0.9}])
    # 缺坐标行 2：完全无候选响应（pending 且无坐标）
    snap = merger.create_snapshot(conn, "B1",
                                  {"provA": "v1", "provB": "v1"}, "r1")
    sid = snap["snapshot_id"]
    # 固定事实：缺坐标行确实没有选定坐标；待决行有坐标且带未决原因
    for sr in NO_COORD_ROWS:
        sel = conn.execute("SELECT sel_lat,sel_lon FROM selections WHERE snapshot_id=? "
                           "AND source_row=?", (sid, sr)).fetchone()
        assert sel["sel_lat"] is None and sel["sel_lon"] is None, sr
    psel = conn.execute("SELECT sel_lat,sel_lon,pending_reasons_json,decision "
                        "FROM selections WHERE snapshot_id=? "
                        "AND source_row='T_PENDING_WITH_COORD'", (sid,)).fetchone()
    assert psel["decision"] == "auto" and psel["sel_lat"] is not None \
        and psel["sel_lon"] is not None and json.loads(psel["pending_reasons_json"]), \
        "待决行必须同时有选定坐标和未决原因"
    conn.close()
    return sid


def record_map(roster):
    return {r["source_row"]: r for r in roster["records"]}


class SqlTracer:
    """收集连接执行过的 SQL 文本（数据库追踪的等效观测）。

    CPython sqlite3 对 trace 回调只持弱引用：直接传绑定方法
    ``conn.set_trace_callback(tracer)`` 时，若 sqlite3 是唯一持有者，
    包装对象可能被回收导致回调静默失效。``install`` 用闭包捕获 self 形成
    强引用环，保证追踪可靠。
    """
    def __init__(self):
        self.statements = []

    def _record(self, statement):
        self.statements.append(statement)

    def install(self, conn):
        def callback(statement):
            self._record(statement)
        conn.set_trace_callback(callback)
        return callback          # 调用方持有，进一步保证不被回收


def main():
    tmp = tempfile.mkdtemp(prefix="dispatch_test_")
    db = os.path.join(tmp, "roster.db")
    worker = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "dispatch_worker.py")

    section("0. 构造归并器现有数据库与成功快照")
    sid = build_snapshot(db)
    expected_total = len(POINTS) + len(MIDDLE_ROWS) + len(NO_COORD_ROWS)
    print(f"快照 {sid}，地址全集 {expected_total} 行")

    # ================================================== 0b. 几何校验纯函数
    section("0b. 几何合法性：闭合/自交/退化/孔关系/日期变更线")
    def reject_geo(tag, geometry):
        try:
            dispatch.validate_polygon(geometry)
            check(tag, False, "未拒绝")
        except dispatch.GeometryError:
            check(tag, True)

    reject_geo("环不闭合", {"outer": [[0, 0], [10, 0], [10, 10], [0, 10]]})
    reject_geo("蝴蝶结自交", {"outer": [[0, 0], [100, 0], [0, 10], [100, 10], [0, 0]]})
    reject_geo("零面积退化环", {"outer": [[0, 0], [50, 50], [100, 100], [0, 0]]})
    reject_geo("共线重叠退化边",
               {"outer": [[0, 0], [50, 0], [50, 0], [100, 0], [100, 50],
                          [0, 50], [0, 0]]})
    reject_geo("孔顶点贴外环边线",
               {"outer": ring(0, 0, 100, 100),
                "holes": [[[0, 50], [20, 50], [20, 70], [0, 70], [0, 50]]]})
    reject_geo("孔顶点等于外环顶点",
               {"outer": ring(0, 0, 100, 100),
                "holes": [[[0, 0], [20, 0], [20, 20], [0, 20], [0, 0]]]})
    reject_geo("两孔共边接触",
               {"outer": ring(0, 0, 100, 100),
                "holes": [[[10, 10], [30, 10], [30, 30], [10, 30], [10, 10]],
                          [[30, 10], [50, 10], [50, 30], [30, 30], [30, 10]]]})
    reject_geo("孔嵌套",
               {"outer": ring(0, 0, 100, 100),
                "holes": [ring(10, 10, 40, 40), ring(20, 20, 30, 30)]})
    reject_geo("跨日期变更线",
               {"outer": [[175000000, 0], [-175000000, 0], [-175000000, 1000000],
                          [175000000, 1000000], [175000000, 0]]})
    reject_geo("非整数顶点",
               {"outer": [[0, 0], [10.5, 0], [10, 10], [0, 10], [0, 0]]})
    # 合法：两个互不接触的孔
    ok_holes = dispatch.validate_polygon(
        {"outer": ring(0, 0, 100, 100),
         "holes": [ring(10, 10, 20, 20), ring(30, 30, 40, 40)]})
    check("两个独立孔合法接受", len(ok_holes["holes"]) == 2)
    # 绕向不影响判定
    cw = dispatch.validate_polygon(
        {"outer": [[0, 0], [0, 80], [80, 80], [80, 0], [0, 0]]})
    ccw = dispatch.validate_polygon({"outer": ring(0, 0, 80, 80)})
    check("顺/逆时针结果一致",
          dispatch.point_covered(Decimal(10), Decimal(10), cw)
          == dispatch.point_covered(Decimal(10), Decimal(10), ccw) is True)
    # 非格点贴边无容差：差 0.1 微度在外侧不被吞，恰在边上入区
    edge_poly = dispatch.validate_polygon(
        {"outer": [[100000000, 0], [100001000, 0], [100001000, 1000],
                   [100000000, 1000], [100000000, 0]]})
    check("恰在边线入区（无容差）",
          dispatch.point_covered(dispatch.decimal_micro(100.0), Decimal(500),
                                 edge_poly) is True)
    check("外侧 0.1 微度不被吞",
          dispatch.point_covered(dispatch.decimal_micro(99.9999999), Decimal(500),
                                 edge_poly) is False)
    check("非整数微度不格点化",
          str(dispatch.decimal_micro(100.0000001)) == "100000000.1000000")

    # ============================================================ 1. 首次建册
    section("1. 首次建册 -> revision 1，逐行人工判定")
    conn = dispatch.connect(db)
    built = dispatch.build_roster(conn, ZONES_REV1, snapshot_id=sid)
    check("建册修订号为 1", built["revision"] == 1)
    check("建册固定快照 ID", built["snapshot_id"] == sid)
    check("建册地址总数=快照 selections 数", built["addresses"] == expected_total,
          str(built))
    check("每个 source_row 恰好一条派单记录",
          conn.execute("SELECT COUNT(*) c FROM roster_records").fetchone()["c"]
          == expected_total)

    r1 = dispatch.read_roster(conn)
    check("读取带快照 ID 与修订号", r1["snapshot_id"] == sid and r1["revision"] == 1)
    check("记录按 source_row 排序",
          [r["source_row"] for r in r1["records"]]
          == sorted(r["source_row"] for r in r1["records"]))
    rm = record_map(r1)
    for sr, (_lon, _lat, expect) in POINTS.items():
        got = (rm[sr]["status"], rm[sr]["zone_id"])
        check(f"rev1/{sr} 归属", got == expect, f"expect {expect} got {got}")
        check(f"rev1/{sr} last_revision=1", rm[sr]["last_revision"] == 1)
    for sr in NO_COORD_ROWS:
        check(f"rev1/{sr} 缺坐标",
              rm[sr]["status"] == "no_coordinate" and rm[sr]["zone_id"] is None,
              str(rm[sr]))
    for sr in MIDDLE_ROWS:
        check(f"rev1/{sr} 未覆盖且记录存在",
              rm[sr]["status"] == "uncovered" and rm[sr]["zone_id"] is None)
    # 重复坐标是不同派单记录
    check("重复坐标两行独立且主键不冲突",
          rm["T_DUP_A"]["source_row"] != rm["T_DUP_B"]["source_row"]
          and rm["T_DUP_A"]["micro_lon"] == rm["T_DUP_B"]["micro_lon"])
    # 坐标换算精确、不格点化：微度文本应精确等于整数微度
    lonu, latu = md(POINTS["T_OUTER_EDGE"][0]), md(POINTS["T_OUTER_EDGE"][1])
    check("微度换算精确不入格点",
          rm["T_OUTER_EDGE"]["micro_lon"] == str(lonu)
          and rm["T_OUTER_EDGE"]["micro_lat"] == str(latu),
          f"{rm['T_OUTER_EDGE']['micro_lon']},{rm['T_OUTER_EDGE']['micro_lat']}")
    # 原归并数据未被改写：快照 selections 仍可经 merger 读取且只读触发器还在
    check("merger 表未被改写（selections 行数不变）",
          conn.execute("SELECT COUNT(*) c FROM selections WHERE snapshot_id=?",
                       (sid,)).fetchone()["c"] == expected_total)
    trig = conn.execute(
        "SELECT COUNT(*) c FROM sqlite_master WHERE type='trigger' "
        "AND name='trg_selections_no_update'").fetchone()["c"]
    check("merger 只读触发器仍在", trig == 1)
    # 重复建册被拒
    try:
        dispatch.build_roster(conn, ZONES_REV1, snapshot_id=sid)
        check("重复建册被拒绝", False, "未报错")
    except dispatch.RosterError:
        check("重复建册被拒绝", True)

    # ============================================================ 2. 非法输入整批拒绝
    section("2. 非法批次/几何整体拒绝，名册不变")
    def expect_reject(tag, batch_text_or_obj, err=dispatch.RosterError):
        before = json.dumps(dispatch.read_roster(conn), sort_keys=True, default=str)
        try:
            if isinstance(batch_text_or_obj, str):
                dispatch.apply_changes(conn, dispatch.normalize_batch(batch_text_or_obj))
            else:
                dispatch.apply_changes(conn, batch_text_or_obj)
            check(tag, False, "未报错")
        except err:
            check(tag, True)
        except dispatch.GeometryError:
            check(tag, True)
        after = json.dumps(dispatch.read_roster(conn), sort_keys=True, default=str)
        check(f"{tag} 后名册不变", before == after)

    expect_reject("空批次被拒", {"expected_revision": 1, "changes": {}})
    expect_reject("删除不存在的区被拒",
                  {"expected_revision": 1, "changes": {"NO_SUCH_ZONE": None}})
    expect_reject("过期修订号被拒",
                  {"expected_revision": 99, "changes":
                   {"S": zone(4, ring(md(100.6), md(40.6), md(101.0), md(41.0)))}})
    expect_reject("非法几何被拒（环不闭合）",
                  {"expected_revision": 1, "changes":
                   {"S": {"priority": 4, "geometry": {"outer": [[0, 0], [1, 0], [1, 1]]}}}})
    expect_reject("非整数优先级被拒",
                  {"expected_revision": 1, "changes":
                   {"S": zone(4.5, ring(0, 0, 1, 1))}}, dispatch.GeometryError)
    expect_reject("空区 ID 被拒",
                  {"expected_revision": 1, "changes":
                   {"": zone(1, ring(0, 0, 1, 1))}}, dispatch.GeometryError)
    # 同批有效项 + 无效项：整批拒绝
    mixed = {"expected_revision": 1, "changes": {
        "S": zone(4, ring(md(100.60), md(40.60), md(101.00), md(41.00))),       # 有效
        "BAD": zone(3, {"outer": [[0, 0], [10, 0], [10, 10]]})}}               # 无效
    expect_reject("同批有效+无效项整批拒绝", mixed)
    # JSON 同批重复区 ID（重复键）
    dup_json = ('{"expected_revision": 1, "changes": {'
                f'"S": {json.dumps(zone(4, ring(0,0,1,1)))} , '
                f'"S": {json.dumps(zone(5, ring(0,0,2,2)))} }}')
    expect_reject("同批重复区 ID（重复 JSON 键）被拒", dup_json)
    check("全部拒绝后修订号仍是 1", dispatch.current_revision(conn) == 1)

    # ================================================= 3. 局部改区：M 从甲搬到乙
    section("3. M 从街区甲移到远处乙：只重算 bbox 并集内地址")
    m_new = zone(10, ring(md(120.0), md(10.0), md(130.0), md(20.0)))
    batch_move = {"expected_revision": 1, "changes": {"M": m_new}}
    tracer = SqlTracer()
    _trace_cb = tracer.install(conn)
    move_result = dispatch.apply_changes(conn, batch_move)
    conn.set_trace_callback(None)

    check("成功批次修订号只 +1（->2）", move_result["revision"] == 2)
    check("改动区只有 M", move_result["touched_zones"] == ["M"])
    # 旧 bbox（甲）与新 bbox（乙）是两个独立矩形，未合并成横跨大矩形
    boxes = move_result["bboxes"]
    check("保留旧、新两个独立外包矩形", len(boxes) == 2, str(boxes))
    check("没有横跨甲乙的合并大矩形",
          all(not (b[0] < 110_000_000 and b[2] > 115_000_000) for b in boxes),
          str(boxes))
    # 数据库追踪：定位查询走索引区间，不出现全表扫描
    bbox_sql = [s for s in tracer.statements
                if "roster_addresses" in s and "BETWEEN" in s.upper()]
    check("改区通过 bbox BETWEEN 索引查询定位", len(bbox_sql) == 2,
          f"got {len(bbox_sql)} bbox queries: {bbox_sql}")
    full_scan = [s for s in tracer.statements if re_full_scan(s)]
    check("追踪中无 roster 表全表扫描（无不带 WHERE 的 SELECT）",
          not full_scan, str(full_scan[:3]))
    # EXPLAIN QUERY PLAN 核实 bbox 查询使用索引而非 SCAN
    plan = conn.execute(
        "EXPLAIN QUERY PLAN SELECT source_row FROM roster_addresses "
        "WHERE x_floor BETWEEN ? AND ? AND y_floor BETWEEN ? AND ? "
        "AND lon_text IS NOT NULL",
        (md(100.7), md(100.95), md(40.7), md(40.95))).fetchall()
    plan_text = " ".join(p["detail"] for p in plan)
    check("查询计划使用 idx_roster_xy 索引（非 SCAN）",
          "idx_roster_xy" in plan_text and "SCAN roster_addresses" not in plan_text,
          plan_text)
    # 每个矩形的命中计数（观测定位范围）
    hits = {tuple(b["bbox"]): b["addresses"] for b in move_result["per_box_hits"]}
    check("乙 bbox 命中 rev1 未覆盖的两个东部地址",
          sum(v for v in hits.values()) >= 2, str(hits))

    r2 = dispatch.read_roster(conn)
    rm2 = record_map(r2)
    check("rev2 修订号", r2["revision"] == 2)
    # 甲：M 移走后由未编辑的区接手（ZT 优先级 6 胜过 S 优先级 4）
    check("甲处 T_M_WINS 由未编辑区 ZT 接手",
          (rm2["T_M_WINS"]["status"], rm2["T_M_WINS"]["zone_id"])
          == ("assigned", "ZT"), str(rm2["T_M_WINS"]))
    check("甲处 T_DUP_A 由 ZT 接手",
          (rm2["T_DUP_A"]["status"], rm2["T_DUP_A"]["zone_id"]) == ("assigned", "ZT"))
    check("甲处 T_DUP_B 由 ZT 接手",
          (rm2["T_DUP_B"]["status"], rm2["T_DUP_B"]["zone_id"]) == ("assigned", "ZT"))
    check("甲处 T_ZT_ONLY 回到 ZT（未编辑区仍获胜）",
          (rm2["T_ZT_ONLY"]["status"], rm2["T_ZT_ONLY"]["zone_id"])
          == ("assigned", "ZT"), str(rm2["T_ZT_ONLY"]))
    check("T_S_ONLY 不在 M 旧 bbox，仍派 S 且不重算",
          (rm2["T_S_ONLY"]["zone_id"], rm2["T_S_ONLY"]["last_revision"])
          == ("S", 1), str(rm2["T_S_ONLY"]))
    # 乙：原先未覆盖地址得到 M 派单
    check("乙处 T_EAST_1 获得 M 派单",
          (rm2["T_EAST_1"]["status"], rm2["T_EAST_1"]["zone_id"])
          == ("assigned", "M"), str(rm2["T_EAST_1"]))
    check("乙处 T_EAST_2 获得 M 派单",
          (rm2["T_EAST_2"]["status"], rm2["T_EAST_2"]["zone_id"])
          == ("assigned", "M"))
    # 范围内记录 last_revision 推进到 2（甲处 M 旧 bbox 内 + 乙处 M 新 bbox 内）
    moved_rows = ("T_M_WINS", "T_DUP_A", "T_DUP_B", "T_ZT_ONLY",
                  "T_EAST_1", "T_EAST_2")
    for sr in moved_rows:
        check(f"rev2/{sr} last_revision=2", rm2[sr]["last_revision"] == 2,
              f"got {rm2[sr]['last_revision']}")
    # 范围外：派单记录不被重算或覆写（last_revision 留痕仍是 1，归属不变）
    outside = [sr for sr in rm2 if sr not in moved_rows]
    untouched_bad = [sr for sr in outside if rm2[sr]["last_revision"] != 1]
    check("范围外所有记录 last_revision 仍为 1（无区外重算）",
          not untouched_bad, str(untouched_bad[:5]))
    for sr in list(MIDDLE_ROWS)[:5] + ["T_OUTER_EDGE", "T_TIE", "T_NO_COORD_1",
                                       "T_UNCOVERED"]:
        check(f"rev2/{sr} 归属与 rev1 完全一致",
              (rm2[sr]["status"], rm2[sr]["zone_id"])
              == (rm[sr]["status"], rm[sr]["zone_id"]))
    check("40 个中间地址全部未被重算",
          all(rm2[sr]["last_revision"] == 1 for sr in MIDDLE_ROWS))
    check("缺坐标行不参与任何重算",
          all(rm2[sr]["status"] == "no_coordinate" and rm2[sr]["last_revision"] == 1
              for sr in NO_COORD_ROWS))

    # ================================================= 4. 改优先级
    section("4. 改 ZT 优先级到 20（高于 M 已搬走，影响甲处 ZT/S 重叠）")
    zt_new = zone(20, ring(md(100.70), md(40.70), md(100.90), md(40.90)))
    pr = dispatch.apply_changes(conn, {"expected_revision": 2,
                                       "changes": {"ZT": zt_new}})
    check("优先级批次修订号 ->3", pr["revision"] == 3)
    rm3 = record_map(dispatch.read_roster(conn))
    # ZT(20) 现在压过 S(4)：甲处重叠点改派 ZT
    check("提优先级后 T_M_WINS 改派 ZT",
          (rm3["T_M_WINS"]["status"], rm3["T_M_WINS"]["zone_id"])
          == ("assigned", "ZT"), str(rm3["T_M_WINS"]))
    check("乙处 M 派单不受优先级改动影响",
          (rm3["T_EAST_1"]["zone_id"], rm3["T_EAST_1"]["last_revision"])
          == ("M", 2), str(rm3["T_EAST_1"]))
    check("中间地址仍未被重算",
          all(rm3[sr]["last_revision"] == 1 for sr in MIDDLE_ROWS))

    # ================================================= 5. 删区
    section("5. 删除 ZB：其独占地址转未覆盖，重叠点归属不变")
    dr = dispatch.apply_changes(conn, {"expected_revision": 3,
                                       "changes": {"ZB": None}})
    check("删区批次修订号 ->4", dr["revision"] == 4)
    rm4 = record_map(dispatch.read_roster(conn))
    check("ZB 独占点 T_ZB_ONLY 变未覆盖",
          rm4["T_ZB_ONLY"]["status"] == "uncovered" and rm4["T_ZB_ONLY"]["zone_id"] is None)
    check("同级重叠点 T_TIE 仍派 ZA（删除 ZB 不影响）",
          rm4["T_TIE"]["zone_id"] == "ZA")
    check("删不存在的区仍被拒",
          _raises(dispatch.RosterError, dispatch.apply_changes, conn,
                  {"expected_revision": 4, "changes": {"ZB": None}}))
    check("旧修订号写入被拒",
          _raises(dispatch.RosterError, dispatch.apply_changes, conn,
                  {"expected_revision": 1,
                   "changes": {"S": zone(4, ring(0, 0, 1, 1))}}))
    check("当前修订号为 4", dispatch.current_revision(conn) == 4)
    check("修订台账完整（1 初始化 + 3 批次）",
          conn.execute("SELECT COUNT(*) c FROM roster_revisions").fetchone()["c"] == 4)

    # ================================================= 6. 提交前崩溃 + 并发
    section("6. 未提交更新的读一致性、进程退出回滚、同号并发写")
    # 6a. 另开连接：在写事务未提交期间只能读到完整旧版（rev4）
    stall_batch_file = os.path.join(tmp, "stall_batch.json")
    stall_ready = os.path.join(tmp, "stall_ready")
    json.dump({"expected_revision": 4, "changes": {"S": zone(
        7, ring(md(100.60), md(40.60), md(101.00), md(41.00)))}},
        open(stall_batch_file, "w", encoding="utf-8"))
    env = dict(os.environ, DISPATCH_PROJECT_DIR=os.path.dirname(os.path.abspath(__file__)))
    # 子进程持有打开但不提交的事务，写 ready 后保持存活
    stall = subprocess.Popen(
        [sys.executable, "-I", worker, "stall", db, stall_batch_file, stall_ready],
        env=env, stderr=subprocess.PIPE)
    try:
        _wait_ready(stall_ready, timeout=10)
        check("stall 子进程仍存活且持有未提交事务", stall.poll() is None)
        reader = dispatch.connect(db)
        ro = dispatch.read_roster(reader)
        check("写未提交时另开连接读到完整旧版 rev4", ro["revision"] == 4,
              f"got {ro['revision']}")
        check("读不到未提交的 S 优先级变化",
              reader.execute("SELECT priority FROM dz_zones WHERE zone_id='S'"
                             ).fetchone()["priority"] == 4)
        check("旧版记录完整可读", record_map(ro) and len(ro["records"]) == expected_total)
        reader.close()
    finally:
        stderr = stall.communicate(timeout=20)[1].decode()
    check("stall 子进程已退出（未提交）", "without commit" in stderr, stderr)
    # 6b. 写进程提交前退出后重开：仍从上一版（rev4）继续
    conn2 = dispatch.connect(db)
    check("进程退出后重开修订号仍是 4（事务已回滚）",
          dispatch.current_revision(conn2) == 4)
    check("回滚后 S 优先级恢复为 4",
          conn2.execute("SELECT priority FROM dz_zones WHERE zone_id='S'"
                        ).fetchone()["priority"] == 4)
    roster_after_crash = dispatch.read_roster(conn2)
    check("回滚后名册与崩溃前 rev4 完全一致",
          json.dumps(roster_after_crash, sort_keys=True, default=str)
          == json.dumps(dispatch.read_roster(conn), sort_keys=True, default=str))
    # 6c. 再正常提交同一批 -> rev5
    ok_batch = dispatch.normalize_batch(open(stall_batch_file, encoding="utf-8").read())
    committed = dispatch.apply_changes(conn2, ok_batch)
    check("同一批在回滚后正常提交 -> rev5", committed["revision"] == 5)
    check("提交后 S 优先级为 7",
          conn2.execute("SELECT priority FROM dz_zones WHERE zone_id='S'"
                        ).fetchone()["priority"] == 7)
    conn2.close()

    # 6d. 同一预期修订号的并发写至多一个成功（同进程两连接 + 线程同步）
    # check_same_thread=False 允许连接在工作线程内使用；BEGIN IMMEDIATE 串行化写入
    w1 = sqlite3.connect(db, timeout=10, check_same_thread=False)
    w1.row_factory = sqlite3.Row
    w1.execute("PRAGMA foreign_keys=ON")
    w2 = sqlite3.connect(db, timeout=10, check_same_thread=False)
    w2.row_factory = sqlite3.Row
    w2.execute("PRAGMA foreign_keys=ON")
    barrier = threading.Barrier(2)
    outcomes = {}

    def writer(tag, c, expected, new_prio):
        barrier.wait()
        try:
            dispatch.apply_changes(c, {"expected_revision": expected, "changes": {
                "S": zone(new_prio, ring(md(100.60), md(40.60),
                                         md(101.00), md(41.00)))}})
            outcomes[tag] = "ok"
        except dispatch.RosterError as e:
            outcomes[tag] = f"rejected:{type(e).__name__}"
        finally:
            c.close()

    t1 = threading.Thread(target=writer, args=("w1", w1, 5, 8))
    t2 = threading.Thread(target=writer, args=("w2", w2, 5, 9))
    t1.start(); t2.start(); t1.join(); t2.join()
    check("同预期号并发写恰好一个成功",
        sorted(outcomes.values()) == ["ok", "rejected:RosterError"], str(outcomes))
    check("并发后修订号只前进一次 ->6",
          dispatch.connect(db).execute(
              "SELECT value v FROM dz_meta WHERE key='revision'").fetchone()["v"] == "6")

    # ================================================= 7. 独立子进程 writer 复核
    section("7. 子进程方式复核并发写冲突结果")
    stale_file = os.path.join(tmp, "stale_batch.json")
    out_file = os.path.join(tmp, "writer_out.json")
    json.dump({"expected_revision": 5, "changes": {"S": zone(
        1, ring(md(100.60), md(40.60), md(101.00), md(41.00)))}},
        open(stale_file, "w", encoding="utf-8"))
    subprocess.run([sys.executable, "-I", worker, "writer", db, stale_file, out_file],
                   env=env, check=True)
    res = json.load(open(out_file, encoding="utf-8"))
    check("子进程以旧预期号写入被拒", res["ok"] is False
          and res["error_type"] == "RosterError", str(res))

    # ================================================= 8. CLI 冒烟
    section("8. CLI 冒烟（build/apply/read/validate-geometry）")
    cli_db = os.path.join(tmp, "cli.db")
    build_snapshot_cli = sid  # 复用快照结构：新建独立库
    # 为 CLI 建一个新库
    cli_sid = build_snapshot(cli_db)
    zones_file = os.path.join(tmp, "zones.json")
    json.dump(ZONES_REV1, open(zones_file, "w", encoding="utf-8"))
    cp = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__),
                                                      "dispatch.py"),
                         "build", cli_db, zones_file, "--snapshot", str(cli_sid)],
                        capture_output=True, text=True, check=True)
    check("CLI build 输出 revision 1", '"revision": 1' in cp.stdout, cp.stdout)
    read_out = os.path.join(tmp, "roster_read.json")
    subprocess.run([sys.executable, "dispatch.py", "read", cli_db, "--out", read_out],
                   cwd=os.path.dirname(os.path.abspath(__file__)),
                   capture_output=True, text=True, check=True)
    loaded = json.load(open(read_out, encoding="utf-8"))
    check("CLI read 带快照 ID/修订号/完整记录",
          loaded["snapshot_id"] == cli_sid and loaded["revision"] == 1
          and loaded["record_count"] == expected_total)
    bfile = os.path.join(tmp, "batch.json")
    json.dump({"expected_revision": 1, "changes":
               {"S": zone(4, ring(md(100.60), md(40.60), md(101.00), md(41.00)))}},
              open(bfile, "w", encoding="utf-8"))
    cp = subprocess.run([sys.executable, "dispatch.py", "apply", cli_db, bfile],
                        cwd=os.path.dirname(os.path.abspath(__file__)),
                        capture_output=True, text=True, check=True)
    check("CLI apply 输出 revision 2", '"revision": 2' in cp.stdout, cp.stdout)
    cp = subprocess.run([sys.executable, "dispatch.py", "validate-geometry", zones_file],
                        cwd=os.path.dirname(os.path.abspath(__file__)),
                        capture_output=True, text=True, check=True)
    check("CLI validate-geometry 通过", '"ok": true' in cp.stdout)

    print(f"\n共 {N_CHECKS[0]} 项核对，失败 {len(FAILURES)} 项")
    if FAILURES:
        print("失败项：")
        for f in FAILURES:
            print(" -", f)
        sys.exit(1)
    print("全部通过。")


def re_full_scan(statement):
    """识别对 roster 表的无过滤全表扫描语句。"""
    s = " ".join(statement.split()).upper()
    if "FROM ROSTER_ADDRESSES" not in s and "FROM ROSTER_RECORDS" not in s:
        return False
    head = s.split(" FROM")[0]
    return head.startswith("SELECT") and " WHERE " not in s


def _raises(exc, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
        return False
    except exc:
        return True


def _wait_ready(path, timeout):
    """轮询等待子进程写出 ready 标记（表示已进入未提交事务并持锁）。"""
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return True
        time.sleep(0.05)
    raise RuntimeError("等待子进程 ready 标记超时")


if __name__ == "__main__":
    main()
