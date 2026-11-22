#!/usr/bin/env python3
"""覆盖审计场景测试（可复跑）：手工几何推导 + 交错提交一致性 + CLI。

运行：``python3 coverage_audit_test.py``（子进程辅助复用 dispatch_worker.py）。

核对方式（不拿程序输出当预期）
------------------------------
所有期望面积与区域个数都从几何本身手工推导（推导过程写在各场景注释里），
定位点用**手写的严格不等式**验证落在应有区域内部；程序输出只被拿来与这些
独立推导比对。另用 dispatch.point_covered 对定位点做签名一致性交叉核对。

场景一览（坐标均为整数微度，小数值便于手算）
------------------------------------------
S1 斜边非整数交点：A=三角形 (0,0),(8,0),(0,8)，斜边 x+y=8；
   B=四边形 (0,5),(9,2),(9,8),(0,8)，斜边 y=5-x/3。两斜边交于 (9/2, 7/2)。
   窗口 [0,9]x[0,8]（面积 72）：
     A∩B   = 三角形 (0,5),(0,8),(9/2,7/2)，面积 = 1/2·3·9/2 = 27/4
     漏划  = 四边形 (9/2,7/2),(8,0),(9,0),(9,2)，面积 = 49/12+26/12 = 25/4
     A 独占 = 32-27/4 = 101/4；B 独占 = 81/2-27/4 = 135/4（单区覆盖，不报）
     校验：101/4+135/4+27/4+25/4 = 288/4 = 72 = 窗口面积
   若把斜边交点舍入到整数微度，27/4 与 25/4 都算不出来。
S2 孔被另一区精确填满 + 两区贴合边：C=[20,24]² 含孔 [21,23]²，D=[21,23]²
   恰填满孔，E=[24,26]x[20,24] 与 C 共边 x=24。窗口 [20,26]x[20,24] 内处处
   恰好单区覆盖 -> 0 异常（孔与贴合边都不制造伪缺口/伪重叠）。
S3 角对角漏划：F=[40,42]x[0,2]，G=[42,44]x[2,4] 只在点 (42,2) 相碰。
   窗口 [40,44]x[0,4]：NW、SE 两片漏划各 2x2=4，只在一点相碰 -> 必须报
   2 片（错误合并会报 1 片面积 8）。
S4 先截窗口：H=[50,54]x[1,3] 横贯窗口 [51,53]x[0,4]。上下两条漏划在窗口外
   绕 H 两端相通，窗内分开计 -> 2 片各 2x1=2。
S5 三重叠加 + 窗口截取：J=[60,64]x[0,4]，K=[62,66]x[0,4]，L=[62,65]x[1,3]。
   窗口 [63,64]x[0,4]：y∈[0,1] {J,K} 面积 1；y∈[1,3] {J,K,L} 面积 2；
   y∈[3,4] {J,K} 面积 1。两片 {J,K} 被 {J,K,L} 隔开 -> 各报各的，共 3 片。
S6 负坐标：P=[-4,-1]x[20,23]，Q=[-3,1]x[20,23]。窗口 [-5,2]x[20,23]：
   漏划 [-5,-4] 与 [1,2] 各 1x3=3；重叠 {P,Q} [-3,-1]x[20,23] 面积 2x3=6。

交错一致性
----------
* 改区批次未提交（stall 子进程持事务）时，API 与 CLI 审计都读到旧修订号
  与旧几何；子进程退出回滚后仍是旧版；正式提交后读到新修订号与新几何。
* 读事务进行中另一连接提交改区（InterleaveConn 在修订号读取后插入提交）：
  报告仍把旧修订号与旧几何配对，绝不出现旧修订号+新区表。
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from fractions import Fraction

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import coverage_audit
import dispatch
from dispatch_roster_test import build_snapshot, ring, zone

PROJECT = os.path.dirname(os.path.abspath(__file__))
WORKER = os.path.join(PROJECT, "dispatch_worker.py")

FAILURES = []
N_CHECKS = [0]


def check(name, cond, detail=""):
    N_CHECKS[0] += 1
    if not cond:
        FAILURES.append(name)
        print(f"[FAIL] {name} -- {detail}")


def section(title):
    print(f"\n=== {title} ===")


def frac(s):
    """报告中的 分子/分母 字符串 -> Fraction。"""
    num, den = s.split("/")
    return Fraction(int(num), int(den))


# ---------------------------------------------------------------- 测试数据

AUDIT_ZONES = {
    # S1：斜边在非整数坐标相交
    "A": zone(1, [[0, 0], [8, 0], [0, 8], [0, 0]]),
    "B": zone(2, [[0, 5], [9, 2], [9, 8], [0, 8], [0, 5]]),
    # S2：孔被精确填满 + 贴合边
    "C": zone(3, ring(20, 20, 24, 24), holes=[ring(21, 21, 23, 23)]),
    "D": zone(4, ring(21, 21, 23, 23)),
    "E": zone(5, ring(24, 20, 26, 24)),
    # S3：角对角
    "F": zone(6, ring(40, 0, 42, 2)),
    "G": zone(7, ring(42, 2, 44, 4)),
    # S4：横贯窗口的区
    "H": zone(8, ring(50, 1, 54, 3)),
    # S5：三重叠加
    "J": zone(9, ring(60, 0, 64, 4)),
    "K": zone(10, ring(62, 0, 66, 4)),
    "L": zone(11, ring(62, 1, 65, 3)),
    # S6：负坐标
    "P": zone(12, ring(-4, 20, -1, 23)),
    "Q": zone(13, ring(-3, 20, 1, 23)),
}

W1 = (0, 0, 9, 8)
W2 = (20, 20, 26, 24)
W3 = (40, 0, 44, 4)
W4 = (51, 0, 53, 4)
W5 = (63, 0, 64, 4)
W6 = (-5, 20, 2, 23)
W_ALL = (-180_000_000, -90_000_000, 180_000_000, 90_000_000)


def _in_box(x, y, minx, miny, maxx, maxy):
    """点严格位于开矩形内部（手写严格不等式，独立验证定位点）。"""
    return minx < x < maxx and miny < y < maxy


# 每个窗口的期望区域：[(zones, area, 定位点严格内部判定)]，全部手工推导
EXPECT = {
    W1: [
        ([], "25/4",
         lambda x, y: x + y > 8 and y < 5 - x / 3 and 0 < y and x < 9),
        (["A", "B"], "27/4",
         lambda x, y: 0 < x < 9 and 0 < y < 8 and x + y < 8 and y > 5 - x / 3),
    ],
    W2: [],
    W3: [
        ([], "4/1", lambda x, y: _in_box(x, y, 40, 2, 42, 4)),
        ([], "4/1", lambda x, y: _in_box(x, y, 42, 0, 44, 2)),
    ],
    W4: [
        ([], "2/1", lambda x, y: _in_box(x, y, 51, 0, 53, 1)),
        ([], "2/1", lambda x, y: _in_box(x, y, 51, 3, 53, 4)),
    ],
    W5: [
        (["J", "K"], "1/1", lambda x, y: _in_box(x, y, 63, 0, 64, 1)),
        (["J", "K"], "1/1", lambda x, y: _in_box(x, y, 63, 3, 64, 4)),
        (["J", "K", "L"], "2/1", lambda x, y: _in_box(x, y, 63, 1, 64, 3)),
    ],
    W6: [
        ([], "3/1", lambda x, y: _in_box(x, y, -5, 20, -4, 23)),
        ([], "3/1", lambda x, y: _in_box(x, y, 1, 20, 2, 23)),
        (["P", "Q"], "6/1", lambda x, y: _in_box(x, y, -3, 20, -1, 23)),
    ],
}


def match_scene(tag, report, window):
    """把报告区域与手工推导期望逐一配对（面积/区表/定位点全部独立核对）。"""
    check(f"{tag} 修订号", report["revision"] == 1, str(report["revision"]))
    check(f"{tag} 窗口回显",
          report["window"] == {"min_lon": window[0], "min_lat": window[1],
                               "max_lon": window[2], "max_lat": window[3]},
          str(report["window"]))
    got = list(report["regions"])
    expect = EXPECT[window]
    check(f"{tag} 区域个数", len(got) == len(expect),
          f"expect {len(expect)} got {len(got)}: {got}")
    check(f"{tag} region_count 字段一致",
          report["region_count"] == len(got))
    used = [False] * len(got)
    for zi, (zones, area, inside) in enumerate(expect):
        hit = None
        for i, r in enumerate(got):
            if used[i] or r["zones"] != zones or r["area"] != area:
                continue
            px, py = frac(r["point"][0]), frac(r["point"][1])
            if inside(px, py):
                hit = (i, px, py)
                break
        check(f"{tag} 期望区域#{zi} {zones} 面积 {area} 找到且定位点严格内部",
              hit is not None, f"got {got}")
        if hit is None:
            continue
        i, px, py = hit
        used[i] = True
        r = got[i]
        # 通用性质：区 ID 升序；面积串已约分且分母为正；点严格在窗口内
        check(f"{tag} 区域#{zi} 区 ID 字符串升序",
              r["zones"] == sorted(r["zones"]))
        check(f"{tag} 区域#{zi} 面积串约分",
              r["area"] == f"{frac(r['area']).numerator}/"
                          f"{frac(r['area']).denominator}"
              and frac(r["area"]).denominator > 0, r["area"])
        check(f"{tag} 区域#{zi} 定位点严格在窗口内",
              _in_box(px, py, window[0], window[1], window[2], window[3]),
              str(r["point"]))
        # 交叉核对：定位点经 point_covered 的签名与报告的区 ID 列表一致
        sig = sorted(zid for zid, z in AUDIT_ZONES.items()
                     if dispatch.point_covered(px, py, z["geometry"]))
        check(f"{tag} 区域#{zi} 定位点签名一致", sig == r["zones"],
              f"{sig} != {r['zones']}")
        # kind 字段与签名一致
        want_kind = "gap" if not zones else "overlap"
        check(f"{tag} 区域#{zi} kind", r["kind"] == want_kind, r["kind"])


def run_cli(db, window, extra=()):
    return subprocess.run(
        [sys.executable, "dispatch.py", "audit-coverage", db,
         "--window", *(str(v) for v in window), *extra],
        cwd=PROJECT, capture_output=True, text=True)


def wait_ready(path, timeout=10):
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return
        time.sleep(0.05)
    raise RuntimeError("等待子进程 ready 标记超时")


def main():
    tmp = tempfile.mkdtemp(prefix="audit_test_")
    db = os.path.join(tmp, "audit.db")

    section("0. 构造名册（快照 + 全部场景区表，修订号 1）")
    sid = build_snapshot(db)
    conn = dispatch.connect(db)
    built = dispatch.build_roster(conn, AUDIT_ZONES, snapshot_id=sid)
    check("建册修订号为 1", built["revision"] == 1)
    zones_before = conn.execute(
        "SELECT * FROM dz_zones ORDER BY zone_id").fetchall()
    roster_before = dispatch.read_roster(conn)

    # -------------------------------------------------- 1. 手工推导场景
    section("1. 手工几何推导：面积/区域个数/定位点（S1-S6）")
    reports = {}
    for window in (W1, W2, W3, W4, W5, W6):
        tag = f"W{window}"
        reports[window] = coverage_audit.audit_coverage(conn, window)
        match_scene(tag, reports[window], window)
    check("S2 孔与贴合边零异常", reports[W2]["regions"] == [],
          str(reports[W2]["regions"]))

    section("1b. 全球窗口：负坐标/边界极值合法，区域数手算")
    # 全球窗口手算：重叠 6 片 —— {A,B} 27/4；{P,Q} 6；J∩K 去掉 L 后上下两条
    # {J,K} 各 2x1=2；{J,K,L}=[62,64]x[1,3] 面积 4；L 超出 J 的部分
    # [64,65]x[1,3] 被 K、L 覆盖 -> {K,L} 面积 2。漏划 1 片（世界减去所有区，
    # 面积 = 360000000*180000000 - 579/4 = 259199999999999421/4，其中覆盖并集
    # = 343/2 - (27/4+6+8+4+6) + 4 = 579/4，容斥手算）。
    r_all = coverage_audit.audit_coverage(conn, W_ALL)
    kinds = sorted(r["kind"] for r in r_all["regions"])
    check("全球窗口区域数=7（6 重叠 + 1 漏划）",
          len(r_all["regions"]) == 7
          and kinds == ["gap"] + ["overlap"] * 6,
          str([(r["kind"], r["zones"], r["area"]) for r in r_all["regions"]]))
    ovl_areas = sorted(r["area"] for r in r_all["regions"]
                       if r["kind"] == "overlap")
    check("全球窗口重叠面积集合与手算一致",
          ovl_areas == ["2/1", "2/1", "2/1", "27/4", "4/1", "6/1"],
          str(ovl_areas))
    gap_all = [r for r in r_all["regions"] if r["kind"] == "gap"][0]
    check("全球窗口漏划面积与容斥手算一致",
          gap_all["area"] == "259199999999999421/4", gap_all["area"])

    # -------------------------------------------------- 2. 稳定性与只读性
    section("2. 重复审计结果稳定；审计不改库")
    again = coverage_audit.audit_coverage(conn, W1)
    check("同一窗口同一版区表重复审计报告逐字节稳定",
          json.dumps(again, sort_keys=True)
          == json.dumps(reports[W1], sort_keys=True))
    check("审计后区表未变",
          conn.execute("SELECT * FROM dz_zones ORDER BY zone_id").fetchall()
          == zones_before)
    check("审计后名册记录未变",
          json.dumps(dispatch.read_roster(conn), sort_keys=True, default=str)
          == json.dumps(roster_before, sort_keys=True, default=str))
    check("审计后修订号未变", dispatch.current_revision(conn) == 1)

    # -------------------------------------------------- 3. 窗口校验（API）
    section("3. 非法窗口 API 拒绝")
    def reject_window(tag, w):
        try:
            coverage_audit.audit_coverage(conn, w)
            check(tag, False, "未报错")
        except coverage_audit.AuditError:
            check(tag, True)

    reject_window("零宽窗口被拒", (0, 0, 0, 10))
    reject_window("零高窗口被拒", (0, 0, 10, 0))
    reject_window("倒置经度被拒", (10, 0, 0, 10))
    reject_window("倒置纬度被拒", (0, 10, 10, 0))
    reject_window("经度超界被拒", (0, 0, 180_000_001, 10))
    reject_window("经度负向超界被拒", (-180_000_001, 0, 0, 10))
    reject_window("纬度超界被拒", (0, 0, 10, 90_000_001))
    reject_window("纬度负向超界被拒", (0, -90_000_001, 10, 0))
    reject_window("非整数（浮点）被拒", (0.5, 0, 10, 10))
    reject_window("非整数（字符串）被拒", ("0", 0, 10, 10))
    reject_window("布尔被拒", (True, 0, 10, 10))
    reject_window("长度不足被拒", (0, 0, 10))
    check("全球极限窗口合法",
          coverage_audit.validate_window(W_ALL) == W_ALL)

    # -------------------------------------------------- 4. CLI
    section("4. CLI：正常审计、稳定输出、--out")
    cp1 = run_cli(db, W1)
    check("CLI 正常审计退出码 0", cp1.returncode == 0, cp1.stderr)
    cli_report = json.loads(cp1.stdout)
    check("CLI 报告与 API 报告一致",
          json.dumps(cli_report, sort_keys=True)
          == json.dumps(reports[W1], sort_keys=True))
    cp2 = run_cli(db, W1)
    check("CLI 重复运行输出逐字节稳定", cp2.stdout == cp1.stdout)
    out_file = os.path.join(tmp, "w3.json")
    cp3 = run_cli(db, W3, extra=("--out", out_file))
    check("CLI --out 退出码 0", cp3.returncode == 0, cp3.stderr)
    check("CLI --out 文件内容即报告",
          json.load(open(out_file, encoding="utf-8"))["regions"]
          == reports[W3]["regions"])
    check("CLI 负坐标窗口（S6）退出码 0",
          run_cli(db, W6).returncode == 0)

    section("4b. CLI：无名册/缺库/非法窗口一律非零退出且不输出报告")
    no_roster = os.path.join(tmp, "no_roster.db")
    build_snapshot(no_roster)                      # 只有 merger 数据，无名册
    cp = run_cli(no_roster, W1)
    check("名册未建立：非零退出", cp.returncode != 0)
    check("名册未建立：标准输出无报告", '"regions"' not in cp.stdout,
          cp.stdout[:200])
    cp = run_cli(os.path.join(tmp, "missing.db"), W1)
    check("数据库文件不存在：非零退出", cp.returncode != 0)
    check("数据库文件不存在：标准输出无报告", '"regions"' not in cp.stdout)
    for tag, w in [("零宽", (0, 0, 0, 10)), ("倒置", (10, 0, 0, 10)),
                   ("超界", (0, 0, 180_000_001, 10))]:
        cp = run_cli(db, w)
        check(f"CLI 非法窗口（{tag}）：非零退出", cp.returncode != 0)
        check(f"CLI 非法窗口（{tag}）：无报告", '"regions"' not in cp.stdout)
    cp = subprocess.run(
        [sys.executable, "dispatch.py", "audit-coverage", db,
         "--window", "0", "0", "1.5", "10"],
        cwd=PROJECT, capture_output=True, text=True)
    check("CLI 非整数窗口：非零退出", cp.returncode != 0)
    check("CLI 非整数窗口：无报告", '"regions"' not in cp.stdout)
    cp = subprocess.run(
        [sys.executable, "dispatch.py", "audit-coverage", db,
         "--window", "0", "0", "10"],
        cwd=PROJECT, capture_output=True, text=True)
    check("CLI 窗口参数不足：非零退出", cp.returncode != 0)

    # -------------------------------------------------- 5. 交错提交一致性
    section("5. 改区提交与读取交错：修订号与几何来自同一已提交版本")
    mn_db = os.path.join(tmp, "mn.db")
    mn_sid = build_snapshot(mn_db)
    mn_zones = {"M": zone(1, ring(0, 0, 4, 4)), "N": zone(2, ring(2, 0, 6, 4))}
    mn_conn = dispatch.connect(mn_db)
    dispatch.build_roster(mn_conn, mn_zones, snapshot_id=mn_sid)
    W_MN = (0, 0, 6, 4)
    # rev1 手算：M=[0,4]²，N=[2,4]x[0,4]；重叠 [2,4]x[0,4] 面积 8，无漏划
    move_batch = {"expected_revision": 1,
                  "changes": {"N": zone(2, ring(10, 0, 14, 4))}}
    # rev2 手算：N 搬走后 M 独占 [0,4]x[0,4]，漏划 [4,6]x[0,4] 面积 8

    # 5a. 未提交写（stall 子进程）期间：API 与 CLI 都读到 rev1 几何
    stall_batch = os.path.join(tmp, "stall_batch.json")
    stall_ready = os.path.join(tmp, "stall_ready")
    json.dump(move_batch, open(stall_batch, "w", encoding="utf-8"))
    env = dict(os.environ, DISPATCH_PROJECT_DIR=PROJECT)
    stall = subprocess.Popen(
        [sys.executable, "-I", WORKER, "stall", mn_db, stall_batch, stall_ready],
        env=env, stderr=subprocess.PIPE)
    try:
        wait_ready(stall_ready)
        r = coverage_audit.audit_coverage(dispatch.connect(mn_db), W_MN)
        check("未提交写期间 API 审计读到旧修订号 1", r["revision"] == 1,
              str(r["revision"]))
        check("未提交写期间 API 审计读到旧几何（重叠 8/1）",
              [(x["zones"], x["area"]) for x in r["regions"]]
              == [(["M", "N"], "8/1")], str(r["regions"]))
        cp = run_cli(mn_db, W_MN)
        check("未提交写期间 CLI 审计退出码 0 且读旧版",
              cp.returncode == 0 and json.loads(cp.stdout)["revision"] == 1,
              cp.stderr)
    finally:
        stderr = stall.communicate(timeout=20)[1].decode()
    check("stall 子进程退出（未提交回滚）", "without commit" in stderr, stderr)
    r = coverage_audit.audit_coverage(dispatch.connect(mn_db), W_MN)
    check("回滚后审计仍是 rev1 几何", r["revision"] == 1
          and [(x["zones"], x["area"]) for x in r["regions"]]
          == [(["M", "N"], "8/1")])

    # 5b. 正式提交 -> rev2：几何与修订号一起前进
    mn_conn2 = dispatch.connect(mn_db)
    applied = dispatch.apply_changes(
        mn_conn2, dispatch.normalize_batch(json.dumps(move_batch)))
    check("正式提交 -> rev2", applied["revision"] == 2)
    r2 = coverage_audit.audit_coverage(mn_conn2, W_MN)
    check("提交后审计读到修订号 2", r2["revision"] == 2)
    check("提交后审计读到新几何（漏划 8/1）",
          [(x["zones"], x["area"]) for x in r2["regions"]] == [([], "8/1")],
          str(r2["regions"]))
    gap_pt = [frac(v) for v in r2["regions"][0]["point"]]
    check("rev2 漏划定位点严格在 (4,6)x(0,4) 内",
          _in_box(gap_pt[0], gap_pt[1], 4, 0, 6, 4), str(gap_pt))

    # 5c. 读事务进行中另一连接提交 rev3：报告仍配对 rev2 修订号与 rev2 几何
    class InterleaveConn(sqlite3.Connection):
        fired = False

        def execute(self, sql, parameters=(), /):
            cur = super().execute(sql, parameters)
            if not self.fired and "dz_meta" in sql:
                self.fired = True
                w = dispatch.connect(mn_db)     # 在读快照建立后提交 rev3
                dispatch.apply_changes(w, {"expected_revision": 2, "changes":
                                           {"M": zone(1, ring(30, 0, 34, 4))}})
                w.close()
            return cur

    iconn = sqlite3.connect(mn_db, timeout=10.0, factory=InterleaveConn)
    iconn.row_factory = sqlite3.Row
    iconn.execute("PRAGMA foreign_keys=ON")
    r3 = coverage_audit.audit_coverage(iconn, W_MN)
    check("交错提交确实发生在读事务进行中", iconn.fired)
    check("交错审计报告配对旧修订号 2", r3["revision"] == 2, str(r3["revision"]))
    check("交错审计报告的几何也是 rev2（漏划 8/1，非 rev3）",
          [(x["zones"], x["area"]) for x in r3["regions"]] == [([], "8/1")],
          str(r3["regions"]))
    iconn.close()
    check("交错后库中当前修订号为 3",
          dispatch.current_revision(dispatch.connect(mn_db)) == 3)
    r4 = coverage_audit.audit_coverage(dispatch.connect(mn_db), W_MN)
    # rev3 手算：rev2 时 N 已搬到 [10,14]x[0,4]，rev3 把 M 搬到 [30,34]x[0,4]，
    # 窗口 [0,6]x[0,4] 内无任何区 -> 整窗漏划，面积 6x4=24
    check("重新审计读到 rev3 几何（M 搬走，整窗漏划 24/1）",
          r4["revision"] == 3
          and [(x["zones"], x["area"]) for x in r4["regions"]] == [([], "24/1")],
          str([(x["zones"], x["area"]) for x in r4["regions"]]))

    print(f"\n共 {N_CHECKS[0]} 项核对，失败 {len(FAILURES)} 项")
    if FAILURES:
        print("失败项：")
        for f in FAILURES:
            print(" -", f)
        sys.exit(1)
    print("全部通过。")


if __name__ == "__main__":
    main()
