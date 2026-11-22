#!/usr/bin/env python3
"""站点同步可复跑示例（中心端 + 站点端）。

运行：``python3 sync_example.py``（可反复运行；所有库建在临时目录）。

场景
----
* 三个配送站：SITE_A（甲区）、SITE_B（乙区）、SITE_C（丙区，从不参与流转，
  但持有大量地址，用于观察导出规模）。
* 建册 rev1 后启用同步（enabled_revision=1），记录发布源 ID。
* 区 M 属于 SITE_B：先停在空地，之后反复搬进甲区只覆盖 R_MOVE_1 的小块
  （优先级高于甲的 ZA），于是该地址在 A/B 之间来回；最后一次停在乙侧不回来。
  修订链：
    rev2  M 搬入甲围绕 R_MOVE_1 的小块   -> R_MOVE_1 A->B
    rev3  M 搬回空地                     -> R_MOVE_1 B->A
    rev4  M 再搬入甲小块                 -> R_MOVE_1 A->B（此后一段留在 B）
    rev5  只改 C 的 ZC                   -> A/B 无变化
    rev6  删 ZL 并缩小 ZA/ZP             -> 站 A 失去 R_DEL（区已不在区表）
    rev7  M 搬到乙的远角                 -> R_MOVE_1 B->A（长区间起点）
    rev8..rev12 只改 C（A/B 无关的修订）
    rev13 M 搬回甲小块                   -> R_MOVE_1 A->B（不回来）
  另有一次改区停在提交前（独立库演练）。

核对
----
1. 启用幂等、发布源 ID 重启不变且独立中心库不同、启用前版本不可导出。
2. 手写 rev1/2/3/4/5/6/7/13 各站地址集，逐版本与库内折叠结果核对。
3. 初装包只含 source_row + 名册经纬度、按 source_row 排序、重复导出逐字节一致。
4. 跨版净差、转了又回不发、老目标重导一致、空补丁可应用、删区仍发删除。
5. 站点应用：重放无副作用；倒序/缺版/身份不符/重复项/冲突项整包拒绝且本地不变。
6. 失败后本地版本与地址逐字节不变。
7. 未提交的改区在独立连接上不可见、不能导出。
8. 查询计划 + SQL 追踪：补丁导出走索引区间，不扫名册、不随其他站点或空修订增长。
9. CLI：enable / export-initial / export-patch / site apply / site show。
"""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import dispatch          # noqa: E402
import merger            # noqa: E402
import sync_center       # noqa: E402
import sync_site         # noqa: E402

FAILURES = []
N = [0]


def check(name, cond, detail=""):
    N[0] += 1
    if not cond:
        FAILURES.append(name)
        print(f"[FAIL] {name} -- {detail}")


def section(t):
    print(f"\n=== {t} ===")


def ring(minx, miny, maxx, maxy):
    return [[minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny]]


def zone(priority, outer):
    return {"priority": priority, "geometry": {"outer": outer}}


MICRO = 1_000_000
JIA = (100 * MICRO, 40 * MICRO, 101 * MICRO, 41 * MICRO)
YI = (120 * MICRO, 10 * MICRO, 130 * MICRO, 20 * MICRO)
BING = (140 * MICRO, 30 * MICRO, 141 * MICRO, 31 * MICRO)
EMPTY = (0, 0, 1 * MICRO, 1 * MICRO)          # 空地：无地址
NEAR_MOVE = (100290000, 40290000, 100310000, 40310000)   # 只罩住 R_MOVE_1
YI_FAR = (128 * MICRO, 18 * MICRO, 130 * MICRO, 20 * MICRO)

ZONES = {
    "ZA": zone(5, ring(*JIA)),        # 甲：SITE_A
    "ZP": zone(9, ring(*JIA)),        # 甲：SITE_A
    "ZL": zone(7, ring(100 * MICRO, 40 * MICRO, 102 * MICRO, 42 * MICRO)),  # SITE_A
    "M":  zone(10, ring(*EMPTY)),     # SITE_B，会搬动
    "ZB": zone(5, ring(*YI)),         # 乙：SITE_B
    "ZC": zone(5, ring(*BING)),       # 丙：SITE_C
}
SITE_ZONES = {"ZA": "SITE_A", "ZP": "SITE_A", "ZL": "SITE_A",
              "M": "SITE_B", "ZB": "SITE_B", "ZC": "SITE_C"}


def cand(lat, lon, addr):
    return {"rank": 1, "normalized_address": addr, "lat": lat, "lon": lon,
            "confidence": 0.9}


SRC = {
    "R_A_1": (100.5, 40.5), "R_A_2": (100.6, 40.6),
    "R_A_3": (100.7, 40.7), "R_A_4": (100.8, 40.8),
    "R_DEL": (100.9, 40.9),        # ZL 独占于缩小后的 ZA/ZP 之外
    "R_MOVE_1": (100.3, 40.3),     # 被搬动的 M 罩住 -> 站随 M 走
    "R_LONG_1": (100.15, 40.15),
    "R_STAY": (100.45, 40.45),
    "R_B_1": (125.0, 15.0), "R_B_2": (126.0, 16.0),
    "R_C_1": (140.5, 30.5), "R_C_2": (140.6, 30.6), "R_C_3": (140.7, 30.7),
}
BULK_C = {f"R_CB_{i:03d}": (140.1 + (i % 50) * 0.001, 30.1 + (i % 40) * 0.001)
          for i in range(600)}
NEUTRAL = {f"R_N_{i:03d}": (0.5 + i * 0.01, 0.5) for i in range(50)}


def build_center_db(db):
    conn = merger.init_db(db)
    rows = [(sr, f"addr-{sr}") for sr in list(SRC) + list(BULK_C) + list(NEUTRAL)]
    merger.ingest_source_rows(conn, "B1", rows)
    merger.register_rule(conn, "r1", preferred_provider="provA",
                         distance_threshold_m=50.0, confidence_diff_threshold=0.2)
    pts = dict(SRC); pts.update(BULK_C); pts.update(NEUTRAL)
    for sr, (lon, lat) in pts.items():
        merger.ingest_response(conn, sr, "provA", "v1",
                               "2026-11-22T08:00:00+00:00",
                               [cand(lat, lon, f"addr-{sr}")])
    sid = merger.create_snapshot(conn, "B1", {"provA": "v1"}, "r1")["snapshot_id"]
    conn.close()
    dconn = dispatch.connect(db)
    built = dispatch.build_roster(dconn, ZONES, snapshot_id=sid,
                                  site_zones=SITE_ZONES)
    dconn.close()
    return sid, built


def manual_sets():
    """手写各版本各站地址集（与实现无关）。"""
    a1 = {"R_A_1", "R_A_2", "R_A_3", "R_A_4", "R_DEL", "R_MOVE_1",
          "R_LONG_1", "R_STAY"}
    a = {1: set(a1), 2: set(a1) - {"R_MOVE_1"}, 3: set(a1)}
    a[4] = set(a1) - {"R_MOVE_1"}
    a[5] = set(a[4])
    a[6] = set(a[5]) - {"R_DEL"}
    a[7] = set(a[6]) | {"R_MOVE_1"}
    for r in range(8, 13):
        a[r] = set(a[7])
    a[13] = set(a[7]) - {"R_MOVE_1"}
    # R_N_* 落在初始空地 EMPTY（lon 0..1, lat 0..1），而 M 属于 B：
    # rev2/rev4 M 搬走后它们脱离 M -> 从 B 消失；rev3 M 回空地 -> 回 B；
    # rev7 M 去乙远角、rev13 M 回甲 -> 它们一直不在 B。
    neutral = {f"R_N_{i:03d}" for i in range(50)}
    b1 = {"R_B_1", "R_B_2"} | neutral
    moved = b1 - neutral | {"R_MOVE_1"}      # M 在甲小块：R_MOVE_1 来，空地地址走
    b = {1: set(b1), 2: set(moved), 3: set(b1), 4: set(moved),
         5: set(moved), 6: set(moved), 7: b1 - neutral}
    for r in range(8, 13):
        b[r] = set(b[7])
    b[13] = set(b[7]) | {"R_MOVE_1"}         # rev13 M 回到甲小块 -> 再次来 B
    c1 = {"R_C_1", "R_C_2", "R_C_3"} | set(BULK_C)
    c = {r: set(c1) for r in range(1, 14)}
    return {"SITE_A": a, "SITE_B": b, "SITE_C": c}


def main():
    tmp = tempfile.mkdtemp(prefix="sync_example_")
    center_db = os.path.join(tmp, "center.db")
    other_db = os.path.join(tmp, "other_center.db")
    site_a_db = os.path.join(tmp, "site_a.db")
    site_b_db = os.path.join(tmp, "site_b.db")
    pkgs = os.path.join(tmp, "pkgs")
    os.makedirs(pkgs)

    section("0. 建快照 + 建册（rev1）+ 启用同步")
    sid, built = build_center_db(center_db)
    conn = dispatch.connect(center_db)
    check("建册 rev1", built["revision"] == 1 and dispatch.current_revision(conn) == 1)
    build_center_db(other_db)
    other_conn = dispatch.connect(other_db)

    en = sync_center.enable_sync(conn)
    check("启用返回发布源 ID", isinstance(en["source_id"], str) and en["source_id"])
    check("启用记下最早同步版本=当时修订号", en["enabled_revision"] == 1, str(en))
    en2 = sync_center.enable_sync(conn)
    check("重复启用返回原身份与起点",
          en2["replayed"] is True and en2["source_id"] == en["source_id"]
          and en2["enabled_revision"] == 1, str(en2))
    conn.close(); conn = dispatch.connect(center_db)
    check("重启（新连接）后发布源 ID 不变",
          sync_center._identity(conn)["source_id"] == en["source_id"])
    oen = sync_center.enable_sync(other_conn)
    check("独立启用的另一中心库发布源 ID 不同",
          oen["source_id"] != en["source_id"])
    try:
        sync_center.export_patch(conn, "SITE_A", 0, 1)
        check("启用前版本（0）拒绝导出", False, "未报错")
    except sync_center.SyncError:
        check("启用前版本（0）拒绝导出", True)
    check("启用时点的版本可导出（空补丁）",
          sync_center.export_patch(conn, "SITE_A", 1, 1)["items"] == [])

    section("1. 修订链 rev1 -> rev13")
    MANUAL = manual_sets()

    def apply(rev_expected, changes, note):
        r = dispatch.apply_changes(conn, {"expected_revision": rev_expected,
                                          "changes": changes})
        check(f"{note} -> rev{r['revision']}",
              r["revision"] == rev_expected + 1, str(r))

    def m_at(box):
        return zone(10, ring(*box))

    apply(1, {"M": m_at(NEAR_MOVE)}, "rev2 M 搬入甲小块")
    apply(2, {"M": m_at(EMPTY)}, "rev3 M 搬回空地")
    apply(3, {"M": m_at(NEAR_MOVE)}, "rev4 M 再搬入甲小块（不回来）")
    apply(4, {"ZC": zone(6, ring(*BING))}, "rev5 只改丙区")
    apply(5, {"ZA": zone(5, ring(100 * MICRO, 40 * MICRO, 100850000, 40850000)),
              "ZP": zone(9, ring(100 * MICRO, 40 * MICRO, 100850000, 40850000)),
              "ZL": None}, "rev6 删 ZL 并缩小 ZA/ZP")
    apply(6, {"M": m_at(YI_FAR)}, "rev7 M 搬到乙远角（长区间起点）")
    for i, rev in enumerate(range(7, 12)):
        apply(rev, {"ZC": zone(6 + i, ring(*BING))}, f"rev{rev + 1} 只改丙区")
    apply(12, {"M": m_at(NEAR_MOVE)}, "rev13 M 搬回甲小块（不回来）")
    check("当前修订号为 13", dispatch.current_revision(conn) == 13,
          str(dispatch.current_revision(conn)))

    section("2. 手写各端点站点地址集 == 库内折叠结果")
    for site in ("SITE_A", "SITE_B", "SITE_C"):
        for rev in sorted(MANUAL[site]):
            got = set(sync_center._site_members_at(conn, site, rev))
            want = MANUAL[site][rev]
            check(f"rev{rev}/{site} 手写期望", got == want,
                  f"missing={sorted(want - got)} extra={sorted(got - want)}")
    # 当前版本直接查库（站点以当前区 -> 站点映射的区 ID 决定）
    for site in ("SITE_A", "SITE_B", "SITE_C"):
        direct = set(sync_center._current_site_rows(conn, site))
        check(f"rev13/{site} 与直接查 roster_records 一致",
              direct == MANUAL[site][13],
              f"missing={sorted(MANUAL[site][13]-direct)} "
              f"extra={sorted(direct-MANUAL[site][13])}")
    check("C 的 600 条批量地址从未进入 A/B 的包",
          all(not any(sr.startswith("R_CB_") for sr in MANUAL[s][r])
              for s in ("SITE_A", "SITE_B") for r in MANUAL[s]))

    section("3. 初装包")
    sync_site.init_db(site_a_db).close()
    saconn = sync_site.connect(site_a_db)
    pkg_initial = sync_center.export_initial(conn, "SITE_A", 1)
    check("初装包类型/发布源/站点/目标修订",
          pkg_initial["package_type"] == "initial"
          and pkg_initial["source_id"] == en["source_id"]
          and pkg_initial["site_id"] == "SITE_A"
          and pkg_initial["target_revision"] == 1)
    check("初装包字段说明齐全",
          set(pkg_initial) == {"protocol", "package_type", "source_id", "site_id",
                               "target_revision", "items"})
    check("初装包地址项只有 source_row/lon/lat",
          all(set(it) == {"source_row", "lon", "lat"} for it in pkg_initial["items"]))
    check("初装包地址项按 source_row 排序",
          [it["source_row"] for it in pkg_initial["items"]]
          == sorted(it["source_row"] for it in pkg_initial["items"]))
    check("初装包内容 == rev1 手写 A 集",
          {it["source_row"] for it in pkg_initial["items"]} == MANUAL["SITE_A"][1])
    check("同一初装包重复导出逐字节一致",
          sync_center.package_text(pkg_initial)
          == sync_center.package_text(sync_center.export_initial(conn, "SITE_A", 1)))
    check("初装包带名册经纬度",
          {it["source_row"]: (it["lon"], it["lat"]) for it in pkg_initial["items"]}
          ["R_MOVE_1"] == (100.3, 40.3))
    res = sync_site.apply_package(saconn, pkg_initial)
    check("站点应用初装包 -> rev1", res["applied"] and res["revision"] == 1, str(res))
    st = sync_site.read_state(saconn)
    check("站点库固定发布源与站点",
          st["source_id"] == en["source_id"] and st["site_id"] == "SITE_A")
    before = json.dumps(sync_site.read_state(saconn), sort_keys=True)
    res2 = sync_site.apply_package(saconn, pkg_initial)
    check("原样重送初装包：成功且无副作用",
          res2.get("replayed") is True
          and json.dumps(sync_site.read_state(saconn), sort_keys=True) == before)

    section("4. 补丁：净差、转了又回不发、老目标重导、空补丁、删区删除项")
    p_1_2 = sync_center.export_patch(conn, "SITE_A", 1, 2)
    check("rev1->2 A：只有 R_MOVE_1 删除项",
          p_1_2["items"] == [{"op": "del", "source_row": "R_MOVE_1"}],
          str(p_1_2["items"]))
    p_1_3 = sync_center.export_patch(conn, "SITE_A", 1, 3)
    check("rev1->3 A：R_MOVE_1 转走又调回来 -> 不发（空补丁）",
          p_1_3["items"] == [], str(p_1_3["items"]))
    p_1_5 = sync_center.export_patch(conn, "SITE_A", 1, 5)
    check("rev1->5 A：R_MOVE_1 最终不在本站 -> 只发删除",
          p_1_5["items"] == [{"op": "del", "source_row": "R_MOVE_1"}],
          str(p_1_5["items"]))
    p_1_4 = sync_center.export_patch(conn, "SITE_A", 1, 4)
    check("rev1->4 A：R_MOVE_1 净删除",
          p_1_4["items"] == [{"op": "del", "source_row": "R_MOVE_1"}],
          str(p_1_4["items"]))
    p_b_1_4 = sync_center.export_patch(conn, "SITE_B", 1, 4)
    check("rev1->4 B：R_MOVE_1 净新增（完整地址行）",
          {it["source_row"]: it for it in p_b_1_4["items"]}.get("R_MOVE_1")
          == {"op": "add", "source_row": "R_MOVE_1", "lon": 100.3, "lat": 40.3,
              "zone_id": "M"}, str(p_b_1_4["items"][:3]))
    p_1_13 = sync_center.export_patch(conn, "SITE_A", 1, 13)
    check("rev1->13 A：删 R_MOVE_1 与 R_DEL（区已删仍发删除）",
          {it["source_row"]: it["op"] for it in p_1_13["items"]}
          == {"R_DEL": "del", "R_MOVE_1": "del"}, str(p_1_13["items"]))
    p_b_1_13 = sync_center.export_patch(conn, "SITE_B", 1, 13)
    check("rev1->13 B：R_MOVE_1 净新增",
          {it["source_row"]: it["op"] for it in p_b_1_13["items"]}
          .get("R_MOVE_1") == "add", str(p_b_1_13["items"][:3]))
    p_5_5 = sync_center.export_patch(conn, "SITE_A", 5, 5)
    check("无变化区间给可应用的空补丁",
          p_5_5["items"] == [] and p_5_5["from_revision"] == 5
          and p_5_5["target_revision"] == 5)
    p_7_13 = sync_center.export_patch(conn, "SITE_A", 7, 13)
    check("长区间 7->13 A：只有 R_MOVE_1 一条删除（不随其他站的记录/空修订增长）",
          p_7_13["items"] == [{"op": "del", "source_row": "R_MOVE_1"}],
          str(p_7_13["items"]))
    # 到 rev13 之后再按原目标版本重导 1->4：逐字节一致
    check("三个以上版本后按原目标版本重导同一补丁逐字节一致",
          sync_center.package_text(sync_center.export_patch(conn, "SITE_A", 1, 4))
          == sync_center.package_text(p_1_4))
    check("补丁字段齐全",
          set(p_b_1_4) == {"protocol", "package_type", "source_id", "site_id",
                           "from_revision", "target_revision", "items"})
    for bad, tag in [((5, 4), "倒序拒绝"), ((1, 99), "目标未提交拒绝"),
                     ((1, 0), "起始启用前拒绝"), ((None, 1), "非整数修订拒绝")]:
        try:
            sync_center.export_patch(conn, "SITE_A", *bad)
            check(tag, False, "未报错")
        except sync_center.SyncError:
            check(tag, True)
    try:
        sync_center.export_patch(conn, "NO_SUCH_SITE", 1, 2)
        check("未知站点不导出", False, "未报错")
    except sync_center.SyncError:
        check("未知站点不导出", True)

    # 站点依次应用 1->2->3->4->5->6->7->13
    for a, b in [(1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 13)]:
        r = sync_site.apply_package(saconn, sync_center.export_patch(conn, "SITE_A", a, b))
        check(f"A 应用 rev{a}->{b} -> rev{b}", r["applied"] and r["revision"] == b, str(r))
        got = {x["source_row"] for x in sync_site.read_addresses(saconn)}
        check(f"A 应用后地址 == 手写 rev{b}", got == MANUAL["SITE_A"][b],
              f"missing={sorted(MANUAL['SITE_A'][b] - got)} "
              f"extra={sorted(got - MANUAL['SITE_A'][b])}")

    section("5. 站点端拒绝场景（失败时本地版本/地址不变）")
    stable = json.dumps(sync_site.read_state(saconn), sort_keys=True)
    last_pkg = sync_center.export_patch(conn, "SITE_A", 7, 13)
    res = sync_site.apply_package(saconn, json.loads(json.dumps(last_pkg)))
    check("重送最近成功的包：replayed 且无副作用",
          res.get("replayed") is True
          and json.dumps(sync_site.read_state(saconn), sort_keys=True) == stable)

    def expect_reject(tag, pkg_obj):
        try:
            sync_site.apply_package(saconn, pkg_obj)
            check(tag, False, "未报错")
        except sync_site.SiteError:
            check(tag, True)
        check(f"{tag} 后本地版本/地址不变",
              json.dumps(sync_site.read_state(saconn), sort_keys=True) == stable)

    tampered = json.loads(json.dumps(last_pkg))
    tampered["items"] = sorted(tampered["items"] + [
        {"op": "del", "source_row": "R_A_1"}], key=lambda it: it["source_row"])
    expect_reject("同一版本另一份内容不同的包被拒", tampered)

    rev_same = {"protocol": "dispatch-site-sync/1", "package_type": "patch",
                "source_id": en["source_id"], "site_id": "SITE_A",
                "from_revision": 13, "target_revision": 13, "items": []}
    back = json.loads(json.dumps(rev_same))
    back["from_revision"], back["target_revision"] = 14, 13
    expect_reject("倒序补丁被拒", back)
    gap = json.loads(json.dumps(rev_same))
    gap["from_revision"], gap["target_revision"] = 6, 7
    expect_reject("缺版（起始早于本站当前）被拒", gap)
    gap2 = json.loads(json.dumps(rev_same))
    gap2["from_revision"], gap2["target_revision"] = 20, 21
    expect_reject("缺版（起始晚于本站当前）被拒", gap2)
    wrong_src = json.loads(json.dumps(rev_same))
    wrong_src["source_id"] = "SRC-SOMEONE-ELSE"
    expect_reject("发布源不符被拒", wrong_src)
    wrong_site = json.loads(json.dumps(rev_same))
    wrong_site["site_id"] = "SITE_B"
    expect_reject("站点不符被拒", wrong_site)
    dup = json.loads(json.dumps(rev_same))
    dup["items"] = [{"op": "del", "source_row": "R_A_1"},
                    {"op": "del", "source_row": "R_A_1"}]
    expect_reject("包内重复地址项被拒", dup)
    unsorted = json.loads(json.dumps(rev_same))
    unsorted["items"] = [{"op": "del", "source_row": "R_A_2"},
                         {"op": "del", "source_row": "R_A_1"}]
    expect_reject("包内未排序地址项被拒", unsorted)
    conflict = json.loads(json.dumps(rev_same))
    conflict["items"] = [{"op": "add", "source_row": "R_NEW_GOOD", "lon": 100.0,
                          "lat": 40.0, "zone_id": "ZA"},
                         {"op": "add", "source_row": "R_A_1", "lon": 100.5,
                          "lat": 40.5, "zone_id": "ZA"}]
    expect_reject("有效新增后跟冲突新增：整包拒绝", conflict)
    bad_del = json.loads(json.dumps(rev_same))
    bad_del["items"] = [{"op": "del", "source_row": "R_B_1"}]
    expect_reject("删除不在本站的地址被拒", bad_del)
    fresh = sync_site.init_db(os.path.join(tmp, "fresh.db"))
    try:
        sync_site.apply_package(fresh, sync_center.export_patch(conn, "SITE_A", 1, 2))
        check("未初装站点拒绝补丁", False, "未报错")
    except sync_site.SiteError:
        check("未初装站点拒绝补丁", True)
    check("失败后未初装站点仍为空",
          fresh.execute("SELECT COUNT(*) c FROM ss_addresses").fetchone()["c"] == 0)
    fresh.close()
    try:
        sync_site.apply_package(saconn, pkg_initial)
        check("已初装后再次初装被拒", False, "未报错")
    except sync_site.SiteError:
        check("已初装后再次初装被拒", True)
    check("再次初装被拒后本地不变",
          json.dumps(sync_site.read_state(saconn), sort_keys=True) == stable)

    section("6. 改区停在提交前：独立连接看不到、不能导出")
    stall_db = os.path.join(tmp, "stall_center.db")
    build_center_db(stall_db)
    sconn = dispatch.connect(stall_db)
    sync_center.enable_sync(sconn)
    dispatch.apply_changes(sconn, {"expected_revision": 1,
                                   "changes": {"M": m_at(NEAR_MOVE)}})
    check("演练库已到 rev2", dispatch.current_revision(sconn) == 2)
    dispatch.apply_changes(sconn, {"expected_revision": 2,
                                   "changes": {"M": m_at(EMPTY)}}, commit=False)
    reader = dispatch.connect(stall_db)
    check("独立连接当前修订仍为 2", dispatch.current_revision(reader) == 2)
    check("独立连接读不到未提交的 M 位置",
          reader.execute("SELECT geometry_json FROM dz_zones WHERE zone_id='M'"
                         ).fetchone()["geometry_json"]
          == json.dumps(dispatch.validate_polygon({"outer": ring(*NEAR_MOVE)}),
                        separators=(",", ":")))
    try:
        sync_center.export_patch(reader, "SITE_A", 2, 3)
        check("未提交的 rev3 不能导出", False, "未报错")
    except sync_center.SyncError:
        check("未提交的 rev3 不能导出", True)
    # 同一条写连接（事务未提交）也不能导出：未提交的改区不得进入任何包
    try:
        sync_center.export_patch(sconn, "SITE_A", 2, 3)
        check("写连接上有未提交事务时拒绝导出", False, "未报错")
    except sync_center.SyncError:
        check("写连接上有未提交事务时拒绝导出", True)
    check("未提交时导出 rev1->2 仍可用",
          {it["source_row"]: it["op"] for it in
           sync_center.export_patch(reader, "SITE_A", 1, 2)["items"]}
          .get("R_MOVE_1") == "del",
          str(sync_center.export_patch(reader, "SITE_A", 1, 2)["items"][:5]))
    check("独立连接看不到未提交的同步变化",
          reader.execute("SELECT COUNT(*) c FROM sync_changes WHERE sync_revision=3"
                         ).fetchone()["c"] == 0)
    reader.close()
    sconn.rollback(); sconn.close()
    reconn = dispatch.connect(stall_db)
    check("回滚后重开仍为 rev2", dispatch.current_revision(reconn) == 2)
    dispatch.apply_changes(reconn, {"expected_revision": 2, "changes": {"M": m_at(EMPTY)}})
    check("回滚后可正常提交 -> rev3", dispatch.current_revision(reconn) == 3)
    p23 = sync_center.export_patch(reconn, "SITE_A", 2, 3)
    check("正常提交后 rev3 可导出（R_MOVE_1 回到 A，带完整地址行）",
          {it["source_row"]: it for it in p23["items"]}.get("R_MOVE_1", {}).get("op")
          == "add" and {it["source_row"]: it for it in p23["items"]}
          ["R_MOVE_1"]["lon"] == 100.3, str(p23["items"][:3]))
    check("rev3 补丁同时把空地地址归还 B（M 回空地）",
          {it["source_row"]: it["op"] for it in p23["items"]}["R_N_000"] == "del"
          if "R_N_000" in {it["source_row"]: it["op"] for it in p23["items"]}
          else {it["source_row"]: it["op"] for it in
                sync_center.export_patch(reconn, "SITE_B", 2, 3)["items"]}
          .get("R_N_000") == "add")
    reconn.close()

    section("6b. 原站所有区都被删除：仍能收到删除（映射不再覆盖该站）")
    gone_db = os.path.join(tmp, "gone_center.db")
    build_center_db(gone_db)
    gonn = dispatch.connect(gone_db)
    sync_center.enable_sync(gonn)
    check("删区前 R_B_1 在 B", "R_B_1" in set(
        sync_center._current_site_rows(gonn, "SITE_B")))
    # 一批同时删除 B 的 M 与 ZB：B 从此没有任何区
    dispatch.apply_changes(gonn, {"expected_revision": 1, "changes": {
        "M": None, "ZB": None}})
    check("B 已无任何区", not sync_center._zone_site(gonn).values()
          or "SITE_B" not in set(sync_center._zone_site(gonn).values()))
    check("B 仍被登记为已知站点", "SITE_B" in sync_center.known_sites(gonn))
    pkg_gone = sync_center.export_patch(gonn, "SITE_B", 1, 2)
    got_gone = {it["source_row"]: it["op"] for it in pkg_gone["items"]}
    check("B 收到其全部原地址的删除项",
          set(got_gone) == manual_sets()["SITE_B"][1]
          and set(got_gone.values()) == {"del"},
          f"{sorted(set(manual_sets()['SITE_B'][1]) - set(got_gone))[:5]}")
    check("已删区不产生任何新增项", "add" not in got_gone.values())
    check("删除项按 source_row 排序",
          [it["source_row"] for it in pkg_gone["items"]]
          == sorted(it["source_row"] for it in pkg_gone["items"]))
    # 一个 B 站点应用这份补丁：地址清空、版本推进
    gone_site = os.path.join(tmp, "gone_site.db")
    sync_site.init_db(gone_site).close()
    gs = sync_site.connect(gone_site)
    sync_site.apply_package(gs, sync_center.export_initial(gonn, "SITE_B", 1))
    r = sync_site.apply_package(gs, pkg_gone)
    check("B 站点应用删除补丁后版本推进到 rev2",
          r["revision"] == 2 and sync_site.read_state(gs)["address_count"] == 0,
          str(sync_site.read_state(gs)))
    # 再次导出同一区间：仍逐字节一致（已删区不影响重复导出）
    check("已删区后重复导出同一补丁逐字节一致",
          sync_center.package_text(pkg_gone)
          == sync_center.package_text(sync_center.export_patch(
              gonn, "SITE_B", 1, 2)))
    # 空补丁（rev2 -> rev2）仍可应用
    r = sync_site.apply_package(gs, sync_center.export_patch(gonn, "SITE_B", 2, 2))
    check("已删区后的空补丁仍可应用", r.get("applied") and r["revision"] == 2, str(r))
    gs.close(); gonn.close()

    section("7. 导出不随其他站点记录或空修订增长（查询计划 + SQL 追踪）")
    plan = conn.execute(
        "EXPLAIN QUERY PLAN SELECT source_row, sync_revision, op FROM sync_changes "
        "WHERE site_id=? AND sync_revision>? AND sync_revision<=?",
        ("SITE_A", 7, 13)).fetchall()
    plan_text = " ".join(r["detail"] for r in plan)
    print("  补丁折叠查询计划:", plan_text)
    check("区间查询走 idx_sync_changes_site_rev 索引",
          "idx_sync_changes_site_rev" in plan_text, plan_text)
    check("补丁折叠查询无全表扫描",
          "SCAN sync_changes" not in plan_text, plan_text)

    tracer = SqlTracer()
    hold = tracer.install(conn)
    pkg_long = sync_center.export_patch(conn, "SITE_A", 7, 13)
    conn.set_trace_callback(None)
    del hold
    check("追踪：折叠只发一条 sync_changes 区间查询",
          len([s for s in tracer.statements if "sync_changes" in s]) == 1,
          str([s for s in tracer.statements if "sync_changes" in s]))
    check("追踪：导出没有对 roster_addresses 的全表扫描",
          all("roster_addresses" not in s or "source_row IN" in s or "source_row=?" in s
              for s in tracer.statements), str(tracer.statements))
    check("追踪：导出没有对 roster_records 的全表扫描",
          all("roster_records" not in s or "source_row=?" in s
              for s in tracer.statements), str(tracer.statements))
    # 无关修订增长：只改 C 的中心库累积 25 个修订，A 的 1->26 仍是空补丁
    grow_db = os.path.join(tmp, "grow.db")
    build_center_db(grow_db)
    gconn = dispatch.connect(grow_db)
    sync_center.enable_sync(gconn)
    for i in range(25):
        dispatch.apply_changes(gconn, {"expected_revision": 1 + i,
                                       "changes": {"ZC": zone(6 + i, ring(*BING))}})
    p_empty = sync_center.export_patch(gconn, "SITE_A", 1, 26)
    check("其他站点累计 25 个修订后，A 的 1->26 仍是空补丁",
          p_empty["items"] == [], str(p_empty["items"]))
    # 空补丁可应用、能推进版本：站点 C 初装后连吃 25 个空补丁
    site_c_db = os.path.join(tmp, "site_c.db")
    sync_site.init_db(site_c_db).close()
    sc = sync_site.connect(site_c_db)
    sync_site.apply_package(sc, sync_center.export_initial(gconn, "SITE_C", 1))
    for rev in range(1, 26):
        r = sync_site.apply_package(sc, sync_center.export_patch(gconn, "SITE_C",
                                                                rev, rev + 1))
        assert r["applied"] and r["revision"] == rev + 1
    check("空补丁逐个应用后 C 推进到 rev26", sync_site.read_state(sc)["revision"] == 26)
    sc.close(); gconn.close()

    section("8. 站点 B 独立初装 + 跨版补丁")
    sync_site.init_db(site_b_db).close()
    sb = sync_site.connect(site_b_db)
    pkg_b = sync_center.export_initial(conn, "SITE_B", 1)
    check("B 初装包内容 == rev1 手写 B 集",
          {it["source_row"] for it in pkg_b["items"]} == MANUAL["SITE_B"][1])
    sync_site.apply_package(sb, pkg_b)
    res = sync_site.apply_package(sb, sync_center.export_patch(conn, "SITE_B", 1, 4))
    check("B 应用 1->4 -> rev4", res["revision"] == 4)
    check("B 站点地址 == 手写 rev4 B 集",
          {x["source_row"] for x in sync_site.read_addresses(sb)}
          == MANUAL["SITE_B"][4])
    res = sync_site.apply_package(sb, sync_center.export_patch(conn, "SITE_B", 4, 13))
    check("B 应用 4->13 -> rev13", res["revision"] == 13)
    check("B 站点地址 == 手写 rev13 B 集",
          {x["source_row"] for x in sync_site.read_addresses(sb)}
          == MANUAL["SITE_B"][13])
    sb.close()

    section("9. CLI：enable / export-initial / export-patch / apply / show")
    cli_db = os.path.join(tmp, "cli_center.db")
    build_center_db(cli_db)
    sites_file = os.path.join(tmp, "site_zones.json")
    json.dump(SITE_ZONES, open(sites_file, "w", encoding="utf-8"))
    cp = subprocess.run([sys.executable, os.path.join(HERE, "sync_center.py"),
                         "enable", cli_db, "--site-zones", sites_file],
                        capture_output=True, text=True)
    check("CLI enable 成功", cp.returncode == 0 and "source_id" in cp.stdout, cp.stderr)
    en_cli = json.loads(cp.stdout)
    out_init = os.path.join(pkgs, "init_a.json")
    cp = subprocess.run([sys.executable, os.path.join(HERE, "sync_center.py"),
                         "export-initial", cli_db, "SITE_A", "--target", "1",
                         "--out", out_init], capture_output=True, text=True)
    check("CLI export-initial 成功", cp.returncode == 0
          and os.path.exists(out_init), cp.stderr)
    cli_site = os.path.join(tmp, "cli_site.db")
    subprocess.run([sys.executable, os.path.join(HERE, "sync_site.py"),
                    "init", cli_site], capture_output=True, text=True, check=True)
    cp = subprocess.run([sys.executable, os.path.join(HERE, "sync_site.py"),
                         "apply", cli_site, out_init], capture_output=True, text=True)
    check("CLI site apply 初装包成功",
          cp.returncode == 0 and '"applied": true' in cp.stdout, cp.stderr)
    bf = os.path.join(tmp, "cli_batch.json")
    json.dump({"expected_revision": 1, "changes": {"M": m_at(NEAR_MOVE)}},
              open(bf, "w"))
    subprocess.run([sys.executable, os.path.join(HERE, "dispatch.py"),
                    "apply", cli_db, bf], capture_output=True, text=True, check=True)
    out_patch = os.path.join(pkgs, "patch_a_1_2.json")
    cp = subprocess.run([sys.executable, os.path.join(HERE, "sync_center.py"),
                         "export-patch", cli_db, "SITE_A", "--from", "1", "--to", "2",
                         "--out", out_patch], capture_output=True, text=True)
    check("CLI export-patch 成功", cp.returncode == 0, cp.stderr)
    cp = subprocess.run([sys.executable, os.path.join(HERE, "sync_site.py"),
                         "apply", cli_site, out_patch], capture_output=True, text=True)
    check("CLI site apply 补丁成功",
          cp.returncode == 0 and '"revision": 2' in cp.stdout, cp.stderr)
    cp = subprocess.run([sys.executable, os.path.join(HERE, "sync_site.py"),
                         "show", cli_site], capture_output=True, text=True)
    check("CLI site show 带发布源/站点/修订号/地址",
          cp.returncode == 0 and en_cli["source_id"] in cp.stdout
          and '"site_id": "SITE_A"' in cp.stdout and '"revision": 2' in cp.stdout
          and "R_A_1" in cp.stdout, cp.stdout[:400])
    cp = subprocess.run([sys.executable, os.path.join(HERE, "sync_center.py"),
                         "export-patch", cli_db, "SITE_A", "--from", "2", "--to", "1",
                         "--out", os.path.join(pkgs, "bad.json")],
                        capture_output=True, text=True)
    check("CLI 倒序导出非零退出", cp.returncode != 0)

    for c in (conn, other_conn, saconn):
        c.close()
    print(f"\n共 {N[0]} 项核对，失败 {len(FAILURES)} 项")
    if FAILURES:
        print("失败项：")
        for f in FAILURES:
            print(" -", f)
        sys.exit(1)
    print("全部通过。")


class SqlTracer:
    """收集连接执行过的 SQL（数据库追踪的等效观测）。"""

    def __init__(self):
        self.statements = []

    def install(self, conn):
        def cb(statement):
            self.statements.append(" ".join(statement.split()))
        conn.set_trace_callback(cb)
        return cb


if __name__ == "__main__":
    main()
