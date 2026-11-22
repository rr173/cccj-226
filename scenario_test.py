#!/usr/bin/env python3
"""场景测试：手工构造两个供应商的数据，逐项核对归并产品。

覆盖：接近/冲突坐标、版本升级只改第二候选、缺响应、重复 rank、无效坐标、
人工保留（继承/失效/重新确认）、同文本不同 source_row、幂等、失败快照、
快照只读、重启后快照身份不重复、接收与查询交错时旧快照仍可读。
"""
import json
import math
import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
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


# 独立参考实现（与 merger 无关），用于交叉核对距离
def ref_haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def cand(rank, addr, lat, lon, conf):
    return {"rank": rank, "normalized_address": addr, "lat": lat, "lon": lon,
            "confidence": conf}


def ingest(conn, sr, provider, version, cands, ts="2026-11-20T08:00:00+00:00"):
    return merger.ingest_response(conn, sr, provider, version, ts, cands)


def qtypes(conn, stage, sr, sid=None):
    if stage == "merge":
        return [r["qtype"] for r in conn.execute(
            "SELECT qtype FROM quarantine WHERE stage='merge' AND snapshot_id=? "
            "AND source_row=?", (sid, sr))]
    return [r["qtype"] for r in conn.execute(
        "SELECT qtype FROM quarantine WHERE stage='ingest' AND source_row=?", (sr,))]


def snap_view(g):
    """快照作用域视图：历史与全版本响应是活视图，不参与“旧快照不可变”的比对。"""
    return {"snapshot": g["snapshot"], "selection": g["selection"],
            "merge_quarantine": [q for q in g["quarantine"] if q["stage"] == "merge"]}


def verify_snapshot(conn, sid, expected, tag):
    """独立规则表：逐源行核对选择、冲突与变化原因。"""
    for sr, e in expected.items():
        sel = conn.execute("SELECT * FROM selections WHERE snapshot_id=? AND source_row=?",
                           (sid, sr)).fetchone()
        check(f"{tag}/{sr}: 有选择记录", sel is not None)
        if sel is None:
            continue
        n0 = N_CHECKS[0]
        check(f"{tag}/{sr}: decision", sel["decision"] == e["decision"],
              f"expect {e['decision']} got {sel['decision']}")
        if e["selected"] is None:
            check(f"{tag}/{sr}: 无选中候选", sel["sel_provider"] is None,
                  f"got {sel['sel_provider']}@{sel['sel_response_version']}#{sel['sel_rank']}")
        else:
            got = (sel["sel_provider"], sel["sel_response_version"], sel["sel_rank"])
            check(f"{tag}/{sr}: 候选身份", got == e["selected"],
                  f"expect {e['selected']} got {got}")
        expl = json.loads(sel["explanation_json"])
        if e.get("applied"):
            check(f"{tag}/{sr}: applied_rule", expl["applied_rule"] == e["applied"],
                  f"expect {e['applied']} got {expl['applied_rule']}")
        if e.get("change"):
            check(f"{tag}/{sr}: change_flag", sel["change_flag"] == e["change"],
                  f"expect {e['change']} got {sel['change_flag']} ({sel['change_reason']})")
        for sub in e.get("reason_has", []):
            check(f"{tag}/{sr}: 变化原因含 {sub!r}", sub in sel["change_reason"],
                  f"got {sel['change_reason']}")
        for sub in e.get("pending_has", []):
            check(f"{tag}/{sr}: 未决原因含 {sub!r}",
                  any(sub in p for p in json.loads(sel["pending_reasons_json"])),
                  f"got {sel['pending_reasons_json']}")
        mq = qtypes(conn, "merge", sr, sid)
        for q in e.get("merge_q", []):
            check(f"{tag}/{sr}: 合并期隔离含 {q}", q in mq, f"got {mq}")
        iq = qtypes(conn, "ingest", sr)
        for q in e.get("ingest_q", []):
            check(f"{tag}/{sr}: 接收期隔离含 {q}", q in iq, f"got {iq}")
        for key in ("selected", "comparison", "considered", "eliminated",
                    "pending_reasons"):
            check(f"{tag}/{sr}: 解释含字段 {key}", key in expl)
        print(f"  {tag}/{sr}: OK ({N_CHECKS[0] - n0} 项核对通过)")


def main():
    tmp = tempfile.mkdtemp(prefix="merger_test_")
    db = os.path.join(tmp, "merger.db")
    conn = merger.init_db(db)

    section("1. 接收源行（含同文本不同 source_row）与规则")
    rows = [(f"S{i}", addr) for i, addr in enumerate([
        "100 Main St", "200 Oak Ave", "300 Pine Rd", "400 Elm St", "500 Birch Blvd",
        "600 Cedar Ct", "700 Walnut St", "700 Walnut St", "900 Lake Dr"], start=1)]
    merger.ingest_source_rows(conn, "B1", rows)
    n = conn.execute("SELECT COUNT(*) c FROM source_rows WHERE raw_address='700 Walnut St'").fetchone()["c"]
    check("同文本不同 source_row 不合并", n == 2, f"got {n}")
    merger.register_rule(conn, "r1", preferred_provider="provA",
                         distance_threshold_m=100.0, confidence_diff_threshold=0.15,
                         holds=[{"source_row": "S6", "provider": "provA",
                                 "response_version": "v1", "rank": 2,
                                 "note": "2026-11-01 人工实地核实"}])
    check("规则重复登记同内容幂等",
          merger.register_rule(conn, "r1", preferred_provider="provA",
                               distance_threshold_m=100.0, confidence_diff_threshold=0.15,
                               holds=[{"source_row": "S6", "provider": "provA",
                                       "response_version": "v1", "rank": 2,
                                       "note": "2026-11-01 人工实地核实"}]) is False)
    try:
        merger.register_rule(conn, "r1", distance_threshold_m=1.0)
        check("规则版本内容不可变", False, "未报错")
    except ValueError:
        check("规则版本内容不可变", True)

    section("2. 接收 v1 响应（含重复 rank / 无效坐标 / 不完整响应 / 缺响应）")
    ingest(conn, "S1", "provA", "v1", [cand(1, "100 Main St, Springfield", 40.0, -75.0, 0.90),
                                       cand(2, "100 Main St Rear", 40.01, -75.01, 0.60)])
    ingest(conn, "S1", "provB", "v1", [cand(1, "100 Main St, Springfield", 40.00005, -75.00004, 0.85),
                                       cand(2, "100 Main St Annex", 40.02, -75.02, 0.40)])
    ingest(conn, "S2", "provA", "v1", [cand(1, "200 Oak Ave", 40.1, -75.1, 0.80),
                                       cand(2, "200 Oak Ave Unit B", 40.101, -75.101, 0.50)])
    ingest(conn, "S2", "provB", "v1", [cand(1, "200 Oak Ave", 40.102, -75.103, 0.78)])
    ingest(conn, "S3", "provA", "v1", [cand(1, "300 Pine Rd", 40.2, -75.2, 0.88)])
    # S3 provB：缺响应（不接收）
    ingest(conn, "S4", "provA", "v1", [cand(1, "400 Elm St", 40.3, -75.3, 0.90),
                                       cand(1, "400 Elm St Ste 2", 40.301, -75.301, 0.70)])  # 重复 rank
    ingest(conn, "S4", "provB", "v1", [cand(1, "400 Elm St", 40.3005, -75.3005, 0.80)])
    ingest(conn, "S5", "provA", "v1", [cand(1, "500 Birch Blvd", 91.5, -75.4, 0.95),   # 无效坐标
                                       cand(2, "500 Birch Blvd", 40.4, -75.4, 0.75)])
    ingest(conn, "S5", "provB", "v1", [cand(1, "500 Birch Blvd", 40.40004, -75.40003, 0.80)])
    ingest(conn, "S6", "provA", "v1", [cand(1, "600 Cedar Ct", 40.5, -75.5, 0.95),
                                       cand(2, "600 Cedar Ct (verified)", 40.505, -75.505, 0.55)])
    ingest(conn, "S6", "provB", "v1", [cand(1, "600 Cedar Ct", 40.5002, -75.5001, 0.90)])
    ingest(conn, "S7", "provA", "v1", [cand(1, "700 Walnut St", 40.6, -75.6, 0.90)])
    ingest(conn, "S7", "provB", "v1", [cand(1, "700 Walnut St", 40.60003, -75.60002, 0.88)])
    ingest(conn, "S8", "provA", "v1", [cand(1, "700 Walnut St", 41.6, -76.6, 0.90)])
    ingest(conn, "S8", "provB", "v1", [{"rank": 1, "normalized_address": "700 Walnut St",
                                        "lat": 41.6, "lon": -76.6}])  # 缺 confidence -> 不完整
    ingest(conn, "S9", "provA", "v1", [cand(1, "900 Lake Dr", 40.7, -75.7, 0.92),
                                       cand(2, "900 Lake Dr Dock", 40.711, -75.711, 0.50)])
    ingest(conn, "S9", "provB", "v1", [cand(1, "900 Lake Dr", 40.70006, -75.70005, 0.87)])

    section("3. 校验接口")
    iss4 = merger.validate_response(conn, "S4", "provA", "v1")
    check("校验发现重复 rank", any(i["qtype"] == "duplicate_rank" for i in iss4), str(iss4))
    iss5 = merger.validate_response(conn, "S5", "provA", "v1")
    check("校验发现无效坐标", any(i["qtype"] == "invalid_coordinate" for i in iss5), str(iss5))
    iss8 = merger.validate_response(conn, "S8", "provB", "v1")
    check("校验发现响应不完整", any(i["qtype"] == "incomplete_response" for i in iss8), str(iss8))
    check("干净响应校验通过", merger.validate_response(conn, "S1", "provA", "v1") == [])
    rid1, again = merger.ingest_response(conn, "S1", "provA", "v1", "2026-11-20T08:00:00+00:00",
                                         [cand(1, "100 Main St, Springfield", 40.0, -75.0, 0.90),
                                          cand(2, "100 Main St Rear", 40.01, -75.01, 0.60)])
    check("相同响应重送幂等", again is False)
    try:
        merger.ingest_response(conn, "S1", "provA", "v1", "2026-11-20T09:00:00+00:00",
                               [cand(1, "X", 40.0, -75.0, 0.9)])
        check("同版本不同内容被拒绝", False, "未报错")
    except ValueError:
        check("同版本不同内容被拒绝", True)

    section("4. 归并 -> 快照1（provA=v1, provB=v1, r1），逐项核对")
    s1 = merger.create_snapshot(conn, "B1", {"provA": "v1", "provB": "v1"}, "r1")
    snap1 = s1["snapshot_id"]
    check("快照1为新快照", s1["replayed"] is False)
    EXPECT1 = {
        "S1": dict(decision="auto", selected=("provA", "v1", 1), applied="providers_agree", change="added"),
        "S2": dict(decision="pending", selected=None, applied="rule_conflict", change="added",
                   pending_has=["rule_conflict"], merge_q=["rule_conflict"]),
        "S3": dict(decision="auto", selected=("provA", "v1", 1), applied="single_provider", change="added",
                   pending_has=["provider_unavailable:provB"], merge_q=["incomplete_response"]),
        "S4": dict(decision="auto", selected=("provB", "v1", 1), applied="single_provider", change="added",
                   pending_has=["provider_unavailable:provA"], merge_q=["incomplete_response"],
                   ingest_q=["duplicate_rank"]),
        "S5": dict(decision="auto", selected=("provA", "v1", 2), applied="providers_agree", change="added",
                   ingest_q=["invalid_coordinate"]),
        "S6": dict(decision="hold", selected=("provA", "v1", 2), applied="human_hold", change="added"),
        "S7": dict(decision="auto", selected=("provA", "v1", 1), applied="providers_agree", change="added"),
        "S8": dict(decision="auto", selected=("provA", "v1", 1), applied="single_provider", change="added",
                   pending_has=["provider_unavailable:provB"], ingest_q=["incomplete_response"]),
        "S9": dict(decision="auto", selected=("provA", "v1", 1), applied="providers_agree", change="added"),
    }
    verify_snapshot(conn, snap1, EXPECT1, "snap1")

    # 解释内容抽查：比较字段 / 被淘汰候选 / 未决原因
    e1 = json.loads(conn.execute("SELECT explanation_json FROM selections WHERE snapshot_id=? "
                                 "AND source_row='S1'", (snap1,)).fetchone()[0])
    d_ref = ref_haversine_m(40.0, -75.0, 40.00005, -75.00004)
    check("snap1/S1 距离与独立计算一致",
          abs(e1["comparison"]["max_pairwise_distance_m"] - d_ref) < 0.5,
          f"{e1['comparison']['max_pairwise_distance_m']} vs {d_ref}")
    elim = {x["identity"]: x["reason"] for x in e1["eliminated"]}
    check("snap1/S1 被淘汰候选及原因",
          elim.get("provB@v1#1") == "not_preferred_provider" and
          elim.get("provA@v1#2") == "lower_rank", str(elim))
    e6 = json.loads(conn.execute("SELECT explanation_json FROM selections WHERE snapshot_id=? "
                                 "AND source_row='S6'", (snap1,)).fetchone()[0])
    check("snap1/S6 保留已适用", e6["hold"]["status"] == "applied")
    check("snap1/S6 其余候选均 not_held",
          all(x["reason"] == "not_held" for x in e6["eliminated"]) and len(e6["eliminated"]) == 2)
    e2 = json.loads(conn.execute("SELECT explanation_json FROM selections WHERE snapshot_id=? "
                                 "AND source_row='S2'", (snap1,)).fetchone()[0])
    d2_ref = ref_haversine_m(40.1, -75.1, 40.102, -75.103)
    check("snap1/S2 冲突距离与独立计算一致",
          abs(e2["comparison"]["max_pairwise_distance_m"] - d2_ref) < 0.5,
          f"{e2['comparison']['max_pairwise_distance_m']} vs {d2_ref}")
    check("snap1/S2 未决时无选中候选", e2["selected"] is None)

    section("5. 查询接口 + 同文本不同源行相互独立")
    g7, g8 = merger.get_source_row(conn, "S7", snap1), merger.get_source_row(conn, "S8", snap1)
    check("S7/S8 原始文本相同", g7["raw_address"] == g8["raw_address"] == "700 Walnut St")
    check("S7/S8 选择相互独立",
          g7["selection"]["selected"]["lat"] == 40.6 and g8["selection"]["selected"]["lat"] == 41.6)
    check("查询含历史与响应来源", len(g7["responses"]) == 2 and len(g7["history"]) == 1)

    section("6. 导出待人工处理清单（带来源与规则证据）")
    exp1 = merger.export_pending(conn, snap1)
    got_rows = {i["source_row"] for i in exp1["items"]}
    check("snap1 待处理集合", got_rows == {"S2", "S3", "S4", "S5", "S8"}, str(got_rows))
    check("干净的人工保留不在待处理中", "S6" not in got_rows)
    it2 = next(i for i in exp1["items"] if i["source_row"] == "S2")
    check("导出来源证据", it2["provenance"]["provider_versions"] == {"provA": "v1", "provB": "v1"})
    check("导出规则证据", it2["rule_evidence"]["rule_version"] == "r1"
          and it2["rule_evidence"]["distance_threshold_m"] == 100.0
          and it2["rule_evidence"]["applied_rule"] == "rule_conflict")
    check("导出非仅经纬度", len(it2["candidates"]) >= 2
          and all("returned_at" in c and "identity" in c for c in it2["candidates"]))
    check("导出含隔离明细", any(q["qtype"] == "rule_conflict" for q in it2["quarantine"]["merge"]))
    it5 = next(i for i in exp1["items"] if i["source_row"] == "S5")
    check("导出含接收期隔离（无效坐标）",
          any(q["qtype"] == "invalid_coordinate" for q in it5["quarantine"]["ingest"]))

    section("7. 幂等重送")
    s1b = merger.create_snapshot(conn, "B1", {"provA": "v1", "provB": "v1"}, "r1")
    check("相同输入重送返回同一快照", s1b["replayed"] is True and s1b["snapshot_id"] == snap1)
    n_ok = conn.execute("SELECT COUNT(*) c FROM snapshots WHERE status='ok'").fetchone()["c"]
    check("幂等重送不产生新快照", n_ok == 1, f"got {n_ok}")

    # 交错点 A：升级前先查询旧快照并存证
    before_s9 = merger.get_source_row(conn, "S9", snap1)
    before_s6 = merger.get_source_row(conn, "S6", snap1)

    section("8. 接收 provA v2（S9 仅第二候选变化；S4 修复重复 rank；S5 修复坐标）")
    ingest(conn, "S1", "provA", "v2", [cand(1, "100 Main St, Springfield", 40.0, -75.0, 0.90),
                                       cand(2, "100 Main St Rear", 40.01, -75.01, 0.60)],
           "2026-11-21T08:00:00+00:00")
    ingest(conn, "S2", "provA", "v2", [cand(1, "200 Oak Ave", 40.1, -75.1, 0.80),
                                       cand(2, "200 Oak Ave Unit B", 40.101, -75.101, 0.50)],
           "2026-11-21T08:00:00+00:00")
    ingest(conn, "S3", "provA", "v2", [cand(1, "300 Pine Rd", 40.2, -75.2, 0.88)],
           "2026-11-21T08:00:00+00:00")
    ingest(conn, "S4", "provA", "v2", [cand(1, "400 Elm St", 40.3, -75.3, 0.90),
                                       cand(2, "400 Elm St Ste 2", 40.301, -75.301, 0.70)],
           "2026-11-21T08:00:00+00:00")
    ingest(conn, "S5", "provA", "v2", [cand(1, "500 Birch Blvd", 40.4, -75.4, 0.90),
                                       cand(2, "500 Birch Blvd Lot 3", 40.41, -75.41, 0.50)],
           "2026-11-21T08:00:00+00:00")
    ingest(conn, "S6", "provA", "v2", [cand(1, "600 Cedar Ct", 40.5, -75.5, 0.95),
                                       cand(2, "600 Cedar Ct (verified)", 40.505, -75.505, 0.55)],
           "2026-11-21T08:00:00+00:00")
    ingest(conn, "S7", "provA", "v2", [cand(1, "700 Walnut St", 40.6, -75.6, 0.90)],
           "2026-11-21T08:00:00+00:00")
    ingest(conn, "S8", "provA", "v2", [cand(1, "700 Walnut St", 41.6, -76.6, 0.90)],
           "2026-11-21T08:00:00+00:00")
    ingest(conn, "S9", "provA", "v2", [cand(1, "900 Lake Dr", 40.7, -75.7, 0.92),          # 第一候选不变
                                       cand(2, "900 Lake Dr Pier 2", 40.722, -75.722, 0.45)],  # 仅第二候选变
           "2026-11-21T08:00:00+00:00")

    # 交错点 B：接收 v2 后、归并前，旧快照仍可读且内容不变
    check("接收新数据后旧快照仍可读", snap_view(merger.get_source_row(conn, "S9", snap1)) == snap_view(before_s9))

    section("9. 模拟进程退出重开：快照身份不重复分配")
    conn.close()
    conn = merger.connect(db)
    check("重开后最新可读快照仍是 snap1", merger.latest_snapshot_id(conn, "B1") == snap1)

    section("10. 失败快照不替换可读版本")
    try:
        merger.create_snapshot(conn, "B1", {"provA": "v2", "provB": "v1"}, "rX")
        check("未知规则版本应失败", False, "未报错")
    except ValueError:
        check("未知规则版本应失败", True)
    failed = conn.execute("SELECT * FROM snapshots WHERE status='failed'").fetchall()
    check("失败快照已留痕", len(failed) == 1 and "rX" in (failed[0]["error"] or ""))
    check("失败后最新可读快照未被替换", merger.latest_snapshot_id(conn, "B1") == snap1)
    check("失败快照不可读", _raises(conn, failed[0]["snapshot_id"]))

    section("11. 归并 -> 快照2（provA=v2, provB=v1, r1），逐项核对")
    s2 = merger.create_snapshot(conn, "B1", {"provA": "v2", "provB": "v1"}, "r1")
    snap2 = s2["snapshot_id"]
    check("版本变化产生新快照身份", s2["replayed"] is False and snap2 > snap1)
    EXPECT2 = {
        "S1": dict(decision="auto", selected=("provA", "v2", 1), applied="providers_agree",
                   change="changed", reason_has=["provider_version_change:provA:v1->v2",
                                                 "selected_value_unchanged"]),
        "S2": dict(decision="pending", selected=None, applied="rule_conflict", change="unchanged",
                   pending_has=["rule_conflict"], merge_q=["rule_conflict"]),
        "S3": dict(decision="auto", selected=("provA", "v2", 1), applied="single_provider",
                   change="changed", reason_has=["selected_value_unchanged"],
                   pending_has=["provider_unavailable:provB"]),
        "S4": dict(decision="auto", selected=("provA", "v2", 1), applied="providers_agree",
                   change="changed", reason_has=["selected_value_changed"]),
        "S5": dict(decision="auto", selected=("provA", "v2", 1), applied="providers_agree",
                   change="changed", reason_has=["selected_value_unchanged"]),
        "S6": dict(decision="auto", selected=("provA", "v2", 1), applied="providers_agree",
                   change="changed", reason_has=["hold_superseded"],
                   pending_has=["hold_superseded"]),
        "S7": dict(decision="auto", selected=("provA", "v2", 1), applied="providers_agree",
                   change="changed", reason_has=["selected_value_unchanged"]),
        "S8": dict(decision="auto", selected=("provA", "v2", 1), applied="single_provider",
                   change="changed", pending_has=["provider_unavailable:provB"]),
        "S9": dict(decision="auto", selected=("provA", "v2", 1), applied="providers_agree",
                   change="changed", reason_has=["selected_value_unchanged"]),
    }
    verify_snapshot(conn, snap2, EXPECT2, "snap2")
    # 候选身份由版本+序号决定：S9 值未变但身份已变
    id1 = conn.execute("SELECT sel_provider,sel_response_version,sel_rank FROM selections "
                       "WHERE snapshot_id=? AND source_row='S9'", (snap1,)).fetchone()
    id2 = conn.execute("SELECT sel_provider,sel_response_version,sel_rank FROM selections "
                       "WHERE snapshot_id=? AND source_row='S9'", (snap2,)).fetchone()
    check("S9 候选身份随版本变化", tuple(id1) != tuple(id2), f"{tuple(id1)} vs {tuple(id2)}")
    check("S9 选中值未变（仅第二候选变化）",
          conn.execute("SELECT sel_lat,sel_lon FROM selections WHERE snapshot_id=? AND source_row='S9'",
                       (snap1,)).fetchone()[:] ==
          conn.execute("SELECT sel_lat,sel_lon FROM selections WHERE snapshot_id=? AND source_row='S9'",
                       (snap2,)).fetchone()[:])
    # 人工保留不静默沿用：S6 在快照2中回退为规则选择并挂起
    g6_s2 = merger.get_source_row(conn, "S6", snap2)
    check("S6 保留失效后未沿用旧选择",
          g6_s2["selection"]["selected_identity"] == "provA@v2#1"
          and g6_s2["selection"]["decision"] == "auto")
    check("S6 保留失效进入待处理",
          "S6" in {i["source_row"] for i in merger.export_pending(conn, snap2)["items"]})
    exp2_rows = {i["source_row"] for i in merger.export_pending(conn, snap2)["items"]}
    check("snap2 待处理集合", exp2_rows == {"S2", "S3", "S6", "S8"}, str(exp2_rows))

    section("12. 比较快照1 vs 快照2")
    cmp12 = merger.compare_snapshots(conn, snap1, snap2)
    check("比较含版本差异", "provA:v1->v2" in cmp12["version_diffs"], str(cmp12["version_diffs"]))
    check("S9 位移为 0（仅第二候选变化）",
          cmp12["rows"]["S9"]["change"] == "changed"
          and cmp12["rows"]["S9"]["coordinate_shift_m"] == 0.0)
    check("S6 标记为变化", cmp12["rows"]["S6"]["change"] == "changed")
    check("S2 两版均未决 -> unchanged", cmp12["rows"]["S2"]["change"] == "unchanged")
    s4_shift = cmp12["rows"]["S4"]["coordinate_shift_m"]
    check("S4 位移与独立计算一致",
          abs(s4_shift - ref_haversine_m(40.3005, -75.3005, 40.3, -75.3)) < 0.5, str(s4_shift))
    tagmap = {"added_in_b": "added", "removed_in_b": "removed"}
    consistent = all(
        (tagmap.get(v["change"], v["change"])) ==
        conn.execute("SELECT change_flag FROM selections WHERE snapshot_id=? AND source_row=?",
                     (snap2, sr)).fetchone()["change_flag"]
        for sr, v in cmp12["rows"].items())
    check("比较结果与快照2存储的变化标记一致", consistent)

    # 交错点 C：新快照生成后，旧快照内容保持原样
    check("新快照生成后旧快照仍可读且未变", snap_view(merger.get_source_row(conn, "S9", snap1)) == snap_view(before_s9))
    check("旧快照中的人工保留未被新数据覆盖", snap_view(merger.get_source_row(conn, "S6", snap1)) == snap_view(before_s6))

    section("13. 人工保留重新确认 -> 规则 r2 -> 快照3")
    merger.register_rule(conn, "r2", preferred_provider="provA",
                         distance_threshold_m=100.0, confidence_diff_threshold=0.15,
                         holds=[{"source_row": "S6", "provider": "provA",
                                 "response_version": "v2", "rank": 2,
                                 "note": "v2 到达后人工重新确认"}])
    s3 = merger.create_snapshot(conn, "B1", {"provA": "v2", "provB": "v1"}, "r2")
    snap3 = s3["snapshot_id"]
    EXPECT3 = {
        "S1": dict(decision="auto", selected=("provA", "v2", 1), change="unchanged"),
        "S2": dict(decision="pending", selected=None, change="unchanged"),
        "S3": dict(decision="auto", selected=("provA", "v2", 1), change="unchanged"),
        "S4": dict(decision="auto", selected=("provA", "v2", 1), change="unchanged"),
        "S5": dict(decision="auto", selected=("provA", "v2", 1), change="unchanged"),
        "S6": dict(decision="hold", selected=("provA", "v2", 2), applied="human_hold",
                   change="changed", reason_has=["rule_change:r1->r2", "hold_applied"]),
        "S7": dict(decision="auto", selected=("provA", "v2", 1), change="unchanged"),
        "S8": dict(decision="auto", selected=("provA", "v2", 1), change="unchanged"),
        "S9": dict(decision="auto", selected=("provA", "v2", 1), change="unchanged"),
    }
    verify_snapshot(conn, snap3, EXPECT3, "snap3")
    cmp23 = merger.compare_snapshots(conn, snap2, snap3)
    check("快照2->3 仅 S6 变化",
          cmp23["summary"].get("changed") == 1 and cmp23["rows"]["S6"]["change"] == "changed",
          str(cmp23["summary"]))

    section("14. provB 升级到 v2 -> 快照4：人工保留跨快照继承")
    for sr, c in [("S1", cand(1, "100 Main St, Springfield", 40.00005, -75.00004, 0.85)),
                  ("S2", cand(1, "200 Oak Ave", 40.102, -75.103, 0.78)),
                  ("S3", cand(1, "300 Pine Rd", 40.20005, -75.20004, 0.83)),
                  ("S4", cand(1, "400 Elm St", 40.3005, -75.3005, 0.80)),
                  ("S5", cand(1, "500 Birch Blvd", 40.40004, -75.40003, 0.80)),
                  ("S6", cand(1, "600 Cedar Ct", 40.5002, -75.5001, 0.90)),
                  ("S7", cand(1, "700 Walnut St", 40.60003, -75.60002, 0.88)),
                  ("S8", cand(1, "700 Walnut St", 41.60005, -76.60004, 0.85)),
                  ("S9", cand(1, "900 Lake Dr", 40.70006, -75.70005, 0.87))]:
        ingest(conn, sr, "provB", "v2", [c], "2026-11-22T08:00:00+00:00")
    s4 = merger.create_snapshot(conn, "B1", {"provA": "v2", "provB": "v2"}, "r2")
    snap4 = s4["snapshot_id"]
    EXPECT4 = {
        "S1": dict(decision="auto", selected=("provA", "v2", 1), change="unchanged"),
        "S2": dict(decision="pending", selected=None, change="unchanged",
                   pending_has=["rule_conflict"], merge_q=["rule_conflict"]),
        "S3": dict(decision="auto", selected=("provA", "v2", 1), applied="providers_agree",
                   change="unchanged"),
        "S4": dict(decision="auto", selected=("provA", "v2", 1), change="unchanged"),
        "S5": dict(decision="auto", selected=("provA", "v2", 1), change="unchanged"),
        "S6": dict(decision="hold", selected=("provA", "v2", 2), applied="human_hold",
                   change="inherited", reason_has=["hold_carried_over"]),
        "S7": dict(decision="auto", selected=("provA", "v2", 1), change="unchanged"),
        "S8": dict(decision="auto", selected=("provA", "v2", 1), applied="providers_agree",
                   change="unchanged"),
        "S9": dict(decision="auto", selected=("provA", "v2", 1), change="unchanged"),
    }
    verify_snapshot(conn, snap4, EXPECT4, "snap4")
    cmp34 = merger.compare_snapshots(conn, snap3, snap4)
    check("快照3->4 S6 标记为继承", cmp34["rows"]["S6"]["change"] == "inherited")
    check("快照3->4 其余行不变",
          all(v["change"] == "unchanged" for k, v in cmp34["rows"].items() if k != "S6"))
    exp4_rows = {i["source_row"] for i in merger.export_pending(conn, snap4)["items"]}
    check("snap4 待处理只剩 S2（其余问题已随版本修复）", exp4_rows == {"S2"}, str(exp4_rows))

    section("15. 历史快照只读 & 新快照不可冒充旧结果")
    for sql, args, label in [
        ("UPDATE snapshots SET status='failed' WHERE snapshot_id=?", (snap1,), "改快照"),
        ("DELETE FROM selections WHERE snapshot_id=?", (snap1,), "删选择"),
        ("UPDATE rules SET distance_threshold_m=1 WHERE rule_version='r1'", (), "改规则"),
        ("DELETE FROM quarantine", (), "删隔离"),
    ]:
        try:
            conn.execute(sql, args)
            conn.rollback()
            check(f"只读保护生效（{label}）", False, "未报错")
        except sqlite3.Error:
            conn.rollback()
            check(f"只读保护生效（{label}）", True)
    fp1 = conn.execute("SELECT fingerprint FROM snapshots WHERE snapshot_id=?",
                       (snap1,)).fetchone()["fingerprint"]
    try:
        conn.execute("INSERT INTO snapshots(fingerprint,batch_id,rule_version,"
                     "provider_versions_json,status,created_at) VALUES(?,?,?,?, 'ok', 'x')",
                     (fp1, "B1", "r1", "{}"))
        conn.rollback()
        check("伪造同指纹快照被拒绝", False, "未报错")
    except sqlite3.Error:
        conn.rollback()
        check("伪造同指纹快照被拒绝", True)
    check("一切操作后旧快照内容仍未变", snap_view(merger.get_source_row(conn, "S9", snap1)) == snap_view(before_s9))
    check("旧快照选择结果未被冒充",
          merger.get_source_row(conn, "S9", snap1)["selection"]["selected_identity"] == "provA@v1#1")

    section("16. 再次重启：身份连续、幂等依旧")
    conn.close()
    conn = merger.connect(db)
    ids = [r["snapshot_id"] for r in
           conn.execute("SELECT snapshot_id FROM snapshots WHERE status='ok' ORDER BY snapshot_id")]
    check("快照身份单调且不重号", ids == sorted(ids) and len(ids) == len(set(ids)) == 4, str(ids))
    check("重开后最新可读快照为 snap4", merger.latest_snapshot_id(conn, "B1") == snap4)
    s1c = merger.create_snapshot(conn, "B1", {"provA": "v1", "provB": "v1"}, "r1")
    check("重开后旧输入仍幂等返回 snap1", s1c["replayed"] is True and s1c["snapshot_id"] == snap1)
    check("重开后旧快照仍可读", snap_view(merger.get_source_row(conn, "S9", snap1)) == snap_view(before_s9))

    section("17. 产品证据抽样")
    it2 = next(i for i in merger.export_pending(conn, snap1)["items"] if i["source_row"] == "S2")
    print(json.dumps({"导出待处理/S2": it2}, ensure_ascii=False, indent=2)[:1600])
    g6 = merger.get_source_row(conn, "S6")
    print(json.dumps({"S6 跨快照历史": g6["history"]}, ensure_ascii=False, indent=2))

    print(f"\n共 {N_CHECKS[0]} 项核对，失败 {len(FAILURES)} 项")
    if FAILURES:
        print("失败项：")
        for f in FAILURES:
            print(" -", f)
        sys.exit(1)
    print("全部通过。")


def _raises(conn, sid):
    try:
        merger._get_ok_snapshot(conn, sid)
        return False
    except ValueError:
        return True


if __name__ == "__main__":
    main()
