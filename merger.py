"""候选快照归并器（geocoding candidate snapshot merger）。

对每个原始地址（稳定 source_row）保存各供应商在指定版本下的候选集合、
可解释的选择结果与未决冲突；人工选择只作为随规则版本声明的“保留（hold）”，
不会被固化为永久真值。

核心不变量
----------
1. 候选身份 = (provider, response_version, rank)。地址文本相同不合并 source_row。
2. 归并请求 = (batch_id, provider_versions, rule_version)。
   指纹相同 -> 幂等返回旧快照；任一版本变化 -> 新快照。历史快照只读。
3. 失败快照仅留痕（status='failed'），永远不会替换上一个可读快照。
4. 人工保留只针对明确的 source_row 与候选身份；供应商版本变化后重新计算，
   并标记 inherited（继承）或 changed（变化），不静默沿用旧选择。
5. 规则冲突、候选坐标不合法、同版本重复序号、供应商响应不完整 -> 隔离（quarantine）。
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import math
import sqlite3
import sys

EARTH_R_M = 6371000.0
# 响应级隔离：命中即整个响应不可用；invalid_coordinate 只隔离单个候选
BLOCKING_INGEST_QTYPES = ("incomplete_response", "duplicate_rank")

SCHEMA = """
CREATE TABLE IF NOT EXISTS source_rows (
  source_row   TEXT PRIMARY KEY,
  batch_id     TEXT NOT NULL,
  raw_address  TEXT NOT NULL,
  created_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS responses (
  response_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  source_row       TEXT NOT NULL REFERENCES source_rows(source_row),
  provider         TEXT NOT NULL,
  response_version TEXT NOT NULL,
  returned_at      TEXT NOT NULL,
  payload_hash     TEXT NOT NULL,
  received_at      TEXT NOT NULL,
  UNIQUE (source_row, provider, response_version)
);
CREATE TABLE IF NOT EXISTS candidates (
  candidate_id       INTEGER PRIMARY KEY AUTOINCREMENT,
  response_id        INTEGER NOT NULL REFERENCES responses(response_id),
  rank               INTEGER NOT NULL,
  normalized_address TEXT,
  lat                REAL,
  lon                REAL,
  confidence         REAL
);
CREATE TABLE IF NOT EXISTS quarantine (
  quarantine_id    INTEGER PRIMARY KEY AUTOINCREMENT,
  stage            TEXT NOT NULL CHECK (stage IN ('ingest','merge')),
  snapshot_id      INTEGER,
  source_row       TEXT NOT NULL,
  provider         TEXT,
  response_version TEXT,
  rank             INTEGER,
  qtype            TEXT NOT NULL CHECK (qtype IN
    ('incomplete_response','invalid_coordinate','duplicate_rank','rule_conflict')),
  detail           TEXT NOT NULL,
  created_at       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rules (
  rule_version              TEXT PRIMARY KEY,
  preferred_provider        TEXT,
  distance_threshold_m      REAL NOT NULL,
  confidence_diff_threshold REAL NOT NULL,
  holds_json                TEXT NOT NULL DEFAULT '[]',
  created_at                TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS snapshots (
  snapshot_id            INTEGER PRIMARY KEY AUTOINCREMENT,
  fingerprint            TEXT NOT NULL,
  batch_id               TEXT NOT NULL,
  rule_version           TEXT NOT NULL,
  provider_versions_json TEXT NOT NULL,
  status                 TEXT NOT NULL CHECK (status IN ('ok','failed')),
  error                  TEXT,
  created_at             TEXT NOT NULL
);
-- 幂等只针对可读快照；失败快照不占指纹、可重试
CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_ok_fingerprint
  ON snapshots(fingerprint) WHERE status = 'ok';
CREATE TABLE IF NOT EXISTS selections (
  snapshot_id            INTEGER NOT NULL REFERENCES snapshots(snapshot_id),
  source_row             TEXT NOT NULL,
  decision               TEXT NOT NULL CHECK (decision IN ('auto','hold','pending')),
  sel_provider           TEXT,
  sel_response_version   TEXT,
  sel_rank               INTEGER,
  sel_normalized_address TEXT,
  sel_lat                REAL,
  sel_lon                REAL,
  sel_confidence         REAL,
  change_flag            TEXT NOT NULL,
  change_reason          TEXT NOT NULL,
  pending_reasons_json   TEXT NOT NULL DEFAULT '[]',
  explanation_json       TEXT NOT NULL,
  PRIMARY KEY (snapshot_id, source_row)
);
"""

READONLY_TABLES = ("source_rows", "responses", "candidates", "quarantine",
                   "rules", "snapshots", "selections")


# ---------------------------------------------------------------- 基础工具

def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def _canon(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _is_num(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_R_M * math.asin(math.sqrt(a))


def ident(provider, version, rank) -> str:
    """候选身份字符串：供应商@版本#序号。"""
    return f"{provider}@{version}#{rank}"


def connect(path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(path) -> sqlite3.Connection:
    conn = connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    # 历史数据只读：禁止 UPDATE/DELETE（快照、选择、响应、规则、隔离记录）
    for t in READONLY_TABLES:
        for op in ("UPDATE", "DELETE"):
            conn.execute(
                f"CREATE TRIGGER IF NOT EXISTS trg_{t}_no_{op.lower()} "
                f"BEFORE {op} ON {t} BEGIN "
                f"SELECT RAISE(ABORT, '{t}: historical records are read-only'); END;")
    conn.commit()
    return conn


# ---------------------------------------------------------------- 接收 + 校验

def ingest_source_rows(conn, batch_id, rows) -> int:
    """rows: [(source_row, raw_address)]。同内容重送幂等；同 source_row 不同内容报错。"""
    inserted = 0
    with conn:
        for source_row, raw_address in rows:
            ex = conn.execute("SELECT batch_id, raw_address FROM source_rows WHERE source_row=?",
                              (source_row,)).fetchone()
            if ex:
                if ex["batch_id"] != batch_id or ex["raw_address"] != raw_address:
                    raise ValueError(f"source_row {source_row} 已存在且内容不同，拒绝覆盖")
                continue
            conn.execute("INSERT INTO source_rows VALUES (?,?,?,?)",
                         (source_row, batch_id, raw_address, _now()))
            inserted += 1
    return inserted


def validate_response_payload(payload) -> list:
    """纯函数校验：返回隔离问题列表 [{qtype, rank, detail}]。"""
    issues = []
    for f in ("provider", "response_version", "returned_at"):
        if not payload.get(f):
            issues.append({"qtype": "incomplete_response", "rank": None,
                           "detail": f"missing response field: {f}"})
    cands = payload.get("candidates")
    if not isinstance(cands, list) or not cands:
        issues.append({"qtype": "incomplete_response", "rank": None,
                       "detail": "missing or empty candidates"})
        return issues
    seen_ranks = set()
    for c in cands:
        if not isinstance(c, dict):
            issues.append({"qtype": "incomplete_response", "rank": None,
                           "detail": "candidate is not an object"})
            continue
        rank = c.get("rank")
        missing = [f for f in ("rank", "normalized_address", "lat", "lon", "confidence")
                   if c.get(f) is None]
        if missing:
            issues.append({"qtype": "incomplete_response", "rank": rank,
                           "detail": f"candidate missing fields: {missing}"})
            continue
        if not isinstance(rank, int) or isinstance(rank, bool) or rank < 1:
            issues.append({"qtype": "incomplete_response", "rank": None,
                           "detail": f"bad rank: {rank!r}"})
            continue
        if rank in seen_ranks:
            issues.append({"qtype": "duplicate_rank", "rank": rank,
                           "detail": f"duplicate rank {rank} within the same response version"})
        seen_ranks.add(rank)
        lat, lon, conf = c["lat"], c["lon"], c["confidence"]
        if (not _is_num(lat) or not _is_num(lon)
                or not (-90 <= lat <= 90) or not (-180 <= lon <= 180)):
            issues.append({"qtype": "invalid_coordinate", "rank": rank,
                           "detail": f"invalid coordinate lat={lat!r} lon={lon!r}"})
        if not _is_num(conf) or not (0 <= conf <= 1):
            issues.append({"qtype": "incomplete_response", "rank": rank,
                           "detail": f"bad confidence: {conf!r}"})
    return issues


def ingest_response(conn, source_row, provider, response_version, returned_at,
                    candidates, received_at=None):
    """接收供应商响应：校验并隔离问题数据。同内容重送幂等；同版本不同内容拒绝。"""
    payload = {"source_row": source_row, "provider": provider,
               "response_version": response_version, "returned_at": returned_at,
               "candidates": candidates}
    phash = _sha(_canon(payload))
    ex = conn.execute(
        "SELECT response_id, payload_hash FROM responses "
        "WHERE source_row=? AND provider=? AND response_version=?",
        (source_row, provider, response_version)).fetchone()
    if ex:
        if ex["payload_hash"] == phash:
            return ex["response_id"], False          # 幂等重放
        raise ValueError(
            f"{source_row}/{provider}@{response_version} 已存在不同内容：版本内容不可变")
    if conn.execute("SELECT 1 FROM source_rows WHERE source_row=?",
                    (source_row,)).fetchone() is None:
        raise ValueError(f"unknown source_row: {source_row}")
    issues = validate_response_payload(payload)
    with conn:
        cur = conn.execute(
            "INSERT INTO responses(source_row,provider,response_version,returned_at,"
            "payload_hash,received_at) VALUES (?,?,?,?,?,?)",
            (source_row, provider, response_version, returned_at, phash,
             received_at or _now()))
        rid = cur.lastrowid
        for c in candidates or []:
            if not isinstance(c, dict):
                continue
            conn.execute(
                "INSERT INTO candidates(response_id,rank,normalized_address,lat,lon,confidence)"
                " VALUES (?,?,?,?,?,?)",
                (rid, c.get("rank"), c.get("normalized_address"),
                 c.get("lat"), c.get("lon"), c.get("confidence")))
        for q in issues:
            conn.execute(
                "INSERT INTO quarantine(stage,snapshot_id,source_row,provider,"
                "response_version,rank,qtype,detail,created_at)"
                " VALUES ('ingest',NULL,?,?,?,?,?,?,?)",
                (source_row, provider, response_version, q["rank"], q["qtype"],
                 q["detail"], _now()))
    return rid, True


def validate_response(conn, source_row, provider, response_version) -> list:
    """校验接口：对已入库响应重新校验并返回问题列表。"""
    r = conn.execute(
        "SELECT * FROM responses WHERE source_row=? AND provider=? AND response_version=?",
        (source_row, provider, response_version)).fetchone()
    if r is None:
        raise ValueError(f"response not found: {source_row}/{provider}@{response_version}")
    cands = [dict(c) for c in conn.execute(
        "SELECT rank,normalized_address,lat,lon,confidence FROM candidates "
        "WHERE response_id=? ORDER BY rank", (r["response_id"],))]
    return validate_response_payload(
        {"provider": provider, "response_version": response_version,
         "returned_at": r["returned_at"], "candidates": cands})


# ---------------------------------------------------------------- 规则（含人工保留）

def register_rule(conn, rule_version, preferred_provider=None,
                  distance_threshold_m=50.0, confidence_diff_threshold=0.2,
                  holds=()) -> bool:
    """登记规则版本。holds: [{source_row, provider, response_version, rank,
    snapshot_id?, note?}] —— 人工保留只针对明确源行与候选身份。"""
    holds = [dict(h) for h in (holds or [])]
    for h in holds:
        for f in ("source_row", "provider", "response_version", "rank"):
            if f not in h:
                raise ValueError(f"hold 缺少字段 {f}: {h}")
    ex = conn.execute("SELECT * FROM rules WHERE rule_version=?", (rule_version,)).fetchone()
    if ex:
        same = (ex["preferred_provider"] == preferred_provider
                and abs(ex["distance_threshold_m"] - float(distance_threshold_m)) < 1e-9
                and abs(ex["confidence_diff_threshold"] - float(confidence_diff_threshold)) < 1e-9
                and json.loads(ex["holds_json"]) == holds)
        if not same:
            raise ValueError(f"rule_version {rule_version} 已登记且内容不同，拒绝覆盖")
        return False
    with conn:
        conn.execute("INSERT INTO rules VALUES (?,?,?,?,?,?)",
                     (rule_version, preferred_provider, float(distance_threshold_m),
                      float(confidence_diff_threshold), _canon(holds), _now()))
    return True


# ---------------------------------------------------------------- 归并

def _fingerprint(batch_id, rows, provider_versions, rule_version) -> str:
    return _sha(_canon({
        "batch_id": batch_id,
        "rows": [[r["source_row"], r["raw_address"]] for r in rows],
        "provider_versions": sorted(provider_versions.items()),
        "rule_version": rule_version,
    }))


def _gather(conn, source_row, provider_versions):
    """收集每个供应商在指定版本下的有效候选；返回 (per_provider, 合并期隔离项)。"""
    per_provider, mq = {}, []
    for provider, version in sorted(provider_versions.items()):
        r = conn.execute(
            "SELECT * FROM responses WHERE source_row=? AND provider=? AND response_version=?",
            (source_row, provider, version)).fetchone()
        if r is None:
            mq.append({"qtype": "incomplete_response", "provider": provider,
                       "response_version": version, "rank": None,
                       "detail": f"no response received from {provider}@{version}"})
            per_provider[provider] = {"version": version, "status": "missing",
                                      "cands": [], "returned_at": None}
            continue
        issues = conn.execute(
            "SELECT qtype, rank FROM quarantine WHERE stage='ingest' AND source_row=? "
            "AND provider=? AND response_version=?",
            (source_row, provider, version)).fetchall()
        blocking = sorted({q["qtype"] for q in issues if q["qtype"] in BLOCKING_INGEST_QTYPES})
        if blocking:
            mq.append({"qtype": "incomplete_response", "provider": provider,
                       "response_version": version, "rank": None,
                       "detail": f"response unusable due to ingest quarantine: {blocking}"})
            per_provider[provider] = {"version": version, "status": "quarantined",
                                      "cands": [], "returned_at": r["returned_at"]}
            continue
        bad_ranks = {q["rank"] for q in issues if q["qtype"] == "invalid_coordinate"}
        cands = [dict(c) for c in conn.execute(
            "SELECT rank,normalized_address,lat,lon,confidence FROM candidates "
            "WHERE response_id=? ORDER BY rank", (r["response_id"],))]
        good = [c for c in cands if c["rank"] not in bad_ranks]
        status = "ok" if good else "empty"
        if not good:
            mq.append({"qtype": "incomplete_response", "provider": provider,
                       "response_version": version, "rank": None,
                       "detail": "no usable candidates after coordinate quarantine"})
        per_provider[provider] = {"version": version, "status": status,
                                  "cands": good, "returned_at": r["returned_at"]}
    return per_provider, mq


def _sel_dict(provider, version, cand, returned_at):
    return {"identity": ident(provider, version, cand["rank"]),
            "provider": provider, "response_version": version, "rank": cand["rank"],
            "normalized_address": cand["normalized_address"], "lat": cand["lat"],
            "lon": cand["lon"], "confidence": cand["confidence"],
            "returned_at": returned_at}


def _decide(per_provider, rule, holds):
    """纯函数：由有效候选 + 规则（含人工保留）计算选择结果与解释。"""
    dist_thr = rule["distance_threshold_m"]
    conf_thr = rule["confidence_diff_threshold"]
    pref = rule["preferred_provider"]

    considered, best = [], {}
    for provider, d in sorted(per_provider.items()):
        for c in d["cands"]:
            considered.append({"identity": ident(provider, d["version"], c["rank"]),
                               "provider": provider, "response_version": d["version"],
                               "rank": c["rank"], "normalized_address": c["normalized_address"],
                               "lat": c["lat"], "lon": c["lon"], "confidence": c["confidence"],
                               "returned_at": d.get("returned_at")})
        if d["cands"]:
            best[provider] = d["cands"][0]

    pending = [f"provider_unavailable:{p}@{d['version']}({d['status']})"
               for p, d in sorted(per_provider.items()) if not d["cands"]]
    comparison = {"preferred_provider": pref,
                  "distance_threshold_m": dist_thr,
                  "confidence_diff_threshold": conf_thr,
                  "contenders": [ident(p, per_provider[p]["version"], best[p]["rank"])
                                 for p in sorted(best)],
                  "max_pairwise_distance_m": None,
                  "confidence_diff": None}
    result = {"decision": "pending", "applied_rule": None, "selected": None,
              "comparison": comparison, "considered": considered, "eliminated": [],
              "pending_reasons": pending, "hold": None, "quarantine": []}

    def lower_rank_elims():
        return [{"identity": ident(p, d["version"], c["rank"]), "reason": "lower_rank"}
                for p, d in sorted(per_provider.items()) for c in d["cands"][1:]]

    # 1) 人工保留：只在保留的候选身份存在于本次请求版本中时适用
    if holds:
        h = holds[0]
        hold_info = {"provider": h["provider"], "response_version": h["response_version"],
                     "rank": h["rank"], "affirmed_snapshot_id": h.get("snapshot_id"),
                     "note": h.get("note")}
        held, d = None, per_provider.get(h["provider"])
        if d and d["version"] == h["response_version"]:
            held = next((c for c in d["cands"] if c["rank"] == h["rank"]), None)
        if held is not None:
            result.update(decision="hold", applied_rule="human_hold")
            result["selected"] = _sel_dict(h["provider"], d["version"], held,
                                           d.get("returned_at"))
            result["hold"] = dict(hold_info, status="applied")
            kept = result["selected"]["identity"]
            result["eliminated"] = [{"identity": c["identity"], "reason": "not_held"}
                                    for c in considered if c["identity"] != kept]
            return result
        # 供应商版本变化等原因使保留身份缺席：不静默沿用，重新计算并挂起待确认
        result["hold"] = dict(hold_info, status="superseded")
        pending.append(
            f"hold_superseded:{ident(h['provider'], h['response_version'], h['rank'])} "
            f"not present under requested provider versions; selection recomputed by rules")

    # 2) 规则链
    if not best:
        result["applied_rule"] = "no_valid_candidates"
        pending.append("no_valid_candidates")
        return result
    if len(best) == 1:
        provider = next(iter(best))
        d = per_provider[provider]
        result.update(decision="auto", applied_rule="single_provider")
        result["selected"] = _sel_dict(provider, d["version"], best[provider],
                                       d.get("returned_at"))
        result["eliminated"] = lower_rank_elims()
        return result

    providers = sorted(best)
    max_dist = 0.0
    for i in range(len(providers)):
        for j in range(i + 1, len(providers)):
            c1, c2 = best[providers[i]], best[providers[j]]
            max_dist = max(max_dist, haversine_m(c1["lat"], c1["lon"],
                                                 c2["lat"], c2["lon"]))
    confs = {p: best[p]["confidence"] for p in providers}
    ordered = sorted(confs.values(), reverse=True)
    conf_diff = ordered[0] - ordered[1] if len(ordered) > 1 else 0.0
    comparison["max_pairwise_distance_m"] = round(max_dist, 3)
    comparison["confidence_diff"] = round(conf_diff, 6)

    if max_dist <= dist_thr:                       # 距离一致 -> 优先供应商
        if pref in best:
            chosen, elim_reason = pref, "not_preferred_provider"
        else:
            chosen, elim_reason = max(confs, key=confs.get), "lower_confidence"
        result.update(decision="auto", applied_rule="providers_agree")
    elif conf_diff >= conf_thr:                    # 距离冲突但置信度差足够 -> 置信度裁决
        chosen, elim_reason = max(confs, key=confs.get), "lower_confidence"
        result.update(decision="auto", applied_rule="confidence_override")
    else:                                          # 规则冲突 -> 隔离 + 未决
        result["applied_rule"] = "rule_conflict"
        pending.append(
            f"rule_conflict: provider top candidates disagree by {max_dist:.1f}m "
            f"(> {dist_thr}m) with confidence diff {conf_diff:.3f} (< {conf_thr}); "
            f"manual review required")
        result["quarantine"].append({
            "qtype": "rule_conflict", "provider": None, "response_version": None,
            "rank": None,
            "detail": _canon({"contenders": comparison["contenders"],
                              "distance_m": comparison["max_pairwise_distance_m"],
                              "confidence_diff": comparison["confidence_diff"],
                              "distance_threshold_m": dist_thr,
                              "confidence_diff_threshold": conf_thr})})
        return result

    d = per_provider[chosen]
    result["selected"] = _sel_dict(chosen, d["version"], best[chosen], d.get("returned_at"))
    for p in providers:
        if p != chosen:
            result["eliminated"].append(
                {"identity": ident(p, per_provider[p]["version"], best[p]["rank"]),
                 "reason": elim_reason})
    result["eliminated"].extend(lower_rank_elims())
    return result


def _change(prev_sel, result, prev_ctx, provider_versions, rule_version):
    """与上一可读快照比较，给出 added/unchanged/inherited/changed 及原因。"""
    hold_status = (result["hold"] or {}).get("status")
    reasons = []
    if prev_ctx is not None:
        for p, v in sorted(provider_versions.items()):
            pv = prev_ctx["provider_versions"].get(p)
            if pv != v:
                reasons.append(f"provider_version_change:{p}:{pv}->{v}")
        if prev_ctx["rule_version"] != rule_version:
            reasons.append(f"rule_change:{prev_ctx['rule_version']}->{rule_version}")
    if hold_status == "superseded":
        reasons.append("hold_superseded")
    cur = result["selected"]
    cur_id = (cur["provider"], cur["response_version"], cur["rank"]) if cur else None
    cur_val = (cur["normalized_address"], cur["lat"], cur["lon"]) if cur else None
    if prev_sel is None:
        return "added", ";".join(reasons + ["first_snapshot_for_row"])
    prev_id = ((prev_sel["sel_provider"], prev_sel["sel_response_version"],
                prev_sel["sel_rank"]) if prev_sel["sel_provider"] is not None else None)
    prev_val = ((prev_sel["sel_normalized_address"], prev_sel["sel_lat"],
                 prev_sel["sel_lon"]) if prev_id else None)
    if cur_id == prev_id and result["decision"] == prev_sel["decision"]:
        if result["decision"] == "hold":
            return "inherited", ";".join(reasons + ["hold_carried_over"])
        return "unchanged", (";".join(reasons) if reasons else "selection_carried_over")
    if hold_status == "applied" and prev_sel["decision"] != "hold":
        reasons.append("hold_applied")
    if cur_id and prev_id:
        reasons.append("selected_value_unchanged" if cur_val == prev_val
                       else "selected_value_changed")
    if result["decision"] != prev_sel["decision"]:
        reasons.append(f"decision_changed:{prev_sel['decision']}->{result['decision']}")
    if not reasons:
        reasons.append("selection_changed")
    return "changed", ";".join(reasons)


def _snapshot_context(conn, sid):
    s = conn.execute("SELECT * FROM snapshots WHERE snapshot_id=?", (sid,)).fetchone()
    sels = {r["source_row"]: r for r in
            conn.execute("SELECT * FROM selections WHERE snapshot_id=?", (sid,))}
    return {"snapshot_id": sid, "rule_version": s["rule_version"],
            "provider_versions": json.loads(s["provider_versions_json"]),
            "selections": sels}


def _merge_one(conn, sid, source_row, provider_versions, rule, holds, prev_ctx):
    per_provider, mq = _gather(conn, source_row, provider_versions)
    result = _decide(per_provider, rule, holds)
    for q in mq + result["quarantine"]:
        conn.execute(
            "INSERT INTO quarantine(stage,snapshot_id,source_row,provider,"
            "response_version,rank,qtype,detail,created_at)"
            " VALUES ('merge',?,?,?,?,?,?,?,?)",
            (sid, source_row, q.get("provider"), q.get("response_version"),
             q.get("rank"), q["qtype"], q["detail"], _now()))
    prev_sel = prev_ctx["selections"].get(source_row) if prev_ctx else None
    flag, reason = _change(prev_sel, result, prev_ctx, provider_versions,
                           rule["rule_version"])
    sel = result["selected"] or {}
    explanation = _canon({k: result[k] for k in
                          ("applied_rule", "decision", "selected", "comparison",
                           "considered", "eliminated", "pending_reasons", "hold")})
    conn.execute(
        "INSERT INTO selections VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (sid, source_row, result["decision"], sel.get("provider"),
         sel.get("response_version"), sel.get("rank"), sel.get("normalized_address"),
         sel.get("lat"), sel.get("lon"), sel.get("confidence"), flag, reason,
         _canon(result["pending_reasons"]), explanation))


def create_snapshot(conn, batch_id, provider_versions, rule_version) -> dict:
    """归并接口：对 (批次, 供应商版本, 规则版本) 生成快照。相同输入幂等。"""
    if not isinstance(provider_versions, dict) or not provider_versions:
        raise ValueError("provider_versions 必须是非空 dict {provider: version}")
    rows = conn.execute(
        "SELECT source_row, raw_address FROM source_rows WHERE batch_id=? "
        "ORDER BY source_row", (batch_id,)).fetchall()
    fp = _fingerprint(batch_id, rows, provider_versions, rule_version)
    hit = conn.execute("SELECT snapshot_id FROM snapshots WHERE fingerprint=? "
                       "AND status='ok'", (fp,)).fetchone()
    if hit:
        return {"snapshot_id": hit["snapshot_id"], "replayed": True}
    try:
        if not rows:
            raise ValueError(f"unknown or empty batch: {batch_id}")
        rule = conn.execute("SELECT * FROM rules WHERE rule_version=?",
                            (rule_version,)).fetchone()
        if rule is None:
            raise ValueError(f"unknown rule_version: {rule_version}")
        holds_by_row = {}
        for h in json.loads(rule["holds_json"]):
            holds_by_row.setdefault(h["source_row"], []).append(h)
        with conn:
            cur = conn.execute(
                "INSERT INTO snapshots(fingerprint,batch_id,rule_version,"
                "provider_versions_json,status,error,created_at)"
                " VALUES (?,?,?,?,'ok',NULL,?)",
                (fp, batch_id, rule_version, _canon(provider_versions), _now()))
            sid = cur.lastrowid
            prev = conn.execute(
                "SELECT snapshot_id FROM snapshots WHERE status='ok' AND batch_id=? "
                "AND snapshot_id<? ORDER BY snapshot_id DESC LIMIT 1",
                (batch_id, sid)).fetchone()
            prev_ctx = _snapshot_context(conn, prev["snapshot_id"]) if prev else None
            for row in rows:
                _merge_one(conn, sid, row["source_row"], provider_versions, rule,
                           holds_by_row.get(row["source_row"], []), prev_ctx)
        return {"snapshot_id": sid, "replayed": False}
    except Exception as e:
        # 失败快照只留痕：不携带任何选择结果，永不参与读取
        with conn:
            conn.execute(
                "INSERT INTO snapshots(fingerprint,batch_id,rule_version,"
                "provider_versions_json,status,error,created_at)"
                " VALUES (?,?,?,?,'failed',?,?)",
                (fp, batch_id, rule_version, _canon(provider_versions), str(e), _now()))
        raise


# ---------------------------------------------------------------- 读取接口

def latest_snapshot_id(conn, batch_id=None):
    """最近的可读快照（失败快照永不被返回）。"""
    sql = "SELECT snapshot_id FROM snapshots WHERE status='ok'"
    args = []
    if batch_id is not None:
        sql += " AND batch_id=?"
        args.append(batch_id)
    sql += " ORDER BY snapshot_id DESC LIMIT 1"
    r = conn.execute(sql, args).fetchone()
    return r["snapshot_id"] if r else None


def _get_ok_snapshot(conn, snapshot_id):
    s = conn.execute("SELECT * FROM snapshots WHERE snapshot_id=?",
                     (snapshot_id,)).fetchone()
    if s is None or s["status"] != "ok":
        raise ValueError(f"snapshot {snapshot_id} 不可读（不存在或已失败）")
    return s


def _snap_meta(s):
    return {"snapshot_id": s["snapshot_id"], "batch_id": s["batch_id"],
            "rule_version": s["rule_version"],
            "provider_versions": json.loads(s["provider_versions_json"]),
            "status": s["status"], "created_at": s["created_at"]}


def _sel_summary(sel):
    if sel is None:
        return None
    return {"decision": sel["decision"],
            "selected": (ident(sel["sel_provider"], sel["sel_response_version"],
                               sel["sel_rank"]) if sel["sel_provider"] else None),
            "lat": sel["sel_lat"], "lon": sel["sel_lon"],
            "confidence": sel["sel_confidence"],
            "change_flag": sel["change_flag"], "change_reason": sel["change_reason"]}


def _selection_view(sel):
    if sel is None:
        return None
    return {"decision": sel["decision"],
            "selected_identity": (ident(sel["sel_provider"], sel["sel_response_version"],
                                        sel["sel_rank"]) if sel["sel_provider"] else None),
            "selected": {"provider": sel["sel_provider"],
                         "response_version": sel["sel_response_version"],
                         "rank": sel["sel_rank"],
                         "normalized_address": sel["sel_normalized_address"],
                         "lat": sel["sel_lat"], "lon": sel["sel_lon"],
                         "confidence": sel["sel_confidence"]},
            "change_flag": sel["change_flag"], "change_reason": sel["change_reason"],
            "pending_reasons": json.loads(sel["pending_reasons_json"]),
            "explanation": json.loads(sel["explanation_json"])}


def get_source_row(conn, source_row, snapshot_id=None) -> dict:
    """查询接口：源行在指定（默认最新可读）快照下的完整视图。"""
    row = conn.execute("SELECT * FROM source_rows WHERE source_row=?",
                       (source_row,)).fetchone()
    if row is None:
        raise ValueError(f"unknown source_row: {source_row}")
    if snapshot_id is None:
        snapshot_id = latest_snapshot_id(conn, row["batch_id"])
        if snapshot_id is None:
            raise ValueError(f"batch {row['batch_id']} 尚无可读快照")
    snap = _get_ok_snapshot(conn, snapshot_id)
    sel = conn.execute("SELECT * FROM selections WHERE snapshot_id=? AND source_row=?",
                       (snapshot_id, source_row)).fetchone()
    responses = []
    for r in conn.execute("SELECT * FROM responses WHERE source_row=? "
                          "ORDER BY provider, response_version", (source_row,)):
        cands = [dict(c) for c in conn.execute(
            "SELECT rank,normalized_address,lat,lon,confidence FROM candidates "
            "WHERE response_id=? ORDER BY rank", (r["response_id"],))]
        issues = [dict(q) for q in conn.execute(
            "SELECT qtype,rank,detail FROM quarantine WHERE stage='ingest' "
            "AND source_row=? AND provider=? AND response_version=?",
            (source_row, r["provider"], r["response_version"]))]
        responses.append({"provider": r["provider"],
                          "response_version": r["response_version"],
                          "returned_at": r["returned_at"],
                          "received_at": r["received_at"],
                          "candidates": cands, "ingest_quarantine": issues})
    quars = [dict(q) for q in conn.execute(
        "SELECT stage,snapshot_id,provider,response_version,rank,qtype,detail "
        "FROM quarantine WHERE source_row=? ORDER BY quarantine_id", (source_row,))]
    history = [dict(h) for h in conn.execute(
        "SELECT sl.snapshot_id, sl.decision, sl.sel_provider, sl.sel_response_version,"
        " sl.sel_rank, sl.change_flag, sl.change_reason "
        "FROM selections sl JOIN snapshots s ON s.snapshot_id=sl.snapshot_id "
        "WHERE sl.source_row=? AND s.status='ok' ORDER BY sl.snapshot_id", (source_row,))]
    return {"source_row": source_row, "batch_id": row["batch_id"],
            "raw_address": row["raw_address"], "snapshot": _snap_meta(snap),
            "selection": _selection_view(sel), "responses": responses,
            "quarantine": quars, "history": history}


def compare_snapshots(conn, a, b) -> dict:
    """比较接口：两个可读快照之间逐源行对比。"""
    sa, sb = _get_ok_snapshot(conn, a), _get_ok_snapshot(conn, b)
    va = json.loads(sa["provider_versions_json"])
    vb = json.loads(sb["provider_versions_json"])
    version_diffs = [f"{p}:{va.get(p)}->{vb.get(p)}"
                     for p in sorted(set(va) | set(vb)) if va.get(p) != vb.get(p)]
    if sa["rule_version"] != sb["rule_version"]:
        version_diffs.append(f"rule:{sa['rule_version']}->{sb['rule_version']}")
    sela = {r["source_row"]: r for r in
            conn.execute("SELECT * FROM selections WHERE snapshot_id=?", (a,))}
    selb = {r["source_row"]: r for r in
            conn.execute("SELECT * FROM selections WHERE snapshot_id=?", (b,))}
    rows, summary = {}, {}
    for sr in sorted(set(sela) | set(selb)):
        xa, xb = sela.get(sr), selb.get(sr)
        shift = None
        if xa is None:
            tag = "added_in_b"
        elif xb is None:
            tag = "removed_in_b"
        else:
            same_identity = ((xa["sel_provider"], xa["sel_response_version"],
                              xa["sel_rank"]) ==
                             (xb["sel_provider"], xb["sel_response_version"],
                              xb["sel_rank"]))
            if xa["sel_lat"] is not None and xb["sel_lat"] is not None:
                shift = round(haversine_m(xa["sel_lat"], xa["sel_lon"],
                                          xb["sel_lat"], xb["sel_lon"]), 3)
            if same_identity and xa["decision"] == xb["decision"]:
                tag = "inherited" if xb["decision"] == "hold" else "unchanged"
            else:
                tag = "changed"
        rows[sr] = {"a": _sel_summary(xa), "b": _sel_summary(xb), "change": tag,
                    "coordinate_shift_m": shift}
        summary[tag] = summary.get(tag, 0) + 1
    return {"snapshot_a": _snap_meta(sa), "snapshot_b": _snap_meta(sb),
            "version_diffs": version_diffs, "rows": rows, "summary": summary}


def export_pending(conn, snapshot_id=None) -> dict:
    """导出待人工处理清单：未决、有待决原因或命中隔离的源行，
    附来源（供应商/版本/返回时间）与规则证据。"""
    if snapshot_id is None:
        snapshot_id = latest_snapshot_id(conn)
        if snapshot_id is None:
            raise ValueError("尚无可读快照")
    snap = _get_ok_snapshot(conn, snapshot_id)
    rule = conn.execute("SELECT * FROM rules WHERE rule_version=?",
                        (snap["rule_version"],)).fetchone()
    requested = json.loads(snap["provider_versions_json"])
    items = []
    for s in conn.execute("SELECT * FROM selections WHERE snapshot_id=? "
                          "ORDER BY source_row", (snapshot_id,)):
        sr = s["source_row"]
        pending = json.loads(s["pending_reasons_json"])
        q_merge = [dict(q) for q in conn.execute(
            "SELECT provider,response_version,rank,qtype,detail FROM quarantine "
            "WHERE stage='merge' AND snapshot_id=? AND source_row=?", (snapshot_id, sr))]
        q_ingest = [dict(q) for p, v in sorted(requested.items()) for q in conn.execute(
            "SELECT provider,response_version,rank,qtype,detail FROM quarantine "
            "WHERE stage='ingest' AND source_row=? AND provider=? AND response_version=?",
            (sr, p, v))]
        if s["decision"] != "pending" and not pending and not q_merge and not q_ingest:
            continue
        expl = json.loads(s["explanation_json"])
        raw = conn.execute("SELECT raw_address FROM source_rows WHERE source_row=?",
                           (sr,)).fetchone()
        items.append({
            "source_row": sr, "raw_address": raw["raw_address"],
            "decision": s["decision"],
            "change_flag": s["change_flag"], "change_reason": s["change_reason"],
            "pending_reasons": pending,
            "selected": expl["selected"],                 # 含候选身份与经纬度
            "candidates": expl["considered"],             # 全部有效候选 + 来源
            "eliminated": expl["eliminated"],             # 被淘汰候选及原因
            "quarantine": {"ingest": q_ingest, "merge": q_merge},
            "rule_evidence": {                            # 规则证据
                "rule_version": snap["rule_version"],
                "preferred_provider": rule["preferred_provider"],
                "distance_threshold_m": rule["distance_threshold_m"],
                "confidence_diff_threshold": rule["confidence_diff_threshold"],
                "applied_rule": expl["applied_rule"],
                "comparison": expl["comparison"]},
            "hold": expl["hold"],
            "provenance": {"provider_versions": requested,
                           "selected_returned_at": (expl["selected"] or {}).get("returned_at")},
        })
    return {"snapshot": _snap_meta(snap), "item_count": len(items), "items": items}


# ---------------------------------------------------------------- CLI

def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def main(argv=None):
    p = argparse.ArgumentParser(prog="merger", description="候选快照归并器")
    sub = p.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("init"); sp.add_argument("db")
    sp = sub.add_parser("add-rows"); sp.add_argument("db"); sp.add_argument("batch_id"); sp.add_argument("file")
    sp = sub.add_parser("add-rule"); sp.add_argument("db"); sp.add_argument("file")
    sp = sub.add_parser("ingest"); sp.add_argument("db"); sp.add_argument("file")
    sp = sub.add_parser("validate"); sp.add_argument("db"); sp.add_argument("source_row"); sp.add_argument("provider"); sp.add_argument("response_version")
    sp = sub.add_parser("snapshot"); sp.add_argument("db"); sp.add_argument("batch_id"); sp.add_argument("rule_version"); sp.add_argument("provider_versions", help="如 provA=v1,provB=v1")
    sp = sub.add_parser("compare"); sp.add_argument("db"); sp.add_argument("a", type=int); sp.add_argument("b", type=int)
    sp = sub.add_parser("get"); sp.add_argument("db"); sp.add_argument("source_row"); sp.add_argument("--snapshot", type=int)
    sp = sub.add_parser("export"); sp.add_argument("db"); sp.add_argument("--snapshot", type=int)
    sp = sub.add_parser("latest"); sp.add_argument("db"); sp.add_argument("--batch")
    args = p.parse_args(argv)

    if args.cmd == "init":
        init_db(args.db)
        _print({"ok": True, "db": args.db})
        return
    conn = connect(args.db)
    if args.cmd == "add-rows":
        rows = json.load(open(args.file, encoding="utf-8"))
        _print({"inserted": ingest_source_rows(
            conn, args.batch_id, [(r["source_row"], r["raw_address"]) for r in rows])})
    elif args.cmd == "add-rule":
        _print({"created": register_rule(conn, **json.load(open(args.file, encoding="utf-8")))})
    elif args.cmd == "ingest":
        rid, created = ingest_response(conn, **json.load(open(args.file, encoding="utf-8")))
        _print({"response_id": rid, "created": created})
    elif args.cmd == "validate":
        _print({"issues": validate_response(conn, args.source_row, args.provider,
                                            args.response_version)})
    elif args.cmd == "snapshot":
        pv = dict(kv.split("=", 1) for kv in args.provider_versions.split(",") if kv)
        _print(create_snapshot(conn, args.batch_id, pv, args.rule_version))
    elif args.cmd == "compare":
        _print(compare_snapshots(conn, args.a, args.b))
    elif args.cmd == "get":
        _print(get_source_row(conn, args.source_row, args.snapshot))
    elif args.cmd == "export":
        _print(export_pending(conn, args.snapshot))
    elif args.cmd == "latest":
        _print({"snapshot_id": latest_snapshot_id(conn, args.batch)})


if __name__ == "__main__":
    main()
