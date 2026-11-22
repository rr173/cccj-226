"""站点端：把初装包/补丁应用到本站 SQLite，一次提交推进到目标修订。

站点库
------
* ``ss_meta``：固定 ``source_id``（发布源）与 ``site_id``（本站），初装后不再改变。
* ``ss_addresses``：本站当前派单地址（``source_row`` + 名册经纬度），
  与 ``ss_meta.revision`` 在同一次事务中更新。
* ``ss_applied``：最近成功应用的包（类型、起止版本、内容摘要），用于无副作用重放。

校验（全部在任何写操作之前完成，失败不改库）
--------------------------------------------
* 发布源 ID 与站点 ID 必须与本站固定身份一致；
* 初装包只能应用到空库；已初装后拒绝再次初装（避免暗中重置）；
* 补丁的 ``from_revision`` 必须等于本站当前修订号（缺版/倒序/跳版都拒绝）；
* 包内地址项必须按 ``source_row`` 严格升序（重复即冲突）；``del`` 的地址
  必须存在、``add`` 的地址必须不存在——有任何冲突整包拒绝，绝不部分应用。

幂等重放
--------
上一次成功应用的包原文若与本次完全一致，直接返回 ``replayed``，不写任何行。
内容不同（哪怕版本号相同）一律拒绝：不能用重送接受另一份内容不同的包。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3

import sync_common

PROTOCOL = "dispatch-site-sync/1"

SCHEMA = """
CREATE TABLE IF NOT EXISTS ss_meta (
  key   TEXT PRIMARY KEY CHECK (key IN
          ('source_id', 'site_id', 'revision', 'last_digest')),
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ss_addresses (
  source_row TEXT PRIMARY KEY,
  lon        REAL,
  lat        REAL
);
CREATE TABLE IF NOT EXISTS ss_applied (
  seq              INTEGER PRIMARY KEY AUTOINCREMENT,
  package_type     TEXT NOT NULL CHECK (package_type IN ('initial', 'patch')),
  from_revision    INTEGER,
  target_revision  INTEGER NOT NULL,
  item_count       INTEGER NOT NULL,
  content_digest   TEXT NOT NULL,
  package_json     TEXT NOT NULL,
  applied_at       TEXT NOT NULL
);
"""

# 本机固定身份；初装后任何包都不得改变
FIXED_KEYS = ("source_id", "site_id")


class SiteError(ValueError):
    """站点应用业务规则错误（身份不符、缺版、倒序、冲突等）。"""


# ---------------------------------------------------------------- 库与读取


def connect(path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(path) -> sqlite3.Connection:
    conn = connect(path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def _ensure_schema(conn):
    for stmt in SCHEMA.split(";"):
        if stmt.strip():
            conn.execute(stmt)


def _meta(conn) -> dict:
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='ss_meta'").fetchone() is None:
        return {}
    return {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM ss_meta")}


def read_state(conn) -> dict:
    """读站点当前状态：身份、当前修订号、地址条数（单事务一致快照）。"""
    old_level = conn.isolation_level
    conn.isolation_level = None
    try:
        conn.execute("BEGIN")
        meta = _meta(conn)
        if "source_id" not in meta:
            conn.rollback()
            raise SiteError("站点库尚未初装（没有固定发布源/站点身份）")
        rows = conn.execute(
            "SELECT source_row, lon, lat FROM ss_addresses ORDER BY source_row"
        ).fetchall()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.isolation_level = old_level
    return {"source_id": meta["source_id"], "site_id": meta["site_id"],
            "revision": int(meta["revision"]),
            "address_count": len(rows),
            "addresses": [{"source_row": r["source_row"], "lon": r["lon"],
                           "lat": r["lat"]} for r in rows]}


def read_addresses(conn) -> list:
    return read_state(conn)["addresses"]


def _digest(pkg) -> str:
    """内容摘要：按规范 JSON（排序键）计算，与字段书写顺序无关。"""
    return hashlib.sha256(sync_common.canon(pkg).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- 校验


def _validate_package(pkg) -> dict:
    if not isinstance(pkg, dict):
        raise SiteError("同步包必须是 JSON 对象")
    if pkg.get("protocol") != PROTOCOL:
        raise SiteError(f"不支持的同步协议: {pkg.get('protocol')!r}")
    ptype = pkg.get("package_type")
    if ptype not in sync_common.PACKAGE_TYPES:
        raise SiteError(f"未知包类型: {ptype!r}")
    for f in ("source_id", "site_id"):
        if not isinstance(pkg.get(f), str) or not pkg[f]:
            raise SiteError(f"包缺少字段 {f}")
    for f in ("from_revision", "target_revision"):
        v = pkg.get(f)
        if v is not None and (isinstance(v, bool) or not isinstance(v, int)):
            raise SiteError(f"{f} 必须是整数修订号: {v!r}")
    if ptype == "initial":
        if pkg.get("from_revision") is not None:
            raise SiteError("初装包不应带 from_revision")
        if pkg.get("target_revision") is None:
            raise SiteError("初装包缺少 target_revision")
    else:
        if pkg.get("from_revision") is None or pkg.get("target_revision") is None:
            raise SiteError("补丁缺少 from_revision/target_revision")
        if pkg["from_revision"] > pkg["target_revision"]:
            raise SiteError("补丁倒序：from_revision 大于 target_revision")
    items = pkg.get("items")
    if not isinstance(items, list):
        raise SiteError("包缺少 items 列表")
    last = None
    for it in items:
        if not isinstance(it, dict) or not isinstance(it.get("source_row"), str) \
                or not it["source_row"]:
            raise SiteError(f"地址项非法: {it!r}")
        sr = it["source_row"]
        if last is not None and sr <= last:
            raise SiteError(
                f"地址项未按 source_row 严格升序（重复或相互冲突）: {sr!r} 在 {last!r} 之后")
        last = sr
        if ptype == "initial":
            for f in ("lon", "lat"):
                v = it.get(f)
                if v is not None and (isinstance(v, bool) or not isinstance(v, (int, float))):
                    raise SiteError(f"地址项 {sr} 的 {f} 非数值: {v!r}")
        else:
            if it.get("op") not in ("add", "del"):
                raise SiteError(f"地址项 {sr} 的 op 非法: {it.get('op')!r}")
            if it["op"] == "add":
                for f in ("lon", "lat"):
                    v = it.get(f)
                    if v is not None and (isinstance(v, bool)
                                          or not isinstance(v, (int, float))):
                        raise SiteError(f"地址项 {sr} 的 {f} 非数值: {v!r}")
    return pkg


def _check_identity(conn, pkg):
    """身份核对：本地已固定身份时必须一致；包带本站未知的身份一律拒绝。"""
    meta = _meta(conn)
    if "source_id" in meta:
        if pkg["source_id"] != meta["source_id"]:
            raise SiteError(
                f"发布源不符：包为 {pkg['source_id']}，本站固定为 {meta['source_id']}")
        if pkg["site_id"] != meta["site_id"]:
            raise SiteError(
                f"站点不符：包为 {pkg['site_id']}，本站固定为 {meta['site_id']}")


def _last_applied(conn):
    return conn.execute(
        "SELECT package_type, from_revision, target_revision, content_digest, "
        "package_json FROM ss_applied ORDER BY seq DESC LIMIT 1").fetchone()


def preview(conn, pkg) -> dict:
    """只校验不写库，返回将执行的动作（供 CLI 干跑/测试）。"""
    _validate_package(pkg)
    _check_identity(conn, pkg)
    meta = _meta(conn)
    installed = "source_id" in meta
    last = _last_applied(conn)
    digest = _digest(pkg)
    if last is not None:
        if digest == last["content_digest"]:
            return {"action": "replayed", "digest": digest,
                    "target_revision": pkg["target_revision"]}
        if (pkg.get("target_revision") == last["target_revision"]
                and pkg.get("from_revision") == last["from_revision"]):
            raise SiteError("同一版本区间已应用过另一份内容不同的包，拒绝")
    if pkg["package_type"] == "initial":
        if installed:
            raise SiteError("站点已初装：拒绝重复初装（不会重置本站地址）")
        return {"action": "install", "digest": digest,
                "target_revision": pkg["target_revision"]}
    if not installed:
        raise SiteError("站点尚未初装：补丁不能应用到空库")
    cur = int(meta["revision"])
    if pkg["from_revision"] != cur:
        if pkg["from_revision"] < cur:
            raise SiteError(
                f"补丁倒序/过期：起始修订 {pkg['from_revision']} 早于本站当前 {cur}")
        raise SiteError(
            f"缺版：补丁起始修订 {pkg['from_revision']} 晚于本站当前 {cur}")
    return {"action": "apply", "digest": digest,
            "from_revision": pkg["from_revision"],
            "target_revision": pkg["target_revision"]}


def apply_package(conn, pkg, dry_run=False) -> dict:
    """应用初装包/补丁：核对 -> 全量预检 -> 单事务更新地址与版本游标。"""
    if isinstance(pkg, str):
        pkg = json.loads(pkg)
    plan = preview(conn, pkg)
    if plan["action"] == "replayed":
        return {"applied": False, "replayed": True,
                "revision": read_state(conn)["revision"],
                "item_count": len(pkg["items"])}
    if dry_run:
        return {"dry_run": True, **plan, "item_count": len(pkg["items"])}

    old_level = conn.isolation_level
    conn.isolation_level = None
    try:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_schema(conn)
        meta = _meta(conn)
        # 事务内再次核对（防止与其它写者竞态）：身份与当前版本
        _check_identity(conn, pkg)
        last = _last_applied(conn)
        digest = _digest(pkg)
        if last is not None and digest == last["content_digest"]:
            conn.rollback()
            return {"applied": False, "replayed": True,
                    "revision": int(_meta(conn)["revision"]),
                    "item_count": len(pkg["items"])}
        if pkg["package_type"] == "initial":
            if "source_id" in meta:
                raise SiteError("站点已初装：拒绝重复初装")
            conn.execute("INSERT INTO ss_meta(key, value) VALUES ('source_id', ?)",
                         (pkg["source_id"],))
            conn.execute("INSERT INTO ss_meta(key, value) VALUES ('site_id', ?)",
                         (pkg["site_id"],))
            for it in pkg["items"]:
                conn.execute("INSERT INTO ss_addresses(source_row, lon, lat) "
                             "VALUES (?,?,?)", (it["source_row"], it.get("lon"),
                                                it.get("lat")))
            from_rev = None
        else:
            cur = int(meta["revision"])
            if pkg["from_revision"] != cur:
                raise SiteError(
                    f"补丁起始修订 {pkg['from_revision']} 与本站当前 {cur} 不一致")
            # 先做全量预检：任何一条冲突都整包拒绝（不留下前面的新增）
            existing = {r["source_row"] for r in conn.execute(
                "SELECT source_row FROM ss_addresses WHERE source_row IN "
                f"({','.join('?' for _ in pkg['items'])})",
                tuple(it["source_row"] for it in pkg["items"]))} if pkg["items"] else set()
            for it in pkg["items"]:
                if it["op"] == "add" and it["source_row"] in existing:
                    raise SiteError(f"地址 {it['source_row']} 已存在，不能重复新增")
                if it["op"] == "del" and it["source_row"] not in existing:
                    raise SiteError(f"地址 {it['source_row']} 不在本站，不能删除")
            for it in pkg["items"]:
                if it["op"] == "add":
                    conn.execute("INSERT INTO ss_addresses(source_row, lon, lat) "
                                 "VALUES (?,?,?)",
                                 (it["source_row"], it.get("lon"), it.get("lat")))
                else:
                    conn.execute("DELETE FROM ss_addresses WHERE source_row=?",
                                 (it["source_row"],))
            from_rev = pkg["from_revision"]
        conn.execute(
            "INSERT INTO ss_meta(key, value) VALUES ('revision', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(pkg["target_revision"]),))
        conn.execute(
            "INSERT INTO ss_meta(key, value) VALUES ('last_digest', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (digest,))
        conn.execute(
            "INSERT INTO ss_applied(package_type, from_revision, target_revision, "
            "item_count, content_digest, package_json, applied_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (pkg["package_type"], from_rev, pkg["target_revision"],
             len(pkg["items"]), digest,
             sync_common.canon(pkg), sync_common.now()))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.isolation_level = old_level
    return {"applied": True, "replayed": False,
            "revision": pkg["target_revision"], "item_count": len(pkg["items"])}


# ---------------------------------------------------------------- CLI


def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def main(argv=None):
    p = argparse.ArgumentParser(
        prog="sync_site", description="站点端：应用初装包/补丁，查看本站地址与版本")
    sub = p.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("apply", help="应用同步包（原子：地址与版本一起提交）")
    sp.add_argument("db"); sp.add_argument("package_file")
    sp.add_argument("--dry-run", action="store_true", help="只校验不写库")
    sp = sub.add_parser("show", help="查看本站身份、当前修订号与全部地址")
    sp.add_argument("db")
    sp.add_argument("--out", help="地址表输出到文件（默认标准输出）")
    sp = sub.add_parser("init", help="建空的站点库（初装包会写入固定身份）")
    sp.add_argument("db")
    args = p.parse_args(argv)

    if args.cmd == "init":
        init_db(args.db).close()
        _print({"ok": True, "db": args.db})
        return None

    conn = connect(args.db)
    try:
        if args.cmd == "apply":
            pkg = json.load(open(args.package_file, encoding="utf-8"))
            result = apply_package(conn, pkg, dry_run=args.dry_run)
            _print(result)
            return result
        state = read_state(conn)
        if args.out:
            open(args.out, "w", encoding="utf-8").write(
                json.dumps(state["addresses"], ensure_ascii=False, indent=2) + "\n")
            _print({k: v for k, v in state.items() if k != "addresses"})
        else:
            _print(state)
        return state
    finally:
        conn.close()


if __name__ == "__main__":
    main()
