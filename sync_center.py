"""中心端站点同步：启用、初装包、量补丁（与派单名册共用同一 SQLite 库）。

口径
----
* **启用**（``enable_sync``）：在已有名册上记下当时的名册修订号作为
  ``enabled_revision``（最早同步版本），并分配一个持久、与其他中心库不同
  的发布源 ID（``source_id``）。重复启用幂等返回原身份与起点；启用前的
  版本没有历史可编造，也不导出。
* **站点** = 派单结果里的区 ID 映射到的站点（``sync_zone_site``）。启用时
  由静态映射（``dz_zone_sites`` 或显式参数）整理一次；之后随原有改区在
  同一事务中演进（``dispatch.apply_changes`` 写 ``sync_changes``）。
* **初装包**：读站点在目标修订当前的已提交地址集（`roster_records.zone_id`
  经发布映射落到该站），每条只给 ``source_row`` 与名册经纬度。
* **补丁**：给站点、起始/目标修订；两端都必须是启用以来已提交的版本，
  ``from <= to``。只取两个版本之间的**净差**：起点不在本站而终点在 -> 完整
  地址行；起点在、终点不在 -> 删除项；两端相同不发。实现上按
  ``(site_id, source_row)`` 索引把该地址在区间内的 ``sync_changes`` 折叠成
  起始/终点状态，因此不扫完整名册、不遍历其他站点，也不随空修订增长。
* 同一请求对同一版本重复导出逐字节一致（地址按 ``source_row`` 排序）。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import secrets

import dispatch
import sync_common

PROTOCOL = "dispatch-site-sync/1"


class SyncError(ValueError):
    """同步业务规则错误（未启用、版本越界、站点未知等）。"""


# ---------------------------------------------------------------- 启用


def _new_source_id(conn) -> str:
    """生成发布源 ID：随机 + 已存身份校验，重启后不变（存库），跨库不撞。"""
    for _ in range(8):
        sid = "SRC-" + secrets.token_hex(8).upper()
        if conn.execute("SELECT 1 FROM sync_meta WHERE key='source_id' AND value=?",
                        (sid,)).fetchone() is None:
            return sid
    raise SyncError("无法生成唯一的发布源 ID")


def enable_sync(conn, site_zones=None) -> dict:
    """在已有名册上启用同步（重复启用幂等返回原身份与起点）。

    ``site_zones`` 可给出启用时的区 -> 站点整理；省略时使用建册时的
    ``dz_zone_sites``。启用只允许整理映射，不改名册任何记录与修订号。
    """
    old_level = conn.isolation_level
    conn.isolation_level = None
    try:
        conn.execute("BEGIN IMMEDIATE")
        sync_common.ensure_sync_tables(conn)
        if not dispatch.roster_exists(conn) or dispatch.current_revision(conn) is None:
            raise SyncError("名册尚未建立：请先建册再启用站点同步")
        cur = dispatch.current_revision(conn)
        sid_row = conn.execute(
            "SELECT value FROM sync_meta WHERE key='source_id'").fetchone()
        if sid_row is not None:
            base = dispatch.zone_site_map(conn)
            if site_zones is not None:
                wanted = dispatch.normalize_site_zones(site_zones)
                if wanted != base:
                    raise SyncError("同步已启用；区 -> 站点映射已锁定，不能再次改动")
            meta = sync_common.sync_meta(conn)
            conn.commit()
            return {"source_id": sid_row["value"],
                    "enabled_revision": int(meta["enabled_revision"]),
                    "current_revision": cur, "replayed": True}
        # 平台整理一次：显式映射优先，否则沿用建册时的静态映射
        if site_zones is not None:
            mapping = dispatch.normalize_site_zones(site_zones)
            live = {r["zone_id"] for r in conn.execute("SELECT zone_id FROM dz_zones")}
            bad = sorted(set(mapping) - live)
            if bad:
                raise SyncError(f"站点映射包含不存在的区: {bad}")
            sync_common.set_static_zone_sites(conn, mapping)
        else:
            mapping = dispatch.zone_site_map(conn)
        sync_common.set_published_zone_sites(conn, mapping)
        for site in sorted(set(mapping.values())):
            sync_common.register_site(conn, site, cur)
        source_id = _new_source_id(conn)
        conn.execute("INSERT INTO sync_meta(key, value) VALUES ('source_id', ?)",
                     (source_id,))
        conn.execute("INSERT INTO sync_meta(key, value) VALUES ('enabled_revision', ?)",
                     (str(cur),))
        conn.execute("INSERT INTO sync_meta(key, value) VALUES ('sync_revision', ?)",
                     (str(cur),))
        conn.execute("INSERT INTO sync_meta(key, value) VALUES ('singleton', '1')")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.isolation_level = old_level
    return {"source_id": source_id, "enabled_revision": cur,
            "current_revision": cur, "replayed": False,
            "sites": sorted(set(mapping.values()))}


# ---------------------------------------------------------------- 版本与身份


def _identity(conn) -> dict:
    meta = sync_common.sync_meta(conn)
    if "source_id" not in meta:
        raise SyncError("同步尚未启用：请先在中心库运行 enable")
    return {"source_id": meta["source_id"],
            "enabled_revision": int(meta["enabled_revision"]),
            "sync_revision": int(meta.get("sync_revision", meta["enabled_revision"]))}


class _read_snapshot:
    """导出用的只读一致性快照。

    连接上存在未提交写事务（例如 ``apply_changes(commit=False)``）时直接拒绝：
    未提交的改区不产生可导出的变化。否则开一个读事务，保证多步查询看到同一
    已提交版本。
    """

    def __init__(self, conn):
        self.conn = conn
        self.own = False

    def __enter__(self):
        if self.conn.in_transaction:
            raise SyncError(
                "连接上有未提交的写事务；未提交的改区不能导出（请先提交或回滚）")
        self.conn.execute("BEGIN")
        self.own = True
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        if self.own:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
        return False


def check_version(conn, revision, who="版本") -> int:
    """校验一个名册修订号是可导出的“已提交且启用以来”的版本。"""
    ident = _identity(conn)
    if isinstance(revision, bool) or not isinstance(revision, int):
        raise SyncError(f"{who}必须是整数修订号: {revision!r}")
    if revision < ident["enabled_revision"]:
        raise SyncError(
            f"{who} {revision} 早于启用版本 {ident['enabled_revision']}：启用前没有可导出历史")
    row = conn.execute("SELECT revision FROM roster_revisions WHERE revision=?",
                       (revision,)).fetchone()
    if row is None:
        raise SyncError(f"{who} {revision} 不是已提交的名册版本")
    if revision > dispatch.current_revision(conn):
        raise SyncError(f"{who} {revision} 尚未提交（当前修订号 "
                        f"{dispatch.current_revision(conn)}）")
    return revision


def _zone_site(conn) -> dict:
    # 发布映射可以为空（例如所有区都被删除）；空映射即“无任何站点持有地址”，
    # 不当作错误：站点登记在 sync_sites，导出删除项仍要能用。
    return sync_common.site_zone_map(conn)


def known_sites(conn):
    """已知站点：启用时登记的映射站点 + 之后出现过变化的站点。"""
    return sorted(sync_common.known_sites(conn))


def _require_site(conn, site_id):
    if not isinstance(site_id, str) or not site_id:
        raise SyncError("站点 ID 必须是非空字符串")
    if site_id not in sync_common.known_sites(conn):
        raise SyncError(f"未知站点 {site_id}：既没有映射也没有任何同步变化")
    return site_id


# ---------------------------------------------------------------- 导出

def _site_members_at(conn, site_id, revision):
    """站点在某已提交版本当前的成员集合。

    当前成员按当前区 -> 站点映射查（走 zone_id 索引）；再沿
    ``(site_id, source_row, sync_revision)`` 索引反演 (revision, 当前] 的事件，
    得到该版本的成员。只触及本站事件，不扫名册、不遍历其他站点。
    """
    cur = dispatch.current_revision(conn)
    if revision == cur:
        return dict(_current_site_rows(conn, site_id))
    out = dict(_current_site_rows(conn, site_id))
    rows = conn.execute(
        "SELECT source_row, op FROM sync_changes WHERE site_id=? "
        "AND sync_revision>? AND sync_revision<=? "
        "ORDER BY source_row, sync_revision DESC",
        (site_id, revision, cur)).fetchall()
    for r in rows:
        if r["op"] == "add":       # 正向加的 -> 反演时移除
            out.pop(r["source_row"], None)
        else:                      # 正向删的 -> 反演时加回
            out[r["source_row"]] = None
    return out


def _current_site_rows(conn, site_id):
    site_zones = [z for z, s in _zone_site(conn).items() if s == site_id]
    if not site_zones:
        return {}
    placeholders = ",".join("?" for _ in site_zones)
    rows = conn.execute(
        "SELECT r.source_row, r.zone_id FROM roster_records r "
        f"WHERE r.zone_id IN ({placeholders}) ORDER BY r.source_row",
        tuple(sorted(site_zones))).fetchall()
    return {r["source_row"]: r["zone_id"] for r in rows}


def _site_rows_at(conn, site_id, revision):
    """站点在某版本的成员及其名册坐标：{source_row: (lon, lat, zone_id)}。"""
    members = _site_members_at(conn, site_id, revision)
    if not members:
        return {}
    out = {}
    ordered = sorted(members)
    # 分块点查坐标（每块走主键索引），不读整册
    for i in range(0, len(ordered), 500):
        chunk = ordered[i:i + 500]
        q = ",".join("?" for _ in chunk)
        for r in conn.execute(
                "SELECT source_row, sel_lon, sel_lat FROM roster_addresses "
                f"WHERE source_row IN ({q})", tuple(chunk)):
            out[r["source_row"]] = (r["sel_lon"], r["sel_lat"], members[r["source_row"]])
    return out


def _initial_items(conn, site_id, revision):
    """初装包地址项：只含 source_row 与名册经纬度（有坐标的行才可派单）。"""
    items = []
    for sr, (lon, lat, _z) in sorted(_site_rows_at(conn, site_id, revision).items()):
        if lon is None or lat is None:
            continue
        items.append({"source_row": sr, "lon": lon, "lat": lat})
    return items


def _site_states(conn, site_id, from_revision, to_revision):
    """把一个区间内该站的变化折叠为 {source_row: (start_member, end_member)}。

    只触及 ``idx_sync_changes_site_rev`` 上 ``(site_id, sync_revision)`` 的区间，
    不读其他站点的行，也不逐修订号顺序遍历。事件只在实际换站时写入，因此
    某地址区间内第一条事件的方向就决定了它在起始版本是否属于本站；最后一条
    事件的方向决定目标版本。区间内无事件的地址不返回（两端相同，必然不发）。
    """
    rows = conn.execute(
        "SELECT source_row, sync_revision, op FROM sync_changes "
        "WHERE site_id=? AND sync_revision>? AND sync_revision<=?",
        (site_id, from_revision, to_revision)).fetchall()
    states = {}
    for r in rows:
        sr = r["source_row"]
        st = states.get(sr)
        if st is None:
            # 第一条事件：del 说明起始时在本站，add 说明起始时不在
            states[sr] = [r["op"] == "del", r["op"] == "add"]
        else:
            st[1] = r["op"] == "add"
    return {sr: (bool(st[0]), bool(st[1])) for sr, st in states.items()}


def export_patch(conn, site_id, from_revision, to_revision) -> dict:
    """导出站点在 [from_revision, to_revision] 之间的净变化补丁。

    全程在一个只读事务里完成，读到的是同一已提交版本；连接上有未提交写事务时
    直接拒绝（未提交的改区不产生可导出的变化）。
    """
    with _read_snapshot(conn):
        return _export_patch(conn, site_id, from_revision, to_revision)


def _export_patch(conn, site_id, from_revision, to_revision):
    _require_site(conn, site_id)
    ident = _identity(conn)
    check_version(conn, from_revision, "起始修订")
    check_version(conn, to_revision, "目标修订")
    if from_revision > to_revision:
        raise SyncError(
            f"起始修订 {from_revision} 大于目标修订 {to_revision}：拒绝倒序补丁")
    # 净差：只查该站点在该区间内的变化（索引区间，不扫名册）
    states = _site_states(conn, site_id, from_revision, to_revision)
    adds, dels = [], []
    for sr in sorted(states):
        start_member, end_member = states[sr]
        if start_member == end_member:
            continue                       # 两个端点都在或都不在：不发（含转了又回）
        if end_member:
            adds.append(sr)
        else:
            dels.append(sr)
    # 新增项需要完整地址行：按主键点查（不读整册）
    items = []
    for sr in adds:
        row = conn.execute(
            "SELECT a.sel_lon, a.sel_lat, r.zone_id FROM roster_addresses a "
            "JOIN roster_records r ON r.source_row=a.source_row "
            "WHERE a.source_row=?", (sr,)).fetchone()
        if row is None or row["sel_lon"] is None or row["sel_lat"] is None:
            continue
        items.append({"op": "add", "source_row": sr,
                      "lon": row["sel_lon"], "lat": row["sel_lat"],
                      "zone_id": row["zone_id"]})
    for sr in dels:
        items.append({"op": "del", "source_row": sr})
    items.sort(key=lambda it: it["source_row"])
    return {
        "protocol": PROTOCOL,
        "package_type": "patch",
        "source_id": ident["source_id"],
        "site_id": site_id,
        "from_revision": from_revision,
        "target_revision": to_revision,
        "items": items,
    }


def export_initial(conn, site_id, target_revision=None) -> dict:
    """导出初装包：站点在目标修订当前已提交的全部派单地址（只读事务）。"""
    with _read_snapshot(conn):
        return _export_initial(conn, site_id, target_revision)


def _export_initial(conn, site_id, target_revision):
    _require_site(conn, site_id)
    ident = _identity(conn)
    if target_revision is None:
        target_revision = dispatch.current_revision(conn)
    check_version(conn, target_revision, "目标修订")
    return {
        "protocol": PROTOCOL,
        "package_type": "initial",
        "source_id": ident["source_id"],
        "site_id": site_id,
        "target_revision": target_revision,
        "items": _initial_items(conn, site_id, target_revision),
    }


def package_text(pkg) -> str:
    return sync_common.dump_package(pkg)


def content_digest(pkg) -> str:
    return hashlib.sha256(sync_common.canon(pkg).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- CLI


def connect(path):
    return dispatch.connect(path)


def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def main(argv=None):
    p = argparse.ArgumentParser(
        prog="sync_center", description="中心端站点同步：启用 / 初装包 / 量补丁")
    sub = p.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("enable", help="在已有名册上启用同步（幂等）")
    sp.add_argument("db")
    sp.add_argument("--site-zones", help="区 -> 站点映射 JSON（启用时整理一次）")
    sp.add_argument("--out", help="把启用结果写入文件，默认标准输出")
    sp = sub.add_parser("export-initial", help="导出站点初装包")
    sp.add_argument("db"); sp.add_argument("site_id")
    sp.add_argument("--target", type=int, help="目标修订号，默认当前修订号")
    sp.add_argument("--out", required=True)
    sp = sub.add_parser("export-patch", help="导出站点净变化补丁")
    sp.add_argument("db"); sp.add_argument("site_id")
    sp.add_argument("--from", dest="from_revision", type=int, required=True)
    sp.add_argument("--to", dest="to_revision", type=int, required=True)
    sp.add_argument("--out", required=True)
    sp = sub.add_parser("sites", help="列出已知站点")
    sp.add_argument("db")
    args = p.parse_args(argv)

    conn = connect(args.db)
    try:
        if args.cmd == "enable":
            site_zones = (json.load(open(args.site_zones, encoding="utf-8"))
                          if args.site_zones else None)
            result = enable_sync(conn, site_zones)
            text = json.dumps(result, ensure_ascii=False, indent=2)
            if args.out:
                open(args.out, "w", encoding="utf-8").write(text + "\n")
            else:
                print(text)
            return result
        if args.cmd == "export-initial":
            pkg = export_initial(conn, args.site_id, args.target)
        elif args.cmd == "export-patch":
            pkg = export_patch(conn, args.site_id, args.from_revision,
                               args.to_revision)
        elif args.cmd == "sites":
            _print({"source_id": _identity(conn)["source_id"],
                    "sites": known_sites(conn)})
            return None
        else:
            raise SystemExit(f"unknown command {args.cmd}")
        open(args.out, "w", encoding="utf-8").write(package_text(pkg))
        _print({"ok": True, "package_type": pkg["package_type"],
                "site_id": pkg["site_id"], "target_revision": pkg["target_revision"],
                "item_count": len(pkg["items"]), "out": args.out})
        return pkg
    finally:
        conn.close()


if __name__ == "__main__":
    main()
