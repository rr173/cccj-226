"""站点同步的共用定义：包格式、中心库同步表与事件写入。

中心端（``sync_center.py``）、站点端（``sync_site.py``）与派单名册
（``dispatch.py``）共用的最小公共部分，避免循环导入。

同步包口径
----------
* 包类型 ``initial``（初装包）或 ``patch``（补丁）。
* 身份：``source_id``（发布源 ID，中心库内唯一且重启不变）+ ``site_id``。
* 版本：初装包带 ``target_revision``；补丁带 ``from_revision``/``target_revision``。
* 初装包地址项只有 ``source_row`` 与名册中的 ``lon``/``lat``（度数，REAL）；
  补丁的新增项还给 ``zone_id``（中心端当前归属，仅供核对，不是收件依据）。
  ``zone_id`` 为 ``null`` 表示该地址在目标版本不属于任何区。
* 地址项一律按 ``source_row`` 升序；同一请求对同一版本重复导出逐字节一致。
"""
from __future__ import annotations

import json

PROTOCOL = "dispatch-site-sync/1"
PACKAGE_TYPES = ("initial", "patch")

# ---------------------------------------------------------------- JSON 工具


def canon(obj) -> str:
    """规范 JSON（排序键、紧凑分隔符），用于导出比对与事件载荷。"""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def dump_package(pkg) -> str:
    """导出文本：字段顺序固定、缩进稳定、以换行结尾。"""
    return json.dumps(pkg, ensure_ascii=False, indent=2) + "\n"


def now() -> str:
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------- 中心库结构
# 逐条列出，便于在已打开的事务内用 conn.execute 建表（executescript 会隐式提交，
# 不能在 BEGIN IMMEDIATE 之后使用）。
SYNC_SCHEMA_STATEMENTS = (
    """CREATE TABLE IF NOT EXISTS sync_meta (
  key   TEXT PRIMARY KEY CHECK (key IN
          ('source_id', 'enabled_revision', 'sync_revision', 'singleton')),
  value TEXT NOT NULL
)""",
    # 启用时整理出的“静态映射”（区 -> 站点），只在启用事务内整体替换
    """CREATE TABLE IF NOT EXISTS sync_zone_sites (
  zone_id  TEXT PRIMARY KEY,
  site_id  TEXT NOT NULL
)""",
    """CREATE INDEX IF NOT EXISTS idx_sync_zone_sites_site
  ON sync_zone_sites(site_id, zone_id)""",
    # 启用后随改区演进的发布映射（与名册同事务更新，可删区）
    """CREATE TABLE IF NOT EXISTS sync_zone_site (
  zone_id  TEXT PRIMARY KEY,
  site_id  TEXT NOT NULL
)""",
    # 已知站点登记：启用时登记映射中的站点；之后任何站点首次出现变化时登记。
    # 即使该站的区已全部从区表删除，仍能从本表认出它（导出删除项要用）。
    """CREATE TABLE IF NOT EXISTS sync_sites (
  site_id        TEXT PRIMARY KEY,
  first_revision INTEGER NOT NULL
)""",
    """CREATE TABLE IF NOT EXISTS sync_changes (
  sync_revision INTEGER NOT NULL,
  site_id       TEXT NOT NULL,
  source_row    TEXT NOT NULL,
  op            TEXT NOT NULL CHECK (op IN ('add', 'del')),
  PRIMARY KEY (sync_revision, site_id, source_row)
)""",
    # 补丁导出按 (site_id, source_row, sync_revision) 定位：不随其他站点或空修订增长
    """CREATE INDEX IF NOT EXISTS idx_sync_changes_site_row_rev
  ON sync_changes(site_id, source_row, sync_revision)""",
    """CREATE INDEX IF NOT EXISTS idx_sync_changes_site_rev
  ON sync_changes(site_id, sync_revision)""",
)

SYNC_SCHEMA = ";\n".join(SYNC_SCHEMA_STATEMENTS) + ";\n"


def ensure_sync_tables(conn) -> None:
    """幂等建表；可在已打开的事务内安全调用（不隐式提交）。"""
    for stmt in SYNC_SCHEMA_STATEMENTS:
        conn.execute(stmt)


def sync_enabled(conn) -> bool:
    """中心库是否已启用同步（只查 sync_meta，不依赖任何全表扫描）。"""
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='sync_meta'").fetchone() is None:
        return False
    return conn.execute("SELECT 1 FROM sync_meta WHERE key='source_id'"
                        ).fetchone() is not None


def site_zone_map(conn) -> dict:
    """中心库的区 -> 站点发布映射（启用时整理出的静态映射）。"""
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='sync_zone_site'").fetchone() is None:
        return {}
    return {r["zone_id"]: r["site_id"] for r in conn.execute(
        "SELECT zone_id, site_id FROM sync_zone_site")}


def _old_site(conn, old_site_map, zone_id):
    """批次前的区所属站点；映射里找不到时回落到静态映射（兼容手工映射）。"""
    if zone_id is None:
        return None
    sid = old_site_map.get(zone_id)
    if sid is not None:
        return sid
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='dz_zone_sites'").fetchone() is not None:
        r = conn.execute("SELECT site_id FROM dz_zone_sites WHERE zone_id=?",
                         (zone_id,)).fetchone()
        if r is not None:
            return r["site_id"]
    return None


def sync_meta(conn) -> dict:
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='sync_meta'").fetchone() is None:
        return {}
    return {r["key"]: r["value"]
            for r in conn.execute("SELECT key, value FROM sync_meta")}


def _insert_change(conn, sync_revision, site_id, source_row, op):
    """写一条站点净变化；同一修订内按 (站点, 地址) 合并，净零则删除。"""
    old = conn.execute(
        "SELECT op FROM sync_changes WHERE sync_revision=? AND site_id=? "
        "AND source_row=?", (sync_revision, site_id, source_row)).fetchone()
    if old is None:
        conn.execute(
            "INSERT INTO sync_changes(sync_revision, site_id, source_row, op) "
            "VALUES (?,?,?,?)", (sync_revision, site_id, source_row, op))
        return
    if old["op"] == op:
        return
    # 同一修订内先 add 后 del（或反之）= 净零：两个端点都在/都不在该站，不发
    conn.execute(
        "DELETE FROM sync_changes WHERE sync_revision=? AND site_id=? "
        "AND source_row=?", (sync_revision, site_id, source_row))


def known_sites(conn):
    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='sync_sites'").fetchone() is None:
        return set()
    return {r["site_id"] for r in conn.execute("SELECT site_id FROM sync_sites")}


def register_site(conn, site_id, revision) -> None:
    conn.execute(
        "INSERT INTO sync_sites(site_id, first_revision) VALUES (?,?) "
        "ON CONFLICT(site_id) DO NOTHING", (site_id, revision))


def set_published_zone_sites(conn, mapping) -> None:
    """启用时由静态映射整理出发布映射 ``sync_zone_site``（整体替换）。"""
    conn.execute("DELETE FROM sync_zone_site")
    for zid, sid in sorted(mapping.items()):
        conn.execute("INSERT INTO sync_zone_site(zone_id, site_id) VALUES (?,?)",
                     (zid, sid))


def set_static_zone_sites(conn, mapping) -> None:
    """整理静态映射 ``dz_zone_sites``（整体替换）。"""
    conn.execute("DELETE FROM dz_zone_sites")
    for zid, sid in sorted(mapping.items()):
        conn.execute("INSERT INTO dz_zone_sites(zone_id, site_id) VALUES (?,?)",
                     (zid, sid))


def record_dispatch_delta(conn, new_revision, changed, old_site_by_zone=None) -> None:
    """在派单事务中记录本批次的站点净变化。

    ``changed`` 是 ``[(old_zone_id_or_None, [source_row, ...]), ...]``：
    重算前归属的区（None = 不在任何区）与该归属下本批重算过的地址。
    ``old_site_by_zone`` 是批次提交前的区 -> 站点映射快照（删区后仍能定位
    原站，保证原站收到删除）。新站点按批次最终映射（``sync_zone_site``）查。
    只在站点变化时写 ``del``（旧站）与 ``add``（新站）；同修订内合并，净零不发。
    """
    old_site_by_zone = old_site_by_zone or {}
    for old_zone, rows in changed:
        if not rows:
            continue
        old_site = _old_site(conn, old_site_by_zone, old_zone)
        for sr in rows:
            rec = conn.execute("SELECT zone_id FROM roster_records WHERE source_row=?",
                               (sr,)).fetchone()
            new_zone = rec["zone_id"] if rec else None
            new_site = None
            if new_zone is not None:
                r = conn.execute("SELECT site_id FROM sync_zone_site WHERE zone_id=?",
                                 (new_zone,)).fetchone()
                if r is not None:
                    new_site = r["site_id"]
                else:
                    # 批次内新引入、批次前就有静态映射的区（也兼容手工映射）
                    old_zone_map = conn.execute(
                        "SELECT site_id FROM dz_zone_sites WHERE zone_id=?",
                        (new_zone,)).fetchone()
                    new_site = old_zone_map["site_id"] if old_zone_map else None
            if old_site == new_site:
                continue
            if old_site is not None:
                register_site(conn, old_site, new_revision)
                _insert_change(conn, new_revision, old_site, sr, "del")
            if new_site is not None:
                register_site(conn, new_site, new_revision)
                _insert_change(conn, new_revision, new_site, sr, "add")
