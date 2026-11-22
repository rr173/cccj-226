"""局部可维护的派单名册（dispatch roster）。

在归并器（merger.py）所在的同一个 SQLite 数据库中，以首次建册时固定的一个
*成功* 地理编码快照为地址全集（该快照 selections 的全部 source_row），把每个
地址派给负责的配送区；之后运营只提交改动区，调度端即可读到同一版本下完整的
逐地址归属，且局部改区只重算受影响地址，不顺序扫描整批地址、不重建全表。

坐标与几何约定
--------------
* 多边形顶点是整数微度（microdegree，1e-6 度），顺序为 ``[经度, 纬度]``；
  在普通经纬度平面内做直线判定，拒绝跨越日期变更线的边。
* 地址点取固定快照 selections 中已存的 ``sel_lon/sel_lat``（REAL），按其
  Python 十进制字符串值精确换算到微度尺度（Decimal，不四舍五入到格点、
  不使用容差），判定全程精确（叉积/乘法），贴边点按规则入区。
* 外环边线算覆盖；孔内部与孔边线均不算覆盖；绕向不影响结果。
* 重叠时优先级数值最大者胜；同级按区 ID 字符串升序最前者胜。

记录状态：``assigned``（有坐标且匹配到区，带 zone_id）、``uncovered``
（有坐标但无区覆盖）、``no_coordinate``（无选定坐标；有待决原因但已有选定
坐标的行照常分区）。相同坐标的不同 source_row 是互相独立的派单记录。

版本与并发
----------
* 首次建册产生 revision=1；每个成功批次只增加一个修订号。
* 改区须带预期修订号；过期号、空批次、删除不存在的区、同批重复区、非法
  几何一律整体拒绝，名册不变。
* 区表、外包矩形定位状态（roster_addresses 的精确坐标列 + 索引）、派单记录、
  修订号在同一个事务/同一份 WAL 中一起生效；读连接只能看到完整旧版或完整
  新版。写连接以 ``BEGIN IMMEDIATE`` 抢保留锁，同一预期号的并发写至多一个
  成功；写进程提交前退出，事务随连接关闭而回滚，可从上一版继续。

站点同步
--------
建册时可同时给出 ``site_zones``（区 ID -> 站点 ID，或按站点给区 ID 列表），
静态映射存于 ``dz_zone_sites``。中心库启用同步后，每次改区在同一事务中把
“地址换了站点”的净变化写入 ``sync_changes``（见 ``sync_common``）：只针对
本批重算过的地址，按旧/新站点比较，不遍历整册，也不为无变化的修订写事件。
站点以区 ID 标识，与多边形相交/覆盖签名无关。

merger 的原有表与触发器一律不改动、不写入。
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from decimal import Decimal, ROUND_FLOOR, getcontext

import sync_common

# 判定中出现的小数位数有界（坐标串约 17 位 + 微度换算 6 位），50 位足够精确
getcontext().prec = 50

MICRO = Decimal(1_000_000)

# ---------------------------------------------------------------- 几何（精确）

class GeometryError(ValueError):
    """非法多边形几何。"""


def _as_int_vertex(v):
    if not isinstance(v, list) or len(v) != 2:
        raise GeometryError(f"顶点必须是 [经度, 纬度] 二元列表: {v!r}")
    x, y = v
    if isinstance(x, bool) or isinstance(y, bool) or not isinstance(x, int) or not isinstance(y, int):
        raise GeometryError(f"顶点必须是整数微度: {v!r}")
    if not (-180 * 1_000_000 <= x <= 180 * 1_000_000) \
            or not (-90 * 1_000_000 <= y <= 90 * 1_000_000):
        raise GeometryError(f"顶点超出经纬度范围: {v!r}")
    return x, y


def _cross(ax, ay, bx, by, cx, cy):
    """叉积 (B-A) x (C-A)，整数精确。"""
    return (bx - ax) * (cy - ay) - (by - ay) * (cx - ax)


def _on_seg(px, py, ax, ay, bx, by):
    """点 P 是否落在闭线段 AB 上（整数精确，共线 + 包围盒内）。"""
    if _cross(ax, ay, bx, by, px, py) != 0:
        return False
    return (min(ax, bx) <= px <= max(ax, bx)
            and min(ay, by) <= py <= max(ay, by))


def _segments_intersect(ax, ay, bx, by, cx, cy, dx, dy):
    """两条闭线段是否相交或接触（含端点相接、部分重叠），整数精确。"""
    if _on_seg(ax, ay, cx, cy, dx, dy) or _on_seg(bx, by, cx, cy, dx, dy) \
            or _on_seg(cx, cy, ax, ay, bx, by) or _on_seg(dx, dy, ax, ay, bx, by):
        return True
    c1 = _cross(ax, ay, bx, by, cx, cy)
    c2 = _cross(ax, ay, bx, by, dx, dy)
    c3 = _cross(cx, cy, dx, dy, ax, ay)
    c4 = _cross(cx, cy, dx, dy, bx, by)
    return ((c1 > 0) != (c2 > 0)) and ((c3 > 0) != (c4 > 0))


def _ring_area2(ring):
    """鞋带公式有向面积的两倍（整数精确）。"""
    a2 = 0
    for i in range(len(ring) - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        a2 += x1 * y2 - x2 * y1
    return a2


def _check_ring(ring, label):
    """单环校验：闭合、至少 3 个不同顶点、无退化边、无自交、不跨日期变更线。

    返回去掉闭合重复点后的顶点表与闭合外包矩形 (minx, miny, maxx, maxy)。
    """
    if not isinstance(ring, list) or not ring:
        raise GeometryError(f"{label} 必须是非空顶点列表")
    pts = [_as_int_vertex(v) for v in ring]
    if pts[0] != pts[-1]:
        raise GeometryError(f"{label} 首尾必须闭合")
    pts = pts[:-1]
    if len(pts) < 3:
        raise GeometryError(f"{label} 至少需要 3 个不同顶点")
    if len(set(pts)) != len(pts):
        raise GeometryError(f"{label} 存在重复顶点（退化）")
    n = len(pts)
    for i in range(n):
        if pts[i] == pts[(i + 1) % n]:
            raise GeometryError(f"{label} 存在零长度边（退化）")
    # 不支持跨日期变更线：任何一条边的经度跨度都不得 >= 180 度
    for i in range(n):
        x1, _ = pts[i]
        x2, _ = pts[(i + 1) % n]
        if abs(x2 - x1) >= 180 * 1_000_000:
            raise GeometryError(f"{label} 存在跨越日期变更线的边，不支持")
    # 非相邻边不得相交或接触（相邻边只允许在公共端点相接）
    for i in range(n):
        a, b = pts[i], pts[(i + 1) % n]
        for j in range(i + 1, n):
            c, d = pts[j], pts[(j + 1) % n]
            adjacent = (j == i + 1) or (i == 0 and j == n - 1)
            if adjacent:
                shared = b if j == i + 1 else a
                other = d if j == i + 1 else c
                # 相邻边除公共端点外不得有任何接触（防止共线重叠等退化）
                if other != shared and _on_seg(other[0], other[1], a[0], a[1], b[0], b[1]):
                    raise GeometryError(f"{label} 相邻边在公共端点之外接触（自交/退化）")
                continue
            if _segments_intersect(a[0], a[1], b[0], b[1], c[0], c[1], d[0], d[1]):
                raise GeometryError(f"{label} 非相邻边相交或接触（自交）")
    if _ring_area2(pts + [pts[0]]) == 0:
        raise GeometryError(f"{label} 面积为零（退化）")
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    return pts, (min(xs), min(ys), max(xs), max(ys))


def _point_in_ring(px, py, ring):
    """点在环内或环边上（精确，绕向无关）。px/py 为 Decimal，环顶点为 int。"""
    n = len(ring)
    inside = False
    for i in range(n):
        ax, ay = ring[i]
        bx, by = ring[(i + 1) % n]
        if _on_seg(px, py, ax, ay, bx, by):          # 边线（含端点、共线重叠）
            return True
        # 水平射线法（半开区间，保证顶点只被一条边计数，避免穿顶点双重翻转）
        if (ay > py) != (by > py):
            # 交点 x* = ax + (bx-ax)*(py-ay)/(by-ay)，射线条件 px < x*。
            # 交叉相乘消去除法，并按分母 (by-ay) 的符号决定不等式方向；
            # 全程 Decimal/int 精确运算，无浮点、无容差（贴边点先由 _on_seg 命中）。
            lhs = px * (by - ay)
            rhs = ax * (by - ay) + (bx - ax) * (py - ay)
            if (lhs < rhs) if (by - ay) > 0 else (lhs > rhs):
                inside = not inside
    return inside


def _bbox_contains(bbox, px, py):
    minx, miny, maxx, maxy = bbox
    return minx <= px <= maxx and miny <= py <= maxy


def validate_polygon(geometry):
    """校验带可选孔洞的多边形，返回规整后的 {outer, holes, bbox}。

    孔须严格处于外环内部，孔与孔之间不能接触、相交或嵌套。
    输入::

        {"outer": [[lon, lat], ...], "holes": [[[lon, lat], ...], ...]}
    """
    if not isinstance(geometry, dict) or "outer" not in geometry:
        raise GeometryError("geometry 必须是含 outer 的对象")
    outer, outer_bbox = _check_ring(geometry["outer"], "外环")
    holes_in = geometry.get("holes") or []
    if not isinstance(holes_in, list):
        raise GeometryError("holes 必须是环列表")
    holes = []
    for k, h in enumerate(holes_in):
        ring, _bbox = _check_ring(h, f"孔#{k}")
        # 孔严格处于外环内部：孔的每个顶点都必须落在外环 *内部*，
        # 落在外环边线上即视为接触，拒绝（边线点会先在 _point_in_ring 命中）
        for vx, vy in ring:
            px, py = Decimal(vx), Decimal(vy)
            if not _point_in_ring(px, py, outer):
                raise GeometryError(f"孔#{k} 不在外环内部")
            if any(_on_seg(px, py, outer[t][0], outer[t][1],
                           outer[(t + 1) % len(outer)][0],
                           outer[(t + 1) % len(outer)][1])
                   for t in range(len(outer))):
                raise GeometryError(f"孔#{k} 与外环边线接触，孔须严格处于外环内部")
        holes.append(ring)
    # 孔之间：不得接触、相交、嵌套（各自顶点均不得落在对方环内或环边上）
    for i in range(len(holes)):
        for j in range(i + 1, len(holes)):
            ri, rj = holes[i], holes[j]
            ni, nj = len(ri), len(rj)
            for a in range(ni):
                ax, ay = ri[a]
                bx, by = ri[(a + 1) % ni]
                for b in range(nj):
                    cx, cy = rj[b]
                    dx, dy = rj[(b + 1) % nj]
                    if _segments_intersect(ax, ay, bx, by, cx, cy, dx, dy):
                        raise GeometryError(f"孔#{i} 与孔#{j} 相交或接触")
            if _point_in_ring(Decimal(rj[0][0]), Decimal(rj[0][1]), ri):
                raise GeometryError(f"孔#{j} 嵌套在孔#{i} 内")
            if _point_in_ring(Decimal(ri[0][0]), Decimal(ri[0][1]), rj):
                raise GeometryError(f"孔#{i} 嵌套在孔#{j} 内")
    return {"outer": outer + [outer[0]],
            "holes": [h + [h[0]] for h in holes], "bbox": outer_bbox}


def point_covered(px, py, polygon) -> bool:
    """点是否被多边形覆盖：外环内/边上且不在任何孔内或孔边上（精确，绕向无关）。"""
    if not _point_in_ring(px, py, polygon["outer"]):
        return False
    for hole in polygon["holes"]:
        if _point_in_ring(px, py, hole):
            return False
    return True


def decimal_micro(value) -> Decimal:
    """把已存 REAL 坐标按 Python 十进制字符串精确换算到微度尺度（不四舍五入）。"""
    d = Decimal(str(value)) * MICRO
    # 精确为整数微度时规范化为整数字符串（如 100500000，而非 100500000.0），
    # 非整数微度保留全部小数位，绝不格点化
    return d.quantize(Decimal(1)) if d == d.to_integral_value() else d


# ---------------------------------------------------------------- 区与批次

class RosterError(ValueError):
    """名册业务规则错误（预期修订号、空批次、删除不存在等）。"""


def _validate_zone(zone_id, priority, geometry):
    if not isinstance(zone_id, str) or not zone_id:
        raise GeometryError(f"区 ID 必须是非空字符串: {zone_id!r}")
    if isinstance(priority, bool) or not isinstance(priority, int):
        raise GeometryError(f"区 {zone_id} 的 priority 必须是整数: {priority!r}")
    return validate_polygon(geometry)


def normalize_zones(zones) -> dict:
    """校验完整区表 {zone_id: {priority, geometry}}，返回 {zone_id: 持久 dict}。"""
    if not isinstance(zones, dict) or not zones:
        raise RosterError("区表必须是非空对象 {zone_id: {priority, geometry}}")
    out = {}
    for zid, z in zones.items():
        if not isinstance(z, dict) or "priority" not in z or "geometry" not in z:
            raise GeometryError(f"区 {zid} 必须含 priority 与 geometry")
        poly = _validate_zone(zid, z["priority"], z["geometry"])
        out[zid] = {"zone_id": zid, "priority": z["priority"],
                    "geometry_json": json.dumps(poly, separators=(",", ":")),
                    "bbox": poly["bbox"]}
    return out


def _loads_no_duplicates(text):
    """JSON 解析，对象内重复键直接报错（用于识别同批重复出现的区 ID）。"""
    def hook(pairs):
        seen = set()
        for k, _ in pairs:
            if k in seen:
                raise RosterError(f"批次 JSON 存在重复键: {k!r}")
            seen.add(k)
        return dict(pairs)
    return json.loads(text, object_pairs_hook=hook)


def normalize_batch(payload) -> dict:
    """校验改区批次。

    接受两种形态：
    * 完整文件对象 ``{"expected_revision": N, "changes": {zone_id: 区|null}}``
    * 仅 changes 子对象 ``{zone_id: 区|null}``（键重复同样被拒绝）
    区值为 null = 删除；否则是新增/替换的完整区（priority + geometry）。
    """
    if isinstance(payload, str):
        payload = _loads_no_duplicates(payload)
    if not isinstance(payload, dict):
        raise RosterError("批次必须是 JSON 对象")
    if "changes" in payload:
        changes = payload["changes"]
        expected = payload.get("expected_revision")
    else:
        changes, expected = payload, None
    if not isinstance(changes, dict) or not changes:
        raise RosterError("空批次：必须至少包含一个改动区")
    norm = {}
    for zid, z in changes.items():
        if not isinstance(zid, str) or not zid:
            raise GeometryError(f"区 ID 必须是非空字符串: {zid!r}")
        if z is None:
            norm[zid] = None
        else:
            if not isinstance(z, dict) or "priority" not in z or "geometry" not in z:
                raise GeometryError(f"区 {zid} 必须含 priority 与 geometry")
            poly = _validate_zone(zid, z["priority"], z["geometry"])
            norm[zid] = {"zone_id": zid, "priority": z["priority"],
                         "geometry_json": json.dumps(poly, separators=(",", ":")),
                         "bbox": poly["bbox"]}
    return {"expected_revision": expected, "changes": norm}


# ---------------------------------------------------------------- 数据库结构

SCHEMA = """
CREATE TABLE IF NOT EXISTS dz_meta (
  key   TEXT PRIMARY KEY CHECK (key IN ('snapshot_id', 'revision')),
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dz_zones (
  zone_id       TEXT PRIMARY KEY,
  priority      INTEGER NOT NULL,
  geometry_json TEXT NOT NULL,
  bbox_minx     INTEGER NOT NULL,
  bbox_miny     INTEGER NOT NULL,
  bbox_maxx     INTEGER NOT NULL,
  bbox_maxy     INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS roster_addresses (
  source_row    TEXT PRIMARY KEY,
  lon_text      TEXT,
  lat_text      TEXT,
  x_floor       INTEGER,
  y_floor       INTEGER,
  sel_lon       REAL,
  sel_lat       REAL
);
-- 外包矩形定位索引：改区按 (x_floor, y_floor) 区间索引查询，不扫整表
CREATE INDEX IF NOT EXISTS idx_roster_xy
  ON roster_addresses(x_floor, y_floor);
CREATE TABLE IF NOT EXISTS roster_records (
  source_row      TEXT PRIMARY KEY REFERENCES roster_addresses(source_row),
  status          TEXT NOT NULL CHECK (status IN
                    ('assigned', 'uncovered', 'no_coordinate')),
  zone_id         TEXT REFERENCES dz_zones(zone_id),
  last_revision   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS roster_revisions (
  revision       INTEGER PRIMARY KEY,
  snapshot_id    INTEGER NOT NULL,
  kind           TEXT NOT NULL CHECK (kind IN ('initialize', 'batch')),
  detail_json    TEXT NOT NULL,
  created_at     TEXT NOT NULL
);
-- 区 -> 站点静态映射（站点以区 ID 标识）。建册时写入；启用同步后锁定。
CREATE TABLE IF NOT EXISTS dz_zone_sites (
  zone_id  TEXT PRIMARY KEY,
  site_id  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dz_zone_sites_site
  ON dz_zone_sites(site_id, zone_id);
"""


def connect(path) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def roster_exists(conn) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='dz_meta'").fetchone() is not None


def current_revision(conn) -> int | None:
    r = conn.execute("SELECT value FROM dz_meta WHERE key='revision'").fetchone()
    return int(r["value"]) if r else None


def fixed_snapshot_id(conn) -> int | None:
    r = conn.execute("SELECT value FROM dz_meta WHERE key='snapshot_id'").fetchone()
    return int(r["value"]) if r else None


def _now() -> str:
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def normalize_site_zones(site_zones, zone_ids=()):
    """规整区 -> 站点映射。

    接受两种形态：
    * ``{"Z1": "SITE_A", ...}``（区 ID -> 站点 ID）
    * ``{"SITE_A": ["Z1", "Z2"], ...}``（站点 ID -> 区 ID 列表）
    返回 ``{zone_id: site_id}``。一个区只能属于一个站点；同一站点重复列出
    同一个区视为同一事实（幂等），不同站点抢同一个区则报错。
    """
    if site_zones is None:
        return {}
    if not isinstance(site_zones, dict):
        raise RosterError("site_zones 必须是对象")
    known = set(zone_ids)
    out = {}

    def put(zid, sid):
        if not isinstance(zid, str) or not zid:
            raise RosterError(f"区 ID 必须是非空字符串: {zid!r}")
        if not isinstance(sid, str) or not sid:
            raise RosterError(f"站点 ID 必须是非空字符串: {sid!r}")
        prev = out.get(zid)
        if prev is not None and prev != sid:
            raise RosterError(f"区 {zid} 同时被映射到站点 {prev} 与 {sid}")
        out[zid] = sid

    for k, v in site_zones.items():
        if isinstance(v, str):
            put(k, v)
        elif isinstance(v, list) and all(isinstance(x, str) for x in v):
            for zid in v:
                put(zid, k)
        else:
            raise RosterError(
                f"site_zones 值必须是站点 ID 或区 ID 列表: {k}={v!r}")
    if known:
        bad = sorted(set(out) - known)
        if bad:
            raise RosterError(f"站点映射包含不在区表中的区: {bad}")
    return out


def zone_site_map(conn) -> dict:
    """读区 -> 站点映射。

    名册自身的静态映射在建册时写入 ``dz_zone_sites``；同步启用后，中心端的
    发布映射 ``sync_zone_site`` 是唯一有效来源（启用时整体整理而来，此后随
    改区演进、可删区）。两者都没有时返回空映射。
    """
    tables = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    if "sync_meta" in tables and conn.execute(
            "SELECT 1 FROM sync_meta WHERE key='source_id'").fetchone() is not None:
        if "sync_zone_site" in tables:
            return {r["zone_id"]: r["site_id"] for r in conn.execute(
                "SELECT zone_id, site_id FROM sync_zone_site")}
        return {}
    if "dz_zone_sites" not in tables:
        return {}
    return {r["zone_id"]: r["site_id"] for r in conn.execute(
        "SELECT zone_id, site_id FROM dz_zone_sites")}


def _load_polygons(conn):
    polys = {}
    for z in conn.execute("SELECT * FROM dz_zones"):
        poly = json.loads(z["geometry_json"])
        polys[z["zone_id"]] = {"priority": z["priority"], "poly": poly}
    return polys


def _winner(px, py, polys):
    """对一个点按 优先级降序、区 ID 升序 选出覆盖区；无覆盖返回 None。"""
    candidates = [zid for zid, z in polys.items() if point_covered(px, py, z["poly"])]
    if not candidates:
        return None
    # max 的 key：优先级越大越优；同级 ID 字符串升序最前者胜
    return max(candidates, key=lambda zid: (polys[zid]["priority"], _Rev(zid)))


class _Rev:
    """字符串反向比较包装，使 max(..., key=(priority, _Rev(id))) 取 ID 升序最前者。"""
    __slots__ = ("s",)

    def __init__(self, s):
        self.s = s

    def __lt__(self, other):
        return self.s > other.s

    def __eq__(self, other):
        return self.s == other.s


def _assign(px, py, polys):
    """返回 (status, zone_id)。"""
    zid = _winner(px, py, polys)
    return ("assigned", zid) if zid else ("uncovered", None)


# ---------------------------------------------------------------- 首次建册

def build_roster(conn, zones, snapshot_id=None, site_zones=None) -> dict:
    """首次建册：固定一个成功快照，以其 selections 为地址全集，产生 revision 1。

    ``site_zones`` 可选：区 -> 站点静态映射（见 ``normalize_site_zones``）；
    与区表在同一个事务里写入 ``dz_zone_sites``。
    """
    norm = normalize_zones(zones)
    site_map = normalize_site_zones(site_zones, zone_ids=norm.keys())
    if roster_exists(conn) and current_revision(conn) is not None:
        raise RosterError("名册已存在：建册只能执行一次，后续改动请走改区批次")
    if snapshot_id is None:
        r = conn.execute("SELECT snapshot_id FROM snapshots WHERE status='ok' "
                         "ORDER BY snapshot_id DESC LIMIT 1").fetchone()
        if r is None:
            raise RosterError("数据库中尚无可读（status='ok'）快照，无法建册")
        snapshot_id = r["snapshot_id"]
    snap = conn.execute("SELECT snapshot_id, status FROM snapshots WHERE snapshot_id=?",
                        (snapshot_id,)).fetchone()
    if snap is None or snap["status"] != "ok":
        raise RosterError(f"快照 {snapshot_id} 不存在或不是成功快照，不能作为名册地址全集")
    sels = conn.execute(
        "SELECT source_row, sel_lon, sel_lat, pending_reasons_json FROM selections "
        "WHERE snapshot_id=? ORDER BY source_row", (snapshot_id,)).fetchall()
    if not sels:
        raise RosterError(f"快照 {snapshot_id} 没有 selections，地址全集为空，拒绝建册")
    # 建表 DDL 在事务外执行（IF NOT EXISTS 幂等）；随后所有写入在一个事务内原子提交
    old_level = conn.isolation_level
    conn.isolation_level = None
    conn.executescript(SCHEMA)
    sync_common.ensure_sync_tables(conn)
    conn.execute("BEGIN IMMEDIATE")
    try:
        # 外键检查延迟到提交点：改区时先删区后重写派单，提交时引用必须重新有效
        conn.execute("PRAGMA defer_foreign_keys=ON")
        if current_revision(conn) is not None:
            raise RosterError("名册已存在：建册只能执行一次，后续改动请走改区批次")
        for zid, z in norm.items():
            minx, miny, maxx, maxy = z["bbox"]
            conn.execute("INSERT INTO dz_zones VALUES (?,?,?,?,?,?,?)",
                         (zid, z["priority"], z["geometry_json"],
                          minx, miny, maxx, maxy))
        for zid, sid in site_map.items():
            conn.execute("INSERT INTO dz_zone_sites(zone_id, site_id) VALUES (?,?)",
                         (zid, sid))
        polys = _load_polygons(conn)
        n_assigned = n_uncovered = n_no_coord = 0
        for s in sels:
            sr = s["source_row"]
            if s["sel_lon"] is None or s["sel_lat"] is None:
                conn.execute("INSERT INTO roster_addresses(source_row) VALUES (?)", (sr,))
                conn.execute(
                    "INSERT INTO roster_records VALUES (?, 'no_coordinate', NULL, 1)", (sr,))
                n_no_coord += 1
                continue
            # 以已存坐标的 Python 十进制字符串精确换算，不回查供应商、不跟随新快照
            px = decimal_micro(s["sel_lon"])
            py = decimal_micro(s["sel_lat"])
            # 外包矩形定位用 floor（负坐标同样向下取整，配合闭区间过滤不丢贴边点）
            fx = int(px.to_integral_value(rounding=ROUND_FLOOR))
            fy = int(py.to_integral_value(rounding=ROUND_FLOOR))
            conn.execute(
                "INSERT INTO roster_addresses"
                "(source_row,lon_text,lat_text,x_floor,y_floor,sel_lon,sel_lat)"
                " VALUES (?,?,?,?,?,?,?)",
                (sr, str(px), str(py), fx, fy, s["sel_lon"], s["sel_lat"]))
            status, zid = _assign(px, py, polys)   # 有待决原因但有坐标的行照常分区
            conn.execute("INSERT INTO roster_records VALUES (?,?,?,1)",
                         (sr, status, zid))
            if status == "assigned":
                n_assigned += 1
            else:
                n_uncovered += 1
        conn.execute("INSERT INTO dz_meta VALUES ('snapshot_id', ?)", (str(snapshot_id),))
        conn.execute("INSERT INTO dz_meta VALUES ('revision', '1')")
        conn.execute(
            "INSERT INTO roster_revisions VALUES (1, ?, 'initialize', ?, ?)",
            (snapshot_id, json.dumps({"zone_count": len(norm)}, separators=(",", ":")),
             _now()))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.isolation_level = old_level
    return {"snapshot_id": snapshot_id, "revision": 1,
            "addresses": len(sels), "assigned": n_assigned,
            "uncovered": n_uncovered, "no_coordinate": n_no_coord,
            "zones": len(norm), "site_zones": len(site_map)}


def set_site_zones(conn, site_zones) -> dict:
    """替换区 -> 站点静态映射（仅供 ``sync_center.enable_sync`` 的一次性整理使用）。

    同步启用后区映射被锁定；普通改区不经此函数。
    """
    mapping = normalize_site_zones(site_zones)
    old_level = conn.isolation_level
    conn.isolation_level = None
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("CREATE TABLE IF NOT EXISTS dz_zone_sites ("
                     " zone_id TEXT PRIMARY KEY, site_id TEXT NOT NULL)")
        conn.execute("DELETE FROM dz_zone_sites")
        for zid, sid in mapping.items():
            conn.execute("INSERT INTO dz_zone_sites(zone_id, site_id) VALUES (?,?)",
                         (zid, sid))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.isolation_level = old_level
    return {"zones": len(mapping)}


# ---------------------------------------------------------------- 局部改区

def _candidates_in_bbox(conn, bbox):
    """按闭合外包矩形用索引定位有坐标地址；无坐标行永不参与重算。"""
    minx, miny, maxx, maxy = bbox
    return conn.execute(
        "SELECT source_row, lon_text, lat_text FROM roster_addresses "
        "WHERE x_floor BETWEEN ? AND ? AND y_floor BETWEEN ? AND ? "
        "  AND lon_text IS NOT NULL",
        (minx, maxx, miny, maxy)).fetchall()


def apply_changes(conn, batch, expected_revision=None, commit=True) -> dict:
    """应用一个改区批次。

    全部输入校验（几何、空批次、同批重复、删除目标存在）在任何写操作之前完成；
    重算范围 = 每个改动区各自 *旧外环 bbox ∪ 新外环 bbox*（逐区独立矩形，
    不合并成横跨两处的大矩形）；范围内按批次最终区表整体重新判定，范围外
    记录一律不重算、不覆写。``commit=False`` 时停在提交点（用于提交前崩溃
    演练），调用方负责 commit/rollback。
    """
    if isinstance(batch, str):
        norm = normalize_batch(batch)
    elif isinstance(batch, dict) and "changes" in batch and batch["changes"] and \
            all(v is None or "geometry_json" in v for v in batch["changes"].values()):
        norm = batch                                   # 已是规整批次
    else:
        norm = normalize_batch(batch)
    if expected_revision is None:
        expected_revision = norm.get("expected_revision")
    changes = norm["changes"]
    if expected_revision is None or isinstance(expected_revision, bool) \
            or not isinstance(expected_revision, int):
        raise RosterError("必须提供整数预期名册修订号 expected_revision")

    old_level = conn.isolation_level
    conn.isolation_level = None
    try:
        conn.execute("BEGIN IMMEDIATE")          # 抢保留锁：并发写至多一个成功
        conn.execute("PRAGMA defer_foreign_keys=ON")
        # 自愈：旧库若没有站点映射表/同步表，按已提交的区表补建（幂等，不扫地址）
        conn.execute("CREATE TABLE IF NOT EXISTS dz_zone_sites ("
                     " zone_id TEXT PRIMARY KEY, site_id TEXT NOT NULL)")
        sync_common.ensure_sync_tables(conn)
        cur = current_revision(conn)
        if cur is None:
            raise RosterError("名册尚未建立，请先建册")
        if cur != expected_revision:
            raise RosterError(
                f"预期修订号 {expected_revision} 已过期，当前修订号为 {cur}；拒绝写入")
        old_zones = {r["zone_id"]: dict(r) for r in
                     conn.execute("SELECT * FROM dz_zones")}
        # 删除目标必须存在；同批先删后建等重复键已在 normalize_batch 拒绝
        for zid, z in changes.items():
            if z is None and zid not in old_zones:
                raise RosterError(f"删除失败：区 {zid} 当前不存在")
        # 启用同步后：批次新引入的区必须有站点映射，并整理进发布映射（同事务）。
        # 已删区的发布映射先保留，等净变化事件写完再删（原站要收到删除）。
        sync_on = sync_common.sync_enabled(conn)
        old_site_map = sync_common.site_zone_map(conn) if sync_on else {}
        if sync_on:
            for zid, z in changes.items():
                published = conn.execute(
                    "SELECT 1 FROM sync_zone_site WHERE zone_id=?", (zid,)).fetchone()
                static = conn.execute(
                    "SELECT site_id FROM dz_zone_sites WHERE zone_id=?", (zid,)
                ).fetchone()
                if published is None and static is None:
                    raise RosterError(
                        f"区 {zid} 没有站点映射；同步启用后不能新引入未映射的区")
                if z is not None and published is None:
                    conn.execute(
                        "INSERT INTO sync_zone_site(zone_id, site_id) VALUES (?,?)",
                        (zid, static["site_id"]))
                    sync_common.register_site(
                        conn, static["site_id"], current_revision(conn) + 1)
        for zid, z in changes.items():
            if z is None:
                conn.execute("DELETE FROM dz_zone_sites WHERE zone_id=?", (zid,))
        # 逐区收集旧/新外环闭合外包矩形，保持为独立矩形的并集（不做大矩形合并）
        boxes, touched_zones = [], []
        for zid, z in changes.items():
            touched_zones.append(zid)
            if zid in old_zones:
                o = old_zones[zid]
                boxes.append((o["bbox_minx"], o["bbox_miny"],
                              o["bbox_maxx"], o["bbox_maxy"]))
            if z is not None:
                boxes.append(tuple(z["bbox"]))
        # 定位受影响地址（矩形之间不去合成大矩形；去重针对同一地址落入多矩形）
        affected = {}
        per_box_hits = []
        for bbox in boxes:
            rows = _candidates_in_bbox(conn, bbox)
            per_box_hits.append({"bbox": list(bbox), "addresses": len(rows)})
            for r in rows:
                affected[r["source_row"]] = r
        # 只对受影响地址取旧归属区：按主键点查，不扫整册
        old_zone_of = {sr: (conn.execute(
            "SELECT zone_id FROM roster_records WHERE source_row=?", (sr,)
        ).fetchone()["zone_id"]) for sr in affected}
        # 写入批次最终区表
        for zid, z in changes.items():
            if z is None:
                conn.execute("DELETE FROM dz_zones WHERE zone_id=?", (zid,))
            else:
                minx, miny, maxx, maxy = z["bbox"]
                conn.execute(
                    "INSERT INTO dz_zones VALUES (?,?,?,?,?,?,?) "
                    "ON CONFLICT(zone_id) DO UPDATE SET "
                    "priority=excluded.priority, geometry_json=excluded.geometry_json, "
                    "bbox_minx=excluded.bbox_minx, bbox_miny=excluded.bbox_miny, "
                    "bbox_maxx=excluded.bbox_maxx, bbox_maxy=excluded.bbox_maxy",
                    (zid, z["priority"], z["geometry_json"],
                     minx, miny, maxx, maxy))
        polys = _load_polygons(conn)               # 批次最终区表（含未编辑的区）
        new_rev = cur + 1
        recomputed = []
        # 按“旧归属区”分组重算后的地址，供事务内写站点净变化
        recomputed_by_old = {}
        for sr, r in sorted(affected.items()):
            px, py = Decimal(r["lon_text"]), Decimal(r["lat_text"])
            status, zid = _assign(px, py, polys)  # 未编辑的区也可能接手/仍获胜
            conn.execute(
                "UPDATE roster_records SET status=?, zone_id=?, last_revision=? "
                "WHERE source_row=?", (status, zid, new_rev, sr))
            recomputed.append({"source_row": sr, "status": status, "zone_id": zid})
            recomputed_by_old.setdefault(old_zone_of[sr], []).append(sr)
        # 站点同步：与名册记录、修订号同事务生效；只覆盖本批重算地址。
        if sync_common.sync_enabled(conn):
            sync_common.record_dispatch_delta(
                conn, new_rev,
                sorted(recomputed_by_old.items(),
                       key=lambda kv: (kv[0] is None, kv[0] or "")),
                old_site_map)
            # 站点变化事件写完后才移除已删区的发布映射
            for zid, z in changes.items():
                if z is None:
                    conn.execute("DELETE FROM sync_zone_site WHERE zone_id=?", (zid,))
        conn.execute("UPDATE dz_meta SET value=? WHERE key='revision'", (str(new_rev),))
        detail = {"touched_zones": touched_zones,
                  "bboxes": [list(b) for b in boxes],
                  "recomputed_addresses": len(recomputed)}
        conn.execute(
            "INSERT INTO roster_revisions VALUES (?,?, 'batch', ?, ?)",
            (new_rev, fixed_snapshot_id(conn),
             json.dumps(detail, separators=(",", ":")), _now()))
        if commit:
            conn.commit()
        result = {"revision": new_rev, "touched_zones": touched_zones,
                  "bboxes": [list(b) for b in boxes],
                  "recomputed_addresses": len(recomputed),
                  "recomputed": recomputed, "per_box_hits": per_box_hits}
        if not commit:
            return result                        # 事务保持打开，供提交前崩溃演练
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        if commit:
            conn.isolation_level = old_level


# ---------------------------------------------------------------- 读取

def read_roster(conn) -> dict:
    """读当前版：快照 ID、名册修订号、按 source_row 排序的完整记录（单事务快照）。"""
    old_level = conn.isolation_level
    conn.isolation_level = None
    try:
        conn.execute("BEGIN")                    # 只读事务：读期间拿到一致性快照
        rev = current_revision(conn)
        if rev is None:
            conn.rollback()
            raise RosterError("名册尚未建立，无可读版本")
        sid = fixed_snapshot_id(conn)
        rows = conn.execute(
            "SELECT a.source_row, r.status, r.zone_id, r.last_revision, "
            "       a.sel_lon, a.sel_lat, a.lon_text, a.lat_text "
            "FROM roster_addresses a JOIN roster_records r "
            "  ON r.source_row = a.source_row ORDER BY a.source_row").fetchall()
        records = [{"source_row": r["source_row"], "status": r["status"],
                    "zone_id": r["zone_id"], "last_revision": r["last_revision"],
                    "lon": r["sel_lon"], "lat": r["sel_lat"],
                    "micro_lon": r["lon_text"], "micro_lat": r["lat_text"]}
                   for r in rows]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.isolation_level = old_level
    return {"snapshot_id": sid, "revision": rev,
            "record_count": len(records), "records": records}


# ---------------------------------------------------------------- CLI

def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def _int_microdegrees(text):
    """argparse 类型：严格整数微度（拒绝小数、指数、下划线等形式）。"""
    s = text.strip()
    digits = s[1:] if s[:1] in ("+", "-") else s
    if not digits or not digits.isdigit():
        raise argparse.ArgumentTypeError(f"窗口四至必须是整数微度: {text!r}")
    return int(s)


def _connect_readonly(path):
    """以只读模式打开数据库（审计命令用，从连接层面保证不改库）。"""
    from urllib.parse import quote
    conn = sqlite3.connect(f"file:{quote(path, safe='/')}?mode=ro",
                           uri=True, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def main(argv=None):
    p = argparse.ArgumentParser(
        prog="dispatch", description="局部可维护的派单名册（与 merger 共用同一 SQLite 库）")
    sub = p.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("build", help="首次建册（完整区表，产生修订号 1）")
    sp.add_argument("db"); sp.add_argument("zones_file"); sp.add_argument("--snapshot", type=int)
    sp.add_argument("--site-zones", help="区 -> 站点映射 JSON（建册时固定的静态映射）")
    sp = sub.add_parser("apply", help="应用改区批次（预期修订号 + JSON 批次）")
    sp.add_argument("db"); sp.add_argument("batch_file")
    sp = sub.add_parser("read", help="读取当前版完整名册")
    sp.add_argument("db"); sp.add_argument("--out", help="输出到文件，默认标准输出")
    sp = sub.add_parser("validate-geometry", help="只校验一个区/区表 JSON，不写库")
    sp.add_argument("file")
    sp = sub.add_parser(
        "audit-coverage",
        help="只读覆盖审计：报告窗口内漏划/重叠区域的精确面积与定位点")
    sp.add_argument("db")
    sp.add_argument("--window", nargs=4, type=_int_microdegrees, required=True,
                    metavar=("MIN_LON", "MIN_LAT", "MAX_LON", "MAX_LAT"),
                    help="检查窗口四至（整数微度，严格正宽高）")
    sp.add_argument("--out", help="输出到文件，默认标准输出")
    args = p.parse_args(argv)

    if args.cmd == "validate-geometry":
        payload = json.load(open(args.file, encoding="utf-8"))
        if isinstance(payload, dict) and "geometry" in payload:
            _print({"ok": True, "polygon": validate_polygon(payload["geometry"])})
        else:
            _print({"ok": True, "zones": list(normalize_zones(payload))})
        return

    if args.cmd == "audit-coverage":
        import coverage_audit
        try:
            conn = _connect_readonly(args.db)
            try:
                report = coverage_audit.audit_coverage(
                    conn, tuple(args.window))
            finally:
                conn.close()
        except (coverage_audit.AuditError, sqlite3.Error, OSError) as e:
            # 没有已建名册或窗口无效：非零退出，不输出貌似成功的报告
            print(f"audit-coverage: error: {e}", file=sys.stderr)
            raise SystemExit(1)
        text = json.dumps(report, ensure_ascii=False, indent=2)
        if args.out:
            open(args.out, "w", encoding="utf-8").write(text + "\n")
            _print({"revision": report["revision"],
                    "region_count": report["region_count"], "out": args.out})
        else:
            print(text)
        return

    conn = connect(args.db)
    try:
        if args.cmd == "build":
            zones = json.load(open(args.zones_file, encoding="utf-8"))
            site_zones = (json.load(open(args.site_zones, encoding="utf-8"))
                          if args.site_zones else None)
            _print(build_roster(conn, zones, args.snapshot, site_zones))
        elif args.cmd == "apply":
            text = open(args.batch_file, encoding="utf-8").read()
            batch = normalize_batch(text)
            _print(apply_changes(conn, batch))
        elif args.cmd == "read":
            result = read_roster(conn)
            text = json.dumps(result, ensure_ascii=False, indent=2, default=str)
            if args.out:
                open(args.out, "w", encoding="utf-8").write(text + "\n")
                _print({"snapshot_id": result["snapshot_id"],
                        "revision": result["revision"],
                        "record_count": result["record_count"], "out": args.out})
            else:
                print(text)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
