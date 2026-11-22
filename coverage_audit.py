"""只读的配送区覆盖审计（coverage audit）。

命令::

    python3 dispatch.py audit-coverage DB --window MIN_LON MIN_LAT MAX_LON MAX_LAT [--out F]

运营指定一个矩形检查窗口（四至为整数微度，严格正宽高、在经纬度范围内、
不跨日期变更线），审计报告窗口内实际存在的每一片**漏划**（覆盖签名 =
空集）或**重叠**（覆盖签名含两个或更多区 ID）区域的精确面积与定位点。

定义与方法
----------
* 覆盖签名 = 实际覆盖该处的全部区 ID 集合（忽略派单优先级）；单区覆盖不报告。
* 直接使用数据库里已提交的配送区几何（含孔洞）与名册修订号，两者在同一个
  只读事务（一致性快照）中读取：审计期间有人提交改区，几何与修订号也来自
  同一个已提交版本。不取地址点作样本，不改库，不追溯旧版。
* 先按窗口截取，再在经纬度平面内做**精确有理数**计算（fractions.Fraction）：
  斜边交点是有理数坐标，绝不四舍五入到整数微度，也不用栅格估算。
* 区域 = 窗口内签名相同且沿正长度公共边相连的极大面积片；仅在一点相碰
  不合并；只能在窗口外绕行相通的片在窗口内分开计；边界线与孤立点本身
  不计面积、不单独报异常。

算法（竖直梯形分解）
--------------------
1. 区表各外环/孔环边裁剪到闭窗口（Liang–Barsky，精确），再加上窗口上下
   两边作为边界线段。
2. 计算所有线段两两交点与共线重叠端点（有理数），把线段切成无内部交点的
   原子线段并去重（多区共边合成一条）。
3. 以所有原子线段端点的 x 为事件，把窗口竖直切成若干板条；板条内边不相交，
   按 (左端 y, 斜率) 排序后相邻边围成梯形，梯形内签名恒定。
4. 相邻板条间，梯形在公共竖直线上以正长度区间相接且该处没有竖直原子线段
   （真实区边）阻隔时并查集合并——穿过人为切分边不多报，只在一点相碰不合并。
5. 每个连通分量是一个签名恒定的面；面积 = 梯形面积和（鞋带/梯形公式，精确）；
   定位点 = 规范梯形（按中点坐标最小者）的中点，严格位于面内部。
6. 面的签名用 dispatch.point_covered 在定位点处精确判定；签名相同且沿正长度
   公共边相邻的面再并为同一区域。只输出签名为空或含 ≥2 个区 ID 的区域。

输出 JSON：窗口四至、名册修订号、异常区域列表；每个区域给出按字符串升序的
完整区 ID 列表（漏划为空）、精确面积（平方微度）与严格位于区域内部的定位点
[经度, 纬度]；面积与点坐标都是约分后的 ``分子/分母`` 字符串（分母为正）；
同一窗口与同一版区表重复审计，定位点与列表顺序完全稳定。
"""
from __future__ import annotations

import json
from fractions import Fraction

import dispatch

LON_LIMIT = 180 * 1_000_000
LAT_LIMIT = 90 * 1_000_000


class AuditError(ValueError):
    """覆盖审计输入错误（窗口非法、名册尚未建立等）。"""


# ---------------------------------------------------------------- 窗口校验

def validate_window(window):
    """校验检查窗口四至 ``(min_lon, min_lat, max_lon, max_lat)``（整数微度）。

    要求严格正宽高、落在经纬度范围内；``min < max`` 的矩形天然不跨日期
    变更线。返回规整后的四元组。
    """
    if not isinstance(window, (tuple, list)) or len(window) != 4:
        raise AuditError("窗口必须是 (min_lon, min_lat, max_lon, max_lat) 四元组")
    for v in window:
        if isinstance(v, bool) or not isinstance(v, int):
            raise AuditError(f"窗口四至必须是整数微度: {v!r}")
    minx, miny, maxx, maxy = window
    if not (-LON_LIMIT <= minx <= LON_LIMIT and -LON_LIMIT <= maxx <= LON_LIMIT):
        raise AuditError(f"窗口经度超出 [-{LON_LIMIT}, {LON_LIMIT}] 微度范围")
    if not (-LAT_LIMIT <= miny <= LAT_LIMIT and -LAT_LIMIT <= maxy <= LAT_LIMIT):
        raise AuditError(f"窗口纬度超出 [-{LAT_LIMIT}, {LAT_LIMIT}] 微度范围")
    if not minx < maxx:
        raise AuditError("窗口宽度必须严格为正（min_lon < max_lon）")
    if not miny < maxy:
        raise AuditError("窗口高度必须严格为正（min_lat < max_lat）")
    return minx, miny, maxx, maxy


# ---------------------------------------------------------------- 精确平面线段

def _clip_segment(p, q, minx, miny, maxx, maxy):
    """把线段 pq 裁剪到闭窗口（Liang–Barsky，Fraction 精确）。

    返回正长度截段 ``(a, b)``；不相交或只擦到一点时返回 None。
    """
    (px, py), (qx, qy) = p, q
    dx, dy = qx - px, qy - py
    t0, t1 = Fraction(0), Fraction(1)
    for d, lo, hi, c in ((dx, minx, maxx, px), (dy, miny, maxy, py)):
        if d == 0:
            if c < lo or c > hi:
                return None
            continue
        ta = (lo - c) / d
        tb = (hi - c) / d
        enter, leave = (ta, tb) if d > 0 else (tb, ta)
        if enter > t0:
            t0 = enter
        if leave < t1:
            t1 = leave
        if t0 > t1:
            return None
    a = (px + t0 * dx, py + t0 * dy)
    b = (px + t1 * dx, py + t1 * dy)
    if a == b:
        return None                      # 只擦到一点，没有正长度
    return a, b


def _sub(p, q):
    return (p[0] - q[0], p[1] - q[1])


def _cross2(u, v):
    return u[0] * v[1] - u[1] * v[0]


def _zone_segments(zones, window):
    """各区外环/孔环边裁剪到窗口 + 窗口上下边界，得到待切分的线段列表。"""
    minx, miny, maxx, maxy = window
    segs = []
    for zid in sorted(zones):
        poly = zones[zid]
        rings = [poly["outer"]] + list(poly.get("holes") or [])
        for ring in rings:
            pts = [(Fraction(x), Fraction(y)) for x, y in ring]
            for i in range(len(pts) - 1):
                a, b = pts[i], pts[i + 1]
                if a == b:
                    continue
                clipped = _clip_segment(a, b, minx, miny, maxx, maxy)
                if clipped is not None:
                    segs.append(clipped)
    # 窗口上下边作为边界线段（左右竖边不需要：板条只覆盖窗口内部）
    segs.append(((Fraction(minx), Fraction(miny)),
                 (Fraction(maxx), Fraction(miny))))
    segs.append(((Fraction(minx), Fraction(maxy)),
                 (Fraction(maxx), Fraction(maxy))))
    return segs


def _split_segments(segs):
    """计算两两交点与共线重叠端点，把线段切成无内部交点的原子线段并去重。"""
    splits = [set() for _ in segs]
    for i, (a, b) in enumerate(segs):
        splits[i].add(a)
        splits[i].add(b)
    for i in range(len(segs)):
        a, b = segs[i]
        v = _sub(b, a)
        for j in range(i + 1, len(segs)):
            c, d = segs[j]
            w = _sub(d, c)
            denom = _cross2(v, w)
            if denom != 0:
                ca = _sub(c, a)
                t = _cross2(ca, w) / denom
                u = _cross2(ca, v) / denom
                if 0 <= t <= 1 and 0 <= u <= 1:
                    p = (a[0] + t * v[0], a[1] + t * v[1])
                    splits[i].add(p)
                    splits[j].add(p)
                continue
            if _cross2(_sub(c, a), v) != 0:
                continue                         # 平行不共线
            # 共线：重叠区间（可能退化为一点）的端点是双方的切分点
            axis = 0 if v[0] != 0 else 1
            lo1, hi1 = sorted((a[axis], b[axis]))
            lo2, hi2 = sorted((c[axis], d[axis]))
            lo, hi = max(lo1, lo2), min(hi1, hi2)
            if lo > hi:
                continue
            for idx, (p1, p2) in ((i, (a, b)), (j, (c, d))):
                vv = _sub(p2, p1)
                for coord in (lo, hi):
                    t = (coord - p1[axis]) / vv[axis]
                    splits[idx].add((p1[0] + t * vv[0], p1[1] + t * vv[1]))
    atomic = set()
    for (a, b), pts in zip(segs, splits):
        v = _sub(b, a)
        axis = 0 if v[0] != 0 else 1
        ordered = sorted(pts, key=lambda p: p[axis])
        for k in range(len(ordered) - 1):
            p, q = ordered[k], ordered[k + 1]
            if p == q:
                continue
            atomic.add((p, q) if p <= q else (q, p))   # 定向规范化以便去重
    return atomic


# ---------------------------------------------------------------- 竖直梯形分解

def _slope(seg):
    (ax, ay), (bx, by) = seg
    return (by - ay) / (bx - ax)


def _y_at(seg, x):
    (ax, ay), (bx, by) = seg
    return ay + (x - ax) * (by - ay) / (bx - ax)


class _DSU:
    """并查集（路径压缩）。"""

    def __init__(self, n):
        self.parent = list(range(n))

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def _has_unblocked(lo, hi, blocked):
    """区间 [lo, hi) 去掉竖直边阻隔区间后是否还剩正长度部分。"""
    cur = lo
    for bl, bh in sorted(blocked):
        if bh <= cur:
            continue
        if bl > cur:
            return True
        cur = max(cur, bh)
        if cur >= hi:
            return False
    return cur < hi


def _decompose(window, atomic):
    """把窗口按事件 x 切成板条，板条内生成梯形；跨板条合并出签名恒定的面。

    返回 ``(faces, adjacency)``：faces 是梯形下标集合的列表（每个面一组梯形），
    adjacency 是共享正长度公共边的梯形对（用于同签名面再合并）。
    """
    minx, _miny, maxx, _maxy = window
    xs = {Fraction(minx), Fraction(maxx)}
    verticals = {}                                # x -> [(ylo, yhi)] 竖直原子边
    nonvertical = []
    for p, q in atomic:
        xs.add(p[0])
        xs.add(q[0])
        if p[0] == q[0]:
            y1, y2 = sorted((p[1], q[1]))
            verticals.setdefault(p[0], []).append((y1, y2))
        else:
            nonvertical.append((p, q))            # 端点已规范化：p.x < q.x
    events = sorted(xs)
    traps = []
    slabs = []                                    # 每个板条的梯形下标（自下而上）
    for k in range(len(events) - 1):
        cx0, cx1 = events[k], events[k + 1]
        active = [s for s in nonvertical
                  if s[0][0] <= cx0 and s[1][0] >= cx1]
        # 板条内边不相交；左端同高者斜率小者在下（共右端点时左端 y 已区分）
        active.sort(key=lambda s: (_y_at(s, cx0), _slope(s)))
        idxs = []
        for i in range(len(active) - 1):
            bot, top = active[i], active[i + 1]
            yb0, yb1 = _y_at(bot, cx0), _y_at(bot, cx1)
            yt0, yt1 = _y_at(top, cx0), _y_at(top, cx1)
            if yb0 == yt0 and yb1 == yt1:
                continue                          # 零面积（不可能：边已去重）
            idxs.append(len(traps))
            traps.append({"x0": cx0, "x1": cx1,
                          "yb0": yb0, "yb1": yb1, "yt0": yt0, "yt1": yt1})
        slabs.append(idxs)
    adjacency = []
    dsu = _DSU(len(traps))
    # 板条内相邻梯形共享正长度公共边
    for idxs in slabs:
        adjacency.extend(zip(idxs, idxs[1:]))
    # 跨板条：在公共竖直线上以正长度区间相接；无竖直边阻隔的部分合并
    for k in range(1, len(events) - 1):
        blocked = verticals.get(events[k], [])
        for tl in slabs[k - 1]:
            lo_l, hi_l = traps[tl]["yb1"], traps[tl]["yt1"]
            for tr in slabs[k]:
                lo_r, hi_r = traps[tr]["yb0"], traps[tr]["yt0"]
                lo, hi = max(lo_l, lo_r), min(hi_l, hi_r)
                if lo >= hi:
                    continue                      # 只在一点相碰：不合并、不相邻
                adjacency.append((tl, tr))
                if _has_unblocked(lo, hi, blocked):
                    dsu.union(tl, tr)
    faces = {}
    for i in range(len(traps)):
        faces.setdefault(dsu.find(i), []).append(i)
    return traps, list(faces.values()), adjacency


# ---------------------------------------------------------------- 面 -> 区域

def _trap_area(t):
    """梯形（两边竖直、上下为直线边）的精确面积。"""
    return (t["x1"] - t["x0"]) * ((t["yt0"] - t["yb0"]) + (t["yt1"] - t["yb1"])) / 2


def _trap_midpoint(t):
    """梯形中点：严格位于梯形（从而所在面）内部。"""
    mx = (t["x0"] + t["x1"]) / 2
    my = (t["yb0"] + t["yb1"] + t["yt0"] + t["yt1"]) / 4
    return (mx, my)


def _signature(zones, point):
    """定位点处的覆盖签名：实际覆盖该点的全部区 ID，按字符串升序。"""
    px, py = point
    return tuple(sorted(zid for zid, poly in zones.items()
                        if dispatch.point_covered(px, py, poly)))


def compute_regions(zones, window):
    """纯几何核心：给定 {zone_id: geometry} 与已校验窗口，返回异常区域列表。

    每个区域:: {"zones": [升序区 ID], "kind": "gap"|"overlap",
                 "area": Fraction, "point": (Fraction, Fraction)}
    只含漏划（空签名）与重叠（≥2 个区 ID）；按 (签名, 定位点) 排序，稳定。
    """
    atomic = _split_segments(_zone_segments(zones, window))
    traps, faces, adjacency = _decompose(window, atomic)
    # 每个面：面积、规范定位点（中点坐标最小的梯形）、签名
    face_info = {}
    for fi, members in enumerate(faces):
        area = sum((_trap_area(traps[i]) for i in members), Fraction(0))
        pt = min((_trap_midpoint(traps[i]) for i in members))
        face_info[fi] = {"area": area, "point": pt,
                         "sig": _signature(zones, pt)}
    # 相同签名且沿正长度公共边相邻的面并为同一区域（仅在一点相碰不合并）
    face_of = {}
    for fi, members in enumerate(faces):
        for i in members:
            face_of[i] = fi
    rdsu = _DSU(len(faces))
    for a, b in adjacency:
        fa, fb = face_of[a], face_of[b]
        if fa != fb and face_info[fa]["sig"] == face_info[fb]["sig"]:
            rdsu.union(fa, fb)
    regions = {}
    for fi in range(len(faces)):
        regions.setdefault(rdsu.find(fi), []).append(fi)
    out = []
    for members in regions.values():
        sig = face_info[members[0]]["sig"]
        if len(sig) == 1:
            continue                              # 单区覆盖不报告
        area = sum((face_info[fi]["area"] for fi in members), Fraction(0))
        pt = min(face_info[fi]["point"] for fi in members)
        out.append({"zones": list(sig),
                    "kind": "gap" if not sig else "overlap",
                    "area": area, "point": pt})
    out.sort(key=lambda r: (r["zones"], r["point"][0], r["point"][1]))
    return out


# ---------------------------------------------------------------- 名册读取 + 报告

def _read_zone_table(conn):
    """在单个只读事务中读取名册修订号与已提交区表（同一已提交版本）。"""
    old_level = conn.isolation_level
    conn.isolation_level = None
    try:
        conn.execute("BEGIN")                    # 一致性快照：读期间不受改区提交影响
        try:
            if not dispatch.roster_exists(conn):
                raise AuditError("名册尚未建立：没有可审计的区表版本")
            rev = dispatch.current_revision(conn)
            if rev is None:
                raise AuditError("名册尚未建立：没有可审计的区表版本")
            sid = dispatch.fixed_snapshot_id(conn)
            rows = conn.execute(
                "SELECT zone_id, geometry_json FROM dz_zones "
                "ORDER BY zone_id").fetchall()
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        conn.isolation_level = old_level
    zones = {r["zone_id"]: json.loads(r["geometry_json"]) for r in rows}
    return rev, sid, zones


def _frac_str(fr):
    """约分后的 分子/分母 字符串（Fraction 保证约分且分母为正）。"""
    return f"{fr.numerator}/{fr.denominator}"


def format_report(window, revision, snapshot_id, zone_count, regions):
    """把计算结果装配成稳定的报告 dict（区域已在 compute_regions 排序）。"""
    minx, miny, maxx, maxy = window
    return {
        "window": {"min_lon": minx, "min_lat": miny,
                   "max_lon": maxx, "max_lat": maxy},
        "revision": revision,
        "snapshot_id": snapshot_id,
        "zone_count": zone_count,
        "region_count": len(regions),
        "regions": [
            {"zones": r["zones"],
             "kind": r["kind"],
             "area": _frac_str(r["area"]),
             "point": [_frac_str(r["point"][0]), _frac_str(r["point"][1])]}
            for r in regions
        ],
    }


def audit_coverage(conn, window):
    """只读覆盖审计：读取当前已提交区表，报告窗口内漏划/重叠区域。

    ``window`` 为 ``(min_lon, min_lat, max_lon, max_lat)`` 整数微度四元组。
    只读事务读取几何与修订号；不取地址点、不写库、不追溯旧版。
    窗口非法或名册未建立时抛 :class:`AuditError`。
    """
    window = validate_window(window)
    revision, snapshot_id, zones = _read_zone_table(conn)
    regions = compute_regions(zones, window)
    return format_report(window, revision, snapshot_id, len(zones), regions)
