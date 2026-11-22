#!/usr/bin/env python3
"""dispatch_roster_test.py 的子进程辅助（以 python3 -I 运行）。

两种模式：
* stall  : 打开独立连接，应用同一批改区但停在提交点（commit=False），
           保持事务打开后退出（进程结束 -> 连接关闭 -> 未提交事务随 WAL 回滚）。
* writer : 独立连接尝试一次改区写入，把结果（ok / 错误类型）写到结果文件，
           用于验证同一预期修订号的并发写至多一个成功。

`-I` 模式不把脚本目录加入 sys.path，故显式插入可信的项目目录。
"""
import json
import os
import sys
import time

sys.path.insert(0, os.environ["DISPATCH_PROJECT_DIR"])
import dispatch  # noqa: E402


def main():
    mode = sys.argv[1]
    db = sys.argv[2]
    payload_file = sys.argv[3]
    out_file = sys.argv[4]
    batch = dispatch.normalize_batch(open(payload_file, encoding="utf-8").read())
    conn = dispatch.connect(db)
    try:
        if mode == "stall":
            # 停在提交点：所有改动已写入事务但未 commit；写出 ready 标记后保持
            # 进程存活（事务随之保持打开），由父进程验证读一致性后结束本进程，
            # 连接关闭 -> 未提交事务随 WAL 回滚，可从上一版继续。
            dispatch.apply_changes(conn, batch, commit=False)
            with open(out_file, "w", encoding="utf-8") as fh:
                fh.write("ready")
                fh.flush()
                os.fsync(fh.fileno())
            sys.stderr.write("stall: transaction left open, sleeping without commit\n")
            time.sleep(12)
            return 0
        if mode == "writer":
            try:
                result = dispatch.apply_changes(conn, batch)
                json.dump({"ok": True, "result": result}, open(out_file, "w"))
            except Exception as e:  # noqa: BLE001 - 子进程需上报任意失败类型
                json.dump({"ok": False, "error_type": type(e).__name__,
                           "error": str(e)}, open(out_file, "w"))
            return 0
        raise SystemExit(f"unknown mode {mode}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
