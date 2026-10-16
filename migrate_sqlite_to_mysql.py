# -*- coding: utf-8 -*-
"""一次性迁移: SQLite(ledger.db) -> MySQL

在 PythonAnywhere 上运行(把本文件上传到 /home/ggglll77/wechat-ledger/ 后):
    python migrate_sqlite_to_mysql.py

前置条件:
  1) PythonAnywhere → Databases 页: 创建 MySQL 数据库并设置密码
  2) 项目 .env 已填写 MYSQL_HOST / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DB
  3) 已安装 pymysql (pip install pymysql)

说明: 迁移会保留原记录的 id 与 created_at; 重复执行是幂等的(ON DUPLICATE 覆盖)。
"""
import os
import sqlite3

import pymysql

from config import (DB_PATH, MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD,
                    MYSQL_DB, MYSQL_PORT)

import db  # noqa: E402  (会加载 config/.env, 用 MySQL 建表)


def main():
    if not (MYSQL_HOST and MYSQL_USER and MYSQL_DB):
        print("❌ 未配置 MySQL, 请先在 .env 填写 MYSQL_HOST/USER/PASSWORD/DB")
        return
    src = DB_PATH
    if not os.path.exists(src):
        print(f"❌ 找不到 SQLite 文件: {src}")
        return

    # 1) 读 SQLite
    s = sqlite3.connect(src)
    s.row_factory = sqlite3.Row
    rows = [dict(r) for r in s.execute("SELECT * FROM records ORDER BY id")]
    cache_rows = [dict(r) for r in s.execute("SELECT * FROM wechat_reply_cache")]
    s.close()
    print(f"[迁移] SQLite 记录 {len(rows)} 条, 回复缓存 {len(cache_rows)} 条: {src}")

    # 2) 建 MySQL 表(幂等)
    db.init_db()
    print(f"[迁移] 连接 MySQL: {MYSQL_USER}@{MYSQL_HOST}/{MYSQL_DB}")

    conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER,
                           password=MYSQL_PASSWORD, database=MYSQL_DB,
                           port=MYSQL_PORT, charset="utf8mb4", autocommit=False)
    try:
        with conn.cursor() as cur:
            if rows:
                cur.executemany(
                    "INSERT INTO records(id,openid,tx_date,type,category,subcategory,"
                    "subcat_src,amount,note,source,created_at) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE openid=VALUES(openid)",
                    [(r["id"], r["openid"], r["tx_date"], r["type"], r["category"],
                      r["subcategory"] or "", r["subcat_src"] or "",
                      float(r["amount"]), r["note"] or "", r["source"] or "text",
                      r["created_at"] or None) for r in rows])
            if cache_rows:
                cur.executemany(
                    "INSERT IGNORE INTO wechat_reply_cache(msg_id, response_xml, created_at) "
                    "VALUES(%s,%s,%s)",
                    [(r["msg_id"], r["response_xml"], r["created_at"] or None)
                     for r in cache_rows])
        conn.commit()
        print(f"✅ 迁移完成: 记录 {len(rows)} 条, 回复缓存 {len(cache_rows)} 条")
    except Exception as e:
        conn.rollback()
        print(f"❌ 迁移失败已回滚: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
