# -*- coding: utf-8 -*-
"""记账存储: SQLite单文件, 线程安全"""
import sqlite3
import threading
from contextlib import closing

from config import DB_PATH

_lock = threading.Lock()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)   # 等待锁最长10秒, 避免直接抛locked
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(_conn()) as conn:
        # WAL: 读写并发不互斥, 大幅避免 database is locked
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=8000")
        conn.execute("""CREATE TABLE IF NOT EXISTS records(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            openid   TEXT NOT NULL,
            tx_date  TEXT NOT NULL,           -- YYYY-MM-DD
            type     TEXT NOT NULL,           -- 支出 / 收入
            category TEXT NOT NULL,           -- 分类
            subcategory TEXT DEFAULT '',      -- 大类
            subcat_src TEXT DEFAULT '',       -- 大类来源: ''未归类 / local本地 / ai AI复核
            amount   REAL NOT NULL,
            note     TEXT DEFAULT '',
            source   TEXT DEFAULT 'text',     -- text / voice / manual
            created_at TEXT DEFAULT (datetime('now','localtime')))""")
        # 迁移: 老库补充 subcategory / subcat_src 列
        cols = [r[1] for r in conn.execute("PRAGMA table_info(records)")]
        if "subcategory" not in cols:
            conn.execute("ALTER TABLE records ADD COLUMN subcategory TEXT DEFAULT ''")
        if "subcat_src" not in cols:
            conn.execute("ALTER TABLE records ADD COLUMN subcat_src TEXT DEFAULT ''")


def add_record(openid: str, tx_date: str, rtype: str, category: str,
               amount: float, note: str = "", source: str = "text",
               subcategory: str = "", subcat_src: str = ""):
    with _lock, closing(_conn()) as conn:
        conn.execute(
            "INSERT INTO records(openid,tx_date,type,category,subcategory,"
            "subcat_src,amount,note,source) VALUES(?,?,?,?,?,?,?,?,?)",
            (openid, tx_date, rtype, category, subcategory, subcat_src,
             amount, note, source))


def _sums(where: str, args: tuple, openid: str) -> dict:
    with closing(_conn()) as conn:
        rows = conn.execute(
            f"SELECT type, SUM(amount) AS s FROM records "
            f"WHERE openid=? AND {where} GROUP BY type", (openid, *args)).fetchall()
    d = {"支出": 0.0, "收入": 0.0}
    for r in rows:
        d[r["type"]] = round(r["s"] or 0, 2)
    return d


def today_stats(openid: str, today: str) -> dict:
    return _sums("tx_date=?", (today,), openid)


def month_stats(openid: str, month: str) -> dict:
    return _sums("substr(tx_date,1,7)=?", (month,), openid)


def _cat_agg(where: str, args: tuple, openid: str) -> list:
    """各分类支出聚合: 大类(subcategory)优先, 未归类则退化为细分类(category)"""
    with closing(_conn()) as conn:
        rows = conn.execute(
            f"SELECT COALESCE(NULLIF(subcategory,''), category) AS cat, "
            f"SUM(amount) AS s FROM records "
            f"WHERE openid=? AND type='支出' AND {where} "
            f"GROUP BY cat ORDER BY s DESC", (openid, *args)).fetchall()
    return [(r["cat"], round(r["s"], 2)) for r in rows]


def month_by_category(openid: str, month: str) -> list:
    """本月各分类支出(按大类聚合), 按金额降序"""
    return _cat_agg("substr(tx_date,1,7)=?", (month,), openid)


def week_stats(openid: str, start: str, end: str) -> dict:
    """自然周(周一~日)收支: start/end 为 YYYY-MM-DD"""
    return _sums("tx_date>=? AND tx_date<=?", (start, end), openid)


def week_by_category(openid: str, start: str, end: str) -> list:
    """一周内各分类支出(按大类聚合), 按金额降序"""
    return _cat_agg("tx_date>=? AND tx_date<=?", (start, end), openid)


def week_daily(openid: str, start: str, end: str) -> list:
    """一周内每天支出统计(旧→新): [(日期, 支出, 收入)]"""
    with closing(_conn()) as conn:
        rows = conn.execute(
            "SELECT tx_date, "
            "SUM(CASE WHEN type='支出' THEN amount ELSE 0 END) AS e, "
            "SUM(CASE WHEN type='收入' THEN amount ELSE 0 END) AS i "
            "FROM records WHERE openid=? AND tx_date>=? AND tx_date<=? "
            "GROUP BY tx_date ORDER BY tx_date",
            (openid, start, end)).fetchall()
    return [{"date": r["tx_date"], "支出": round(r["e"] or 0, 2),
             "收入": round(r["i"] or 0, 2)} for r in rows]


def year_stats(openid: str, year: str) -> dict:
    """整年收支统计: year 为 'YYYY'"""
    return _sums("substr(tx_date,1,4)=?", (year,), openid)


def year_by_category(openid: str, year: str) -> list:
    """整年各分类支出(按大类聚合), 按金额降序"""
    return _cat_agg("substr(tx_date,1,4)=?", (year,), openid)


def year_monthly(openid: str, year: str) -> list:
    """整年逐月支出统计(1~12月): [(YYYY-MM, 支出, 收入)]"""
    with closing(_conn()) as conn:
        rows = conn.execute(
            "SELECT substr(tx_date,1,7) AS m, "
            "SUM(CASE WHEN type='支出' THEN amount ELSE 0 END) AS e, "
            "SUM(CASE WHEN type='收入' THEN amount ELSE 0 END) AS i "
            "FROM records WHERE openid=? AND substr(tx_date,1,4)=? "
            "GROUP BY m ORDER BY m",
            (openid, year)).fetchall()
    return [{"month": r["m"], "支出": round(r["e"] or 0, 2),
             "收入": round(r["i"] or 0, 2)} for r in rows]


def recent_records(openid: str, limit: int = 5) -> list:
    """最近几笔记录(新→旧), 含id供删除用"""
    with closing(_conn()) as conn:
        rows = conn.execute(
            "SELECT id, tx_date, type, category, subcategory, amount, note FROM records "
            "WHERE openid=? ORDER BY id DESC LIMIT ?", (openid, limit)).fetchall()
    return [dict(r) for r in rows]


def delete_record_by_match(openid: str, tx_date: str, kind: str, amount,
                           note: str = None):
    """按日期+分类(或细分类)+金额删除一条账目; amount 为 None 时不限金额
    note 非空时再按备注精确匹配
    多个匹配时删除最早的一笔; 返回(被删记录dict, 剩余相同记录数); 无匹配返回(None, 0)"""
    with _lock, closing(_conn()) as conn:
        where = "openid=? AND tx_date=? AND (category=? OR subcategory=?)"
        args = [openid, tx_date, kind, kind]
        if amount is not None:
            where += " AND ABS(amount-?)<0.005"
            args.append(amount)
        if note:
            where += " AND note=?"
            args.append(note)
        rows = conn.execute(
            f"SELECT id, tx_date, type, category, subcategory, amount, note "
            f"FROM records WHERE {where} ORDER BY id", args).fetchall()
        if not rows:
            return None, 0
        row = rows[0]
        conn.execute("DELETE FROM records WHERE id=?", (row["id"],))
    return dict(row), len(rows) - 1


def day_records(openid: str, day: str) -> list:
    """某一天的全部记录(旧→新), 含id供删除用"""
    with closing(_conn()) as conn:
        rows = conn.execute(
            "SELECT id, tx_date, type, category, subcategory, amount, note FROM records "
            "WHERE openid=? AND tx_date=? ORDER BY id", (openid, day)).fetchall()
    return [dict(r) for r in rows]


def unclassified_records(limit: int = 500) -> list:
    """待AI复核/回填的记录(subcat_src != 'ai'): [(id, category, type)]"""
    with closing(_conn()) as conn:
        rows = conn.execute(
            "SELECT id, category, type FROM records "
            "WHERE subcat_src != 'ai' ORDER BY id LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


def update_record_subcategory(rid: int, subcategory: str, src: str = ""):
    """按记录id回填/修正大类, 同时记录来源(local/ai)"""
    with _lock, closing(_conn()) as conn:
        conn.execute("UPDATE records SET subcategory=?, subcat_src=? WHERE id=?",
                     (subcategory, src, rid))
