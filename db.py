# -*- coding: utf-8 -*-
"""记账存储: SQLite单文件, 线程安全"""
import sqlite3
import threading
import time
from contextlib import contextmanager

from config import DB_PATH


class _DbLock:
    """进程内串行锁: 等锁超时 + 卡死自愈 + 持有者可追溯

    正常时所有DB读写串行, 后来者最多等 timeout 秒;
    若持有者超过 STUCK_TTL 仍未释放(网络盘I/O卡住/建连卡住), 后来者会
    放弃该持有者继续执行, 避免"一个卡住的调用让整个服务瘫到uWSGI harakiri"。
    真实文件访问仍由SQLite自身锁兜底, 抢占后最坏是报 database is locked,
    由调用方降级回复(远好过整个服务10分钟无响应)。"""

    STUCK_TTL = 15.0

    def __init__(self):
        self._cv = threading.Condition()
        self._holder = None                 # 持有者线程 ident
        self._since = 0.0                   # 持锁起始时刻(单调时钟)

    @contextmanager
    def hold(self, timeout: float = 2.0):
        me = threading.get_ident()
        with self._cv:
            deadline = time.monotonic() + timeout
            while self._holder is not None:
                held = time.monotonic() - self._since
                if held > self.STUCK_TTL:        # 持有者疑似卡死 -> 抢占自愈
                    print(f"[db] ⚠️ 线程{self._holder}持锁已{held:.0f}s未释放, "
                          f"判定卡死并抢占(线程{me}继续)", flush=True)
                    break
                remain = deadline - time.monotonic()
                if remain <= 0:
                    print(f"[db] ⚠️ 等锁超时: 线程{self._holder}已持锁{held:.1f}s",
                          flush=True)
                    raise TimeoutError("数据库忙, 请稍后重试")
                self._cv.wait(min(0.2, remain))
            self._holder, self._since = me, time.monotonic()
        try:
            yield
        finally:
            with self._cv:
                if self._holder == me:
                    self._holder = None
                self._cv.notify_all()


_lock = _DbLock()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=2.0)
    conn.row_factory = sqlite3.Row
    # 部署环境的home是网络盘: 默认的rollback journal每次提交都要建文件+fsync,
    # 在慢盘上极易卡死几分钟(请求被uWSGI harakiri杀掉)。改用内存日志 +
    # 关闭同步, 把每次提交的网络I/O降到最低; busy_timeout限制SQLite级等锁
    try:
        conn.execute("PRAGMA journal_mode=MEMORY")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=2000")
        conn.execute("PRAGMA temp_store=MEMORY")
    except Exception as e:
        conn.close()
        raise RuntimeError(f"数据库初始化失败: {e}")
    return conn


@contextmanager
def _session():
    """连接会话: 进程内串行(带锁超时/自愈) + 正常结束提交, 异常回滚, 无论成败都关闭连接
    (注意: sqlite3 close() 会回滚未提交事务, 不能只 close 不 commit)
    网络盘上并发访问SQLite极易卡死, 这里用一把锁把所有读写串行化;
    锁最多等2秒(微信被动回复只有5秒窗口), 超时立即抛错由调用方降级回复"""
    with _lock.hold(2.0):
        conn = _conn()          # 建连在锁内: 由 _lock.hold 的finally兜底释放, 不会泄漏
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def init_db():
    with _session() as conn:
        # 网络盘上WAL依赖-shm共享内存会卡死; DELETE日志每次提交都要建文件+fsync也很慢。
        # 统一用内存日志(与_conn里的PRAGMA一致), 只保留建表这类必须的落盘
        mode = conn.execute("PRAGMA journal_mode=MEMORY").fetchone()[0]
        conn.execute("PRAGMA busy_timeout=2000")
        # 启动打点: 真实库路径 + 实际生效的日志模式(应为memory)
        print(f"[db] DB_PATH={DB_PATH} journal_mode={mode}")
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
        # 微信被动回复缓存：跨 uWSGI worker 持久化 MsgId -> XML 回复
        conn.execute("""CREATE TABLE IF NOT EXISTS wechat_reply_cache(
            msg_id TEXT PRIMARY KEY,
            response_xml TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')))""")
        # 迁移: 老库补充 subcategory / subcat_src 列
        cols = [r[1] for r in conn.execute("PRAGMA table_info(records)")]
        if "subcategory" not in cols:
            conn.execute("ALTER TABLE records ADD COLUMN subcategory TEXT DEFAULT ''")
        if "subcat_src" not in cols:
            conn.execute("ALTER TABLE records ADD COLUMN subcat_src TEXT DEFAULT ''")



def get_wechat_reply(msg_id: str):
    """读取已完成的微信消息回复；返回 XML 字符串或 None。"""
    if not msg_id:
        return None
    with _session() as conn:
        row = conn.execute("SELECT response_xml FROM wechat_reply_cache WHERE msg_id=?", (msg_id,)).fetchone()
    return row["response_xml"] if row else None


def save_wechat_reply(msg_id: str, response_xml: str):
    """保存完整被动回复；幂等更新，保留最近7天缓存。"""
    if not msg_id or not response_xml or not response_xml.lstrip().startswith("<xml>"):
        return
    with _session() as conn:
        conn.execute("""INSERT INTO wechat_reply_cache(msg_id, response_xml) VALUES(?, ?)
            ON CONFLICT(msg_id) DO UPDATE SET response_xml=excluded.response_xml,
            created_at=datetime('now','localtime')""", (msg_id, response_xml))
        conn.execute("DELETE FROM wechat_reply_cache WHERE created_at < datetime('now','localtime','-7 days')")


def add_record(openid: str, tx_date: str, rtype: str, category: str,
               amount: float, note: str = "", source: str = "text",
               subcategory: str = "", subcat_src: str = ""):
    with _session() as conn:
        conn.execute(
            "INSERT INTO records(openid,tx_date,type,category,subcategory,"
            "subcat_src,amount,note,source) VALUES(?,?,?,?,?,?,?,?,?)",
            (openid, tx_date, rtype, category, subcategory, subcat_src,
             amount, note, source))


def _sums(where: str, args: tuple, openid: str) -> dict:
    with _session() as conn:
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
    with _session() as conn:
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
    with _session() as conn:
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
    with _session() as conn:
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
    with _session() as conn:
        rows = conn.execute(
            "SELECT id, tx_date, type, category, subcategory, amount, note FROM records "
            "WHERE openid=? ORDER BY id DESC LIMIT ?", (openid, limit)).fetchall()
    return [dict(r) for r in rows]


def delete_record_by_match(openid: str, tx_date: str, kind: str, amount,
                           note: str = None):
    """按日期+分类(或细分类)+金额删除一条账目; amount 为 None 时不限金额
    note 非空时再按备注精确匹配
    多个匹配时删除最早的一笔; 返回(被删记录dict, 剩余相同记录数); 无匹配返回(None, 0)"""
    with _session() as conn:
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
    with _session() as conn:
        rows = conn.execute(
            "SELECT id, tx_date, type, category, subcategory, amount, note FROM records "
            "WHERE openid=? AND tx_date=? ORDER BY id", (openid, day)).fetchall()
    return [dict(r) for r in rows]


def unclassified_records(limit: int = 500) -> list:
    """待AI复核/回填的记录(subcat_src != 'ai'): [(id, category, type)]"""
    with _session() as conn:
        rows = conn.execute(
            "SELECT id, category, type FROM records "
            "WHERE subcat_src != 'ai' ORDER BY id LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


def update_record_subcategory(rid: int, subcategory: str, src: str = ""):
    """按记录id回填/修正大类, 同时记录来源(local/ai)"""
    with _session() as conn:
        conn.execute("UPDATE records SET subcategory=?, subcat_src=? WHERE id=?",
                     (subcategory, src, rid))


def update_subcategories_bulk(updates: list):
    """批量回填/修正大类, 单次事务提交
    (网络盘上每次commit都有落盘开销, 合并提交可把N次I/O降为1次)
    updates: [(id, subcategory, src), ...]"""
    if not updates:
        return
    with _session() as conn:
        conn.executemany("UPDATE records SET subcategory=?, subcat_src=? WHERE id=?",
                         [(sub, src, rid) for rid, sub, src in updates])
