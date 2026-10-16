# -*- coding: utf-8 -*-
"""记账存储: 内存SQLite主库 + 后台增量备份到磁盘

背景: PythonAnywhere免费版(2026-01-15后新注册账户)不提供MySQL; 而其网络盘上
SQLite每次提交实测卡5~15秒, 远超微信5秒被动回复窗口, 曾导致服务反复瘫痪。
方案: 所有读写走进程内 :memory: 共享缓存库(毫秒级, 微信请求5秒内必回);
后台线程每DB_BACKUP_INTERVAL秒把内存库在线备份到磁盘 ledger.db;
启动/进程重启时把磁盘库加载回内存。
权衡: 进程被强杀或主动Reload时, 最多丢最近一次备份后新写的账(默认30秒)。
对个人记账可接受; 明显优于"每次提交卡死导致整个服务无响应"。
注意: 只适用于单进程部署(PythonAnywhere免费版即单worker)。
"""
import os
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager

from config import DB_PATH

# 后台备份间隔(秒): 越小越不易丢数据; 网络盘上备份本身也慢, 30秒较平衡
DB_BACKUP_INTERVAL = int(os.getenv("DB_BACKUP_INTERVAL", "30"))

# 共享缓存内存库URI: 主连接读写 + 备份连接在线备份, 互不阻塞
_MEM_URI = "file:ledger_mem?mode=memory&cache=shared"

_mem_conn = None            # 主连接(进程内由 _lock 串行)
_init_lock = threading.Lock()
_backup_thread = None
_stop_backup = False


def _caller() -> str:
    """慢会话打点用: 报出"db操作函数 <- 上层调用者"(跳过contextlib与db.py内部帧)"""
    f = sys._getframe(2)
    while f and "contextlib" in f.f_code.co_filename:      # with语句由contextlib驱动
        f = f.f_back
    if not f:
        return "?"
    op = f.f_code.co_name
    g = f.f_back
    while g and g.f_code.co_filename == __file__:          # 跳过db.py内部调用
        g = g.f_back
    return f"{op} <- {g.f_code.co_name}" if g else op


class _DbLock:
    """进程内串行锁: 等锁超时 + 卡死自愈 + 持有者可追溯

    正常时所有DB读写串行, 后来者最多等 timeout 秒;
    若持有者超过 STUCK_TTL 仍未释放(内存库操作理论上不会, 防御性保留),
    后来者会放弃该持有者继续执行, 避免一个卡住的调用让整个服务瘫痪。
    内存库操作都是毫秒级, 等锁超时几乎不会发生。"""

    STUCK_TTL = 15.0

    def __init__(self):
        self._cv = threading.Condition()
        self._holder = None                 # 持有者线程 ident
        self._since = 0.0                   # 持锁起始时刻(单调时钟)

    @contextmanager
    def hold(self, timeout: float = 2.0, wait: bool = True):
        me = threading.get_ident()
        with self._cv:
            if not wait and self._holder is not None:
                raise TimeoutError("数据库忙(后台写放弃, 不抢占用户请求)")
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


def _load_disk_into(conn):
    """启动/重启时: 磁盘库存在则加载进内存连接"""
    if os.path.exists(DB_PATH) and os.path.getsize(DB_PATH) > 0:
        src = sqlite3.connect(DB_PATH)
        try:
            src.backup(conn)
        finally:
            src.close()
        print(f"[db] 已从磁盘加载 {DB_PATH}", flush=True)


def _init_schema(conn):
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
    # 微信被动回复缓存: 跨进程重启后仍可重放 MsgId -> XML 回复
    conn.execute("""CREATE TABLE IF NOT EXISTS wechat_reply_cache(
        msg_id TEXT PRIMARY KEY,
        response_xml TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now','localtime')))""")
    # 老库补充 subcategory / subcat_src 列
    cols = [r[1] for r in conn.execute("PRAGMA table_info(records)")]
    if "subcategory" not in cols:
        conn.execute("ALTER TABLE records ADD COLUMN subcategory TEXT DEFAULT ''")
    if "subcat_src" not in cols:
        conn.execute("ALTER TABLE records ADD COLUMN subcat_src TEXT DEFAULT ''")


def init_db():
    """初始化: 建内存库 + 加载磁盘数据 + 建表(幂等)。
    备份线程不在这里启动(master进程只负责fork, 不处理请求,
    避免master与worker同时写磁盘冲突); 由 _ensure_backup 在worker内启动"""
    global _mem_conn
    with _init_lock:
        if _mem_conn is not None:
            return
        conn = sqlite3.connect(_MEM_URI, uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        _load_disk_into(conn)
        _init_schema(conn)
        _mem_conn = conn
        print(f"[db] 内存库就绪(备份间隔{DB_BACKUP_INTERVAL}s -> {DB_PATH})", flush=True)


def _ensure_backup():
    """确保后台备份线程已启动。fork后线程不会复制到worker, 每次会话入口检查,
    未启动则补启动(只会在worker里生效, 因为只有worker会处理请求)"""
    global _backup_thread
    if _backup_thread is not None and _backup_thread.is_alive():
        return
    with _init_lock:
        if _backup_thread is not None and _backup_thread.is_alive():
            return
        _backup_thread = threading.Thread(target=_backup_loop, daemon=True,
                                          name="db-backup")
        _backup_thread.start()


def _backup_loop():
    while not _stop_backup:
        time.sleep(DB_BACKUP_INTERVAL)
        try:
            _backup_once()
        except Exception as e:
            print(f"[db] ⚠️ 后台备份失败(下次重试): {e}", flush=True)


def _backup_once():
    """在线备份: 独立连接打开共享缓存内存库 -> 备份到磁盘文件。
    备份期间主连接读写不阻塞(backup只短暂持有源库读锁, 页写入慢在磁盘侧)"""
    if _mem_conn is None:
        return
    t0 = time.monotonic()
    src = sqlite3.connect(_MEM_URI, uri=True)
    disk = sqlite3.connect(DB_PATH)
    try:
        src.backup(disk)
    finally:
        disk.close()
        src.close()
    print(f"[db] 已备份 {DB_PATH} ({time.monotonic()-t0:.1f}s)", flush=True)


@contextmanager
def _session(wait: bool = True):
    """内存库会话: 进程内串行(带锁超时/自愈) + 正常结束提交, 异常回滚
    所有操作都是内存级, 毫秒返回, 微信5秒窗口内必然完成
    wait=False: 供后台写使用, 锁被占用时立即放弃而非排队"""
    _ensure_backup()          # 惰性启动备份线程(worker内首次请求触发)
    with _lock.hold(2.0, wait=wait):
        t0 = time.monotonic()
        conn = _mem_conn
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        total = time.monotonic() - t0
        if total > 1.0:       # 防御性打点: 内存操作正常远小于1s
            print(f"[db] ⚠️ 慢会话 {total:.1f}s 来源:{_caller()}", flush=True)


def get_wechat_reply(msg_id: str):
    """读取已完成的微信消息回复；返回 XML 字符串或 None。"""
    if not msg_id:
        return None
    with _session() as conn:
        row = conn.execute("SELECT response_xml FROM wechat_reply_cache WHERE msg_id=?", (msg_id,)).fetchone()
    return row["response_xml"] if row else None


def save_wechat_reply(msg_id: str, response_xml: str, best_effort: bool = False):
    """保存完整被动回复；幂等更新，保留最近7天缓存。
    best_effort=True: 后台写, 锁被别人占用时直接放弃(不抢用户请求的锁)"""
    if not msg_id or not response_xml or not response_xml.lstrip().startswith("<xml>"):
        return
    with _session(wait=not best_effort) as conn:
        conn.execute("""INSERT INTO wechat_reply_cache(msg_id, response_xml) VALUES(?, ?)
            ON CONFLICT(msg_id) DO UPDATE SET response_xml=excluded.response_xml,
            created_at=datetime('now','localtime')""", (msg_id, response_xml))
        conn.execute("DELETE FROM wechat_reply_cache WHERE created_at < "
                     "datetime('now','localtime','-7 days')")


def add_record(openid: str, tx_date: str, rtype: str, category: str,
               amount: float, note: str = "", source: str = "text",
               subcategory: str = "", subcat_src: str = ""):
    with _session() as conn:
        conn.execute(
            "INSERT INTO records(openid,tx_date,type,category,subcategory,"
            "subcat_src,amount,note,source) VALUES(?,?,?,?,?,?,?,?,?)",
            (openid, tx_date, rtype, category, subcategory, subcat_src,
             amount, note, source))


def add_records_bulk(openid: str, tx_date: str, rows: list, source: str = "text"):
    """一次事务批量插入多笔"""
    if not rows:
        return
    with _session() as conn:
        conn.executemany(
            "INSERT INTO records(openid,tx_date,type,category,subcategory,"
            "subcat_src,amount,note,source) VALUES(?,?,?,?,?,?,?,?,?)",
            [(openid, tx_date, rt, cat, sub, src, amt, note, source)
             for rt, cat, amt, note, sub, src in rows])


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
    updates: [(id, subcategory, src), ...]"""
    if not updates:
        return
    with _session() as conn:
        conn.executemany("UPDATE records SET subcategory=?, subcat_src=? WHERE id=?",
                         [(sub, src, rid) for rid, sub, src in updates])
