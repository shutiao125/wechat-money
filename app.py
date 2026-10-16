"""微信测试号回调服务入口: 消息回调 + 记账
运行: python app.py  (开发时配合内网穿透暴露公网HTTPS)
"""
import re
import time
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from flask import Flask, request
from werkzeug.exceptions import HTTPException

from config import (WECHAT_TOKEN, ALLOWED_OPENIDS, PUSH_TOKEN,
                    RUN_SCHEDULER, SUBCAT_AI, PUSH_HOUR, PUSH_MINUTE, LLM_API_KEY, now)
import wechat
import db
import agent
from llm_parser import (parse_record, ai_report, parse_batch, classify_local,
                        _cf_classify, tokenize_amounts, annual_report, ai_summary)
from report import (build_daily_report, build_weekly_report,
                    build_annual_report, push_daily, push_weekly, push_annual)

app = Flask(__name__)
db.init_db()


# 回复缓存: 内存优先(重试秒回, 不被慢写拖住); 落库转后台异步, 且不抢用户请求的锁
_mem_replies, _mem_lock = OrderedDict(), threading.Lock()
_persist_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="persist")


def _remember_reply(msg_id: str, payload: str):
    with _mem_lock:
        _mem_replies[msg_id] = payload
        while len(_mem_replies) > 300:                  # 只留最近300条, 防止无限增长
            _mem_replies.popitem(last=False)


def _mem_reply(msg_id: str):
    with _mem_lock:
        return _mem_replies.get(msg_id)


def _persist_reply(msg_id: str, payload: str):
    try:
        db.save_wechat_reply(msg_id, payload, best_effort=True)
    except Exception as e:
        print(f"[wechat] 回复缓存落库跳过(不影响本次回复): {type(e).__name__}: {e}",
              flush=True)


@app.after_request
def _cache_wechat_reply(response):
    """把生成的被动XML回复记入内存缓存(供微信同一MsgId重试时重放),
    并投给后台线程落库; 落库慢也不阻塞回复"""
    try:
        if request.path == "/wechat" and request.method == "POST":
            msg = wechat.parse_xml(request.get_data(cache=True))
            msg_id = msg.get("MsgId", "")
            if msg_id and response.status_code == 200:
                payload = response.get_data(as_text=True)
                if payload.lstrip().startswith("<xml>"):
                    _remember_reply(msg_id, payload)
                    _persist_pool.submit(_persist_reply, msg_id, payload)
    except Exception as e:
        print(f"[wechat] 回复缓存写入失败: {type(e).__name__}: {e}", flush=True)
    return response


@app.errorhandler(Exception)
def _on_unhandled(e):
    """兜底: 任何未捕获异常都别让微信侧"无回复"(500会被微信当失败, 用户端什么都看不到)
    记好堆栈, 同时回一条可见提示, 便于定位"""
    if isinstance(e, HTTPException):
        return e
    import traceback
    traceback.print_exc()
    print(f"[wechat] ⚠️ 处理异常, 降级回复: {e!r}")
    try:
        msg = wechat.parse_xml(request.data)     # request.data 已缓存, 可重复读取
        openid = msg.get("FromUserName", "")
        if openid:
            return wechat.to_text_reply(
                msg.get("ToUserName", ""), openid,
                "⚠️ 系统开了会儿小差, 请再发一次~\n"
                "若反复出现, 把这条消息的时间发给开发者查日志")
    except Exception:
        pass
    return "success"

# ==================== 大类(subcategory)回填 ====================
def _backfill_subcategories():
    """启动时先本地回填历史记录大类(避免几百条同时调AI阻塞启动):
    收入直接归"收入", 支出用本地关键词; 所有回填都标 local,
    由 _subcat_worker 继续用AI逐条复核为最终结果;
    多条合并为一次提交, 避免网络盘上逐条commit拖慢启动"""
    rows = db.unclassified_records()
    if not rows:
        return
    updates = []
    for r in rows:
        sub = classify_local(r["category"], r["type"])
        if sub:
            updates.append((r["id"], sub, "local"))
    if updates:
        db.update_subcategories_bulk(updates)
    print(f"[subcat] 启动本地回填 {len(updates)}/{len(rows)} 条, 其余留待AI复核")


# 后台AI复核专用单线程池: 对每条记录用 result(timeout) 强制"总时长"硬超时,
# 防止 _cf_classify 网络请求挂起时拖住线程; 配合db锁2秒超时, 后台线程再卡
# 也不会阻塞微信请求(曾因网络盘I/O挂起持锁导致"今日"卡到uWSGI harakiri强杀)
_subcat_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="subcat")


def _subcat_worker():
    """后台线程: 逐条用AI复核所有未定稿(subcat_src != 'ai')记录的大类,
    每8秒一批, 完全在后台执行, 不占用/阻塞微信5秒回复;
    单条复核总时长≤2秒, 一批写回一次提交, 绝不影响微信请求"""
    while True:
        try:
            updates = []
            for r in db.unclassified_records(limit=10):
                try:
                    fut = _subcat_pool.submit(_cf_classify, r["category"], r["type"])
                    sub = fut.result(timeout=2.0)   # 单条总时长硬上限
                except Exception as e:
                    print(f"[subcat] 单条复核超时/失败: {e}")
                    sub = None
                if sub:
                    updates.append((r["id"], sub, "ai"))
            if updates:
                try:
                    db.update_subcategories_bulk(updates)
                except Exception as e:
                    print(f"[subcat] 批量写回失败: {e}")
        except Exception as e:
            print(f"[subcat] AI复核异常: {e}")
        time.sleep(8)


_backfill_subcategories()
if SUBCAT_AI:
    threading.Thread(target=_subcat_worker, daemon=True).start()
    print("[subcat] 后台AI大类复核已启动(SUBCAT_AI=1)")
else:
    print("[subcat] 后台AI大类复核已关闭(默认); 需要时设 SUBCAT_AI=1 启用")

# MsgId回复缓存存储在SQLite中，跨uWSGI worker有效；不提前在内存标记消息已处理。

# AI分析专用线程池: 用 result(timeout) 强制"总时长"硬超时(requests的read
# timeout只看两次读间隔, 无法限制总时长, 曾导致被动回复超过微信5秒上限)
_llm_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="llm")


def _pick_subcategory(name: str, rtype: str, ai_timeout: float = 2.0):
    """记账时本地秒判大类(不等待AI): 关键词命中即返回, 判断不了暂归"其他"
    (标local, 由后台 _subcat_worker 用AI复核修正)
    返回 (大类, 来源); 来源: local=本地兜底"""
    sub = classify_local(name, rtype)
    return (sub, "local") if sub else ("其他", "local")


def _pick_subcategories(pairs):
    """批量本地秒判大类(不等待AI): 关键词命中即返回, 判断不了暂归"其他"
    (标local, 后台AI复核修正)
    pairs: [(name, rtype), ...] -> [(大类, 来源), ...]"""
    if not pairs:
        return []
    return [(classify_local(name, rtype) or "其他", "local")
            for name, rtype in pairs]

WELCOME = (
    "👋 欢迎使用记账助手!\n"
    "直接发消息即可记账, 例如:\n"
    "  午饭花了35块\n"
    "  昨天打车去机场68元\n"
    "  发工资啦 12000\n\n"
    "命令: 【今日】【本月】【最近】【分析总结】【周报】【年报】【帮助】"
)
HELP = ("📌 记账详细命令:\n"
        "  直接发文字, 例如:\n"
        "    「午饭35」「打车花20」「发工资12000」\n"
        "    「昨天买书58」「补记2026年9月5日吃饭30」\n"
        "    「奶茶6 洗澡2 喝水3」——一次记多笔\n"
        "  不用记命令也行, AI能听懂, 例如:\n"
        "    「帮我把昨天的奶茶删掉」「上周花了多少」\n"
        "🔎 查账:\n"
        "  最近——查看最近5条记录\n"
        "  本月——查看本月记录情况\n"
        "  今日——查看今天记录情况\n"
        "  也可指定查询某天，如:\n"
        "  查2026年9月5日(查20260905)\n"
        "🗑️ 删账:\n"
        "  删除2026年9月7日 餐饮 35\n"
        "  删除2026年9月7日 奶茶6 洗澡2 —— 批量删\n"
        "✏️ 补、改账:\n"
        "  修改2026年9月7日 餐饮 35 打车 40\n"
        "  (把2026年9月7日的「餐饮35」改成「打车40」)\n"
        "  补记20260915奶茶5\n"
        "📆 周报:\n"
        "   仅支持在每周日查看周报\n"
        "🗓️ 年报:\n"
        "   仅支持在每年12月31日查看年报\n"
        "🤖 分析总结:\n"
        "   支持上周/本周/上月/本月/今年/去年\n"
        "   命令例如:分析上周")


def _allowed(openid: str) -> bool:
    return not ALLOWED_OPENIDS or openid in ALLOWED_OPENIDS


def _is_duplicate(msg_id: str) -> bool:
    """微信5秒收不到响应会重推, 按MsgId去重"""
    with _seen_lock:
        if msg_id in _seen_msgs:
            return True
        _seen_msgs.add(msg_id)
        if len(_seen_msgs) > 5000:          # 防止集合无限增长
            _seen_msgs.clear()
        return False


def _compact_ymd(digits: str):
    """紧凑日期解析: 20250902 / 2025920 / 202592 -> (年, 月, 日)"""
    if not digits.isdigit() or len(digits) < 6:
        raise ValueError("日期位数不足")
    y = int(digits[:4])
    rest = digits[4:]
    if len(rest) == 4:
        m, d = int(rest[:2]), int(rest[2:])
    elif len(rest) == 3:
        m, d = int(rest[0]), int(rest[1:])
    else:
        m, d = int(rest[0]), int(rest[1])
    datetime(y, m, d)                       # 校验日期合法性(如2月30日会抛错)
    return y, m, d


def _strip_date(s: str):
    """从字符串开头剥离日期, 返回(Y-M-D日期或None, 剩余文本)"""
    s = s.strip()
    m = re.match(r"^(?P<rel>今天|昨天)", s)
    if m:
        back = 1 if m.group("rel") == "昨天" else 0
        day = (now() - timedelta(days=back)).strftime("%Y-%m-%d")
        return day, s[m.end():].strip()
    m = re.match(r"^(?:(?P<y>\d{4})年)?(?P<m>\d{1,2})月(?P<d>\d{1,2})[日号]", s)
    if m:
        year = int(m.group("y")) if m.group("y") else now().year
        try:
            day = f"{year}-{int(m.group('m')):02d}-{int(m.group('d')):02d}"
            return day, s[m.end():].strip()
        except ValueError:
            pass
    m = re.match(r"^(?P<y>\d{4})[年/\-]?(?P<m>\d{1,2})[月/\-]?(?P<d>\d{1,2})[日号]?", s)
    if m:
        try:
            day = f"{int(m.group('y'))}-{int(m.group('m')):02d}-{int(m.group('d')):02d}"
            return day, s[m.end():].strip()
        except ValueError:
            pass
    return None, s


def _cat_disp(r):
    """显示用分类: 细分类（大类）; 未归大类时只显示细分类"""
    sub = r.get("subcategory") or ""
    return r["category"] + (f"（{sub}）" if sub else "")


def _analysis_data(openid: str, period: str):
    """统计指定周期的收支供AI分析, period为 本月/上月/本周/上周/今年/去年
    返回 (展示标题, 周期标签, data字典); data含 expense/income/balance/分类支出/上期支出/上期收入"""
    now = now()
    if period in ("本月", "上月"):
        month = now.strftime("%Y-%m") if period == "本月" else _prev_month(now)
        stats = db.month_stats(openid, month)
        cats = db.month_by_category(openid, month)
        pstats = db.month_stats(openid, _prev_month(month))
        label, plabel = month, period
    elif period in ("本周", "上周"):
        base = now - timedelta(weeks=1) if period == "上周" else now
        start = base - timedelta(days=base.weekday())
        end = start + timedelta(days=6)
        s, e = start.date().isoformat(), end.date().isoformat()
        stats = db.week_stats(openid, s, e)
        cats = db.week_by_category(openid, s, e)
        ps, pe = start - timedelta(weeks=1), start - timedelta(days=1)
        pstats = db.week_stats(openid, ps.date().isoformat(), pe.date().isoformat())
        label = f"{s[5:]}~{e[5:]}"
        plabel = period
    else:                                     # 今年 / 去年
        year = now.year if period == "今年" else now.year - 1
        stats = db.year_stats(openid, str(year))
        cats = db.year_by_category(openid, str(year))
        pstats = db.year_stats(openid, str(year - 1))
        label, plabel = f"{year}年", period
    bal = round(stats["收入"] - stats["支出"], 2)
    bal_str = f"+{bal:.2f}" if bal > 0 else f"{bal:.2f}"
    data = {"expense": stats["支出"], "income": stats["收入"], "balance": bal_str,
            "分类支出": {c: v for c, v in cats},
            "上期支出": pstats["支出"], "上期收入": pstats["收入"]}
    return label, plabel, data


def _prev_month(ref):
    """返回 ref(YYYY-MM 或 datetime) 的上一个月字符串 YYYY-MM"""
    if isinstance(ref, str):
        y, mo = map(int, ref.split("-"))
    else:
        y, mo = ref.year, ref.month
    return f"{y-1}-12" if mo == 1 else f"{y}-{mo-1:02d}"


def _build_plain_analysis(label: str, plabel: str, data: dict) -> str:
    """LLM超时的本地降级: 用数据库统计拼一段连贯的文字分析, 而非数字清单
    plabel 为本月/上月/本周/上周/今年/去年, data 结构见 _analysis_data"""
    exp, inc = data["expense"], data["income"]
    top = sorted((data.get("分类支出") or {}).items(), key=lambda kv: -kv[1])
    if not exp and not inc:
        return f"{label} 期间没有支出和收入记录，本期无账可分析。"
    pexp = data.get("上期支出", 0.0)
    diff = exp - pexp
    if diff > 0:
        cm = f"比上期多花了 ¥{diff:.2f}"
    elif diff < 0:
        cm = f"比上期少花了 ¥{abs(diff):.2f}"
    else:
        cm = "与上期基本持平"
    parts = [f"{plabel}支出 ¥{exp:.2f}，收入 ¥{inc:.2f}，结余 {data['balance']}，{cm}。"]
    if top:
        heads = "、".join(f"{c}（¥{v:.2f}）" for c, v in top[:3])
        parts.append(f"支出主要集中在{heads}，其中「{top[0][0]}」花销最高。")
    else:
        parts.append("本期没有分类支出记录。")
    if exp > inc:
        parts.append("整体收不抵支，建议适当控制非必要消费，留意大额支出。")
    elif exp == 0:
        parts.append("本期几乎无支出，花销很克制。")
    else:
        parts.append("整体收支平衡，支出处于可控水平。")
    return "".join(parts)


# ==================== AI Agent 工具处理器 ====================
# agent.route 返回的工具名 -> 执行函数: 由LLM决定意图与参数, 这里只做校验执行
# 校验不过抛异常 -> 调用方转正则兜底; 查询/删除类查不到时直接返回提示文本

def _norm_date(s, default=None):
    """严格校验 YYYY-MM-DD, 非法日期抛错(触发正则兜底)"""
    if not s:
        return default
    try:
        datetime.strptime(str(s), "%Y-%m-%d")
        return str(s)
    except ValueError:
        raise ValueError(f"非法日期: {s}")


def _agent_amount(v):
    """校验金额: 数字且>0, 返回两位小数; 其余抛错"""
    if isinstance(v, bool) or not isinstance(v, (int, float)) or v <= 0:
        raise ValueError(f"金额无效: {v!r}")
    return round(float(v), 2)


def _income_cat(cat: str) -> bool:
    return any(k in cat for k in ("工资", "到账", "收入", "进账", "收到红包", "退款"))


def _agent_record(openid: str, args: dict, source: str, text: str = "") -> str:
    day = _norm_date(args.get("date"), now().strftime("%Y-%m-%d")) \
        or now().strftime("%Y-%m-%d")
    items = args.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("无有效账目条目")
    rows = []
    for it in items[:20]:
        if not isinstance(it, dict):
            raise ValueError("条目格式错误")
        cat = str(it.get("category") or "").strip()[:20]
        if not cat:
            raise ValueError("类别为空")
        amt = _agent_amount(it.get("amount"))
        rtype = it.get("type") if it.get("type") in ("支出", "收入") else \
            ("收入" if _income_cat(cat) else "支出")
        note = str(it.get("note") or cat)[:20]
        rows.append((rtype, cat, amt, note))
    subs = _pick_subcategories([(c, t) for t, c, _, _ in rows])
    for (rtype, cat, amt, note), (sub, src) in zip(rows, subs):
        db.add_record(openid, day, rtype, cat, amt, note, source, sub, src)
    if len(rows) == 1:
        rtype, cat, amt, note = rows[0]
        icon = "💸" if rtype == "支出" else "💰"
        note_part = f" | {note}" if note != cat else ""
        return (f"{icon} 已记账 {day}\n{cat} ¥{amt:.2f}{note_part}\n"
                f"发送【今日】查看汇总")
    total = round(sum(a for _, _, a, _ in rows), 2)
    lines = [f"  {c} ¥{a:.2f}" for _, c, a, _ in rows]
    return (f"💸 已记账 {day} 共{len(rows)}笔, 合计 ¥{total:.2f}:\n"
            + "\n".join(lines))


def _agent_delete(openid: str, args: dict, source: str, text: str = "") -> str:
    day = _norm_date(args.get("date"), now().strftime("%Y-%m-%d"))
    items = args.get("items")
    pairs = []
    if isinstance(items, list):
        for it in items[:20]:
            if not isinstance(it, dict):
                raise ValueError("条目格式错误")
            cat = str(it.get("category") or "").strip()[:20]
            if not cat:
                raise ValueError("类别为空")
            amt = it.get("amount")
            amt = None if amt is None else _agent_amount(amt)
            pairs.append((cat, amt))
    if not pairs:
        raise ValueError("无删除条目")
    if len(pairs) == 1:
        kind, amount = pairs[0]
        rec, more = db.delete_record_by_match(openid, day, kind, amount)
        if rec is None:
            amt_s = f" ¥{amount:.2f}" if amount is not None else ""
            return (f"❌ 没有找到匹配记录: {day} {kind}{amt_s}\n"
                    f"可先发【查{day}】核对该天的账目")
        reply = (f"🗑 已删除 {rec['tx_date']} {rec['category']} "
                 f"¥{rec['amount']:.2f} {rec['note']}")
        if more:
            reply += f"\n⚠️ 还有{more}条相同记录, 再次发送本命令可继续删除"
        return reply
    deleted, missing = [], []
    for kind, amount in pairs:
        rec, _ = db.delete_record_by_match(openid, day, kind, amount, kind)
        (deleted if rec is not None else missing).append(f"  {kind} ¥{amount:.2f}")
    reply = f"🗑 已删除 {day} {len(deleted)}笔:\n" + "\n".join(deleted)
    if missing:
        reply += "\n❌ 未找到:\n" + "\n".join(missing)
    return reply


def _agent_modify(openid: str, args: dict, source: str, text: str = "") -> str:
    old_day = _norm_date(args.get("date"))
    if not old_day:
        raise ValueError("缺少原记录日期")
    old_cat = str(args.get("old_category") or "").strip()[:20]
    new_cat = str(args.get("new_category") or "").strip()[:20]
    if not old_cat or not new_cat:
        raise ValueError("缺少修改前后的类别")
    try:
        old_amt = _agent_amount(args.get("old_amount"))
    except ValueError:
        old_amt = None                        # 用户没说原金额: 按日期+类别定位
    new_amt = _agent_amount(args.get("new_amount"))
    # 用户没说"改到某天"就忽略模型给的新日期(防止幻觉挪账); 只在原文有改日期意图时才用
    if any(k in text for k in ("改到", "挪到", "移到", "记到", "换到")):
        new_day = _norm_date(args.get("new_date"), old_day) or old_day
    else:
        new_day = old_day
    rec, more = db.delete_record_by_match(openid, old_day, old_cat, old_amt)
    if rec is None and old_amt is not None:
        # 原金额不匹配(多为模型猜测): 按日期+类别兜底定位
        rec, more = db.delete_record_by_match(openid, old_day, old_cat, None)
    if rec is None:
        amt_s = f" ¥{old_amt:.2f}" if old_amt is not None else ""
        return (f"❌ 没有找到匹配记录: {old_day} {old_cat}{amt_s}\n"
                f"可先发【查{old_day}】/【最近】核对账目")
    rtype = "收入" if _income_cat(new_cat) else "支出"
    db.add_record(openid, new_day, rtype, new_cat, new_amt, new_cat, source, new_cat)
    reply = (f"✏️ 已修改 {old_day} {old_cat} ¥{rec['amount']:.2f} → "
             f"{new_day} {new_cat} ¥{new_amt:.2f}")
    if more:
        reply += f"\n⚠️ 还有{more}条相同旧账, 未被删除"
    return reply


# 周期别名: 8B模型偶尔输出 last_week 等英文周期, 统一归一化
_PERIOD_ALIAS = {"last_week": "上周", "this_week": "本周", "week": "本周",
                 "last_month": "上月", "this_month": "本月", "month": "本月",
                 "year": "今年", "this_year": "今年", "last_year": "去年"}


def _norm_period(p) -> str:
    """只做确定性的归一化: 精确值/别名映射; 垃圾值原样返回, 交给原文回扫"""
    raw = str(p or "").strip()
    if raw in ("本月", "上月", "本周", "上周", "今年", "去年"):
        return raw
    return _PERIOD_ALIAS.get(raw.lower(), raw)


def _period_from_text(text: str) -> str:
    """模型输出的周期异常时, 回扫用户原文推导周期"""
    for kw, val in (("本周", "本周"), ("这周", "本周"), ("上周", "上周"),
                    ("本月", "本月"), ("这个月", "本月"), ("上月", "上月"),
                    ("今年", "今年"), ("去年", "去年")):
        if kw in text:
            return val
    return _norm_period(text) if text else ""


def _agent_query(openid: str, args: dict, source: str, text: str = "") -> str:
    period = _norm_period(args.get("period") or "today")
    if period not in ("today", "yesterday", "this_month", "recent", "date",
                      "本周", "上周", "今年", "去年"):
        period = _period_from_text(text) or period
    now = now()
    if period in ("本周", "上周"):
        base = now - timedelta(weeks=1) if period == "上周" else now
        start = base - timedelta(days=base.weekday())
        end = start + timedelta(days=6)
        s, e = start.date().isoformat(), end.date().isoformat()
        t = db.week_stats(openid, s, e)
        cats = db.week_by_category(openid, s, e)
        top = "、".join(f"{c} ¥{v}" for c, v in cats[:3]) if cats else "暂无"
        return (f"📅 {period}({s[5:]}~{e[5:]})支出: ¥{t['支出']:.2f}\n"
                f"📅 收入: ¥{t['收入']:.2f}\n分类支出: {top}")
    if period in ("今年", "去年"):
        year = now.year if period == "今年" else now.year - 1
        t = db.year_stats(openid, str(year))
        cats = db.year_by_category(openid, str(year))
        top = "、".join(f"{c} ¥{v}" for c, v in cats[:3]) if cats else "暂无"
        return (f"📅 {year}年支出: ¥{t['支出']:.2f}\n"
                f"📅 收入: ¥{t['收入']:.2f}\n分类支出: {top}")
    if period == "today":
        t = db.today_stats(openid, now.strftime("%Y-%m-%d"))
        return f"📊 今日支出: ¥{t['支出']:.2f}\n📊 今日收入: ¥{t['收入']:.2f}"
    if period == "yesterday":
        y = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        t = db.today_stats(openid, y)
        return f"📊 昨日({y})支出: ¥{t['支出']:.2f}\n📊 昨日收入: ¥{t['收入']:.2f}"
    if period == "this_month":
        month = now.strftime("%Y-%m")
        m = db.month_stats(openid, month)
        cats = db.month_by_category(openid, month)
        top = "、".join(f"{c} ¥{v}" for c, v in cats[:5]) if cats else "暂无"
        return (f"📅 本月支出: ¥{m['支出']:.2f}\n📅 本月收入: ¥{m['收入']:.2f}\n"
                f"分类支出: {top}")
    if period == "recent":
        rows = db.recent_records(openid)
        if not rows:
            return "📭 还没有任何记账记录"
        lines = [f"{r['tx_date']} {_cat_disp(r)} ¥{r['amount']:.2f} "
                 f"{r['note']} [{r['type']}]" for r in rows]
        return "📋 最近5笔:\n" + "\n".join(lines)
    if period == "date":
        day = _norm_date(args.get("date"))
        if not day:
            raise ValueError("缺少查询日期")
        rows = db.day_records(openid, day)
        if not rows:
            return f"📭 {day} 没有账目记录"
        total = sum(r["amount"] for r in rows)
        lines = [f"{r['type']} {_cat_disp(r)} ¥{r['amount']:.2f}"
                 + (f" {r['note']}" if r["note"] and r["note"] != r["category"] else "")
                 for r in rows]
        return (f"📅 {day} 共{len(rows)}笔, 合计 ¥{total:.2f}:\n"
                + "\n".join(lines)
                + f"\n删除某条: 删除{day} 分类 金额")
    raise ValueError(f"未知查询周期: {period}")


def _agent_analyze(openid: str, args: dict, source: str, text: str = "") -> str:
    """Agent路径的分析: 用本地统计文字分析(不再二次调LLM, 保证5秒窗口)"""
    period = _norm_period(args.get("period") or "本月")
    if period not in ("本月", "上月", "本周", "上周", "今年", "去年"):
        period = _period_from_text(text) or period
    if period not in ("本月", "上月", "本周", "上周", "今年", "去年"):
        raise ValueError(f"未知分析周期: {period}")
    label, plabel, data = _analysis_data(openid, period)
    if not data["expense"] and not data["income"]:
        return f"📭 {plabel}({label}) 暂无收支记录, 记账后再来分析~"
    plain = _build_plain_analysis(label, plabel, data)
    return f"🤖 账单分析 {plabel}({label})\n{plain}"


AGENT_HANDLERS = {
    "record": _agent_record,
    "delete": _agent_delete,
    "modify": _agent_modify,
    "query": _agent_query,
    "analyze": _agent_analyze,
}


def handle_record(openid: str, text: str, source: str, day: str = "") -> str:
    """纯本地秒级解析并入库, 返回确认文本(作为被动回复, 未认证订阅号无客服消息接口)
    day 非空时覆盖解析出的日期(供补记/修改指定日期使用)
    全程不等待AI(解析走正则, 大类本地秒判), 大类待后台AI异步复核修正"""
    r = parse_record(text)
    sub = r.get("subcategory", "") or ""
    if sub:
        src = "ai"
    else:
        sub, src = _pick_subcategory(r["category"], r["type"])
    tx_date = day or r["date"]
    db.add_record(openid, tx_date, r["type"], r["category"],
                  r["amount"], r["note"], source, sub, src)
    icon = "💸" if r["type"] == "支出" else "💰"
    note_part = f" | {r['note']}" if r.get("note") and r["note"] != r["category"] else ""
    return (f"{icon} 已记账 {tx_date}\n"
            f"{_cat_disp(r)} ¥{r['amount']:.2f}{note_part}\n"
            f"发送【今日】查看汇总")


@app.route("/wechat", methods=["GET", "POST"])
def wechat_callback():
    if request.method == "GET":
        # 测试号接口配置的URL验证
        if wechat.check_signature(WECHAT_TOKEN, request.args.get("signature", ""),
                                  request.args.get("timestamp", ""),
                                  request.args.get("nonce", "")):
            return request.args.get("echostr", "")
        return "签名校验失败", 403

    # ---------- POST: 消息处理 ----------
    # 诊断日志：签名失败/XML解析失败此前会静默返回 success。
    # 不记录 signature/token 等凭据；flush=True 让日志及时显示。
    signature = request.args.get("signature", "")
    timestamp = request.args.get("timestamp", "")
    nonce = request.args.get("nonce", "")
    body = request.get_data(cache=True)
    signature_ok = wechat.check_signature(
        WECHAT_TOKEN, signature, timestamp, nonce
    )
    print(
        f"[wechat-debug] POST /wechat signature_ok={signature_ok} "
        f"token_configured={bool(WECHAT_TOKEN)} timestamp_present={bool(timestamp)} "
        f"nonce_present={bool(nonce)} body_bytes={len(body)} "
        f"content_type={request.content_type!r}",
        flush=True,
    )
    if not signature_ok:
        print(
            "[wechat-debug] 签名校验失败；请核对公众号配置中的 Token "
            "与 PythonAnywhere 环境变量 WECHAT_TOKEN。",
            flush=True,
        )
        return "success"

    try:
        msg = wechat.parse_xml(body)
    except Exception as e:
        print(
            f"[wechat-debug] XML解析异常: {type(e).__name__}: {e}",
            flush=True,
        )
        return "success"
    if not msg:
        print(
            f"[wechat-debug] XML解析结果为空，body_bytes={len(body)}；"
            "请检查请求体格式。",
            flush=True,
        )
        return "success"

    openid = msg.get("FromUserName", "")
    msg_type = msg.get("MsgType", "")
    print(f"[msg] openid={openid!r} MsgType={msg_type!r} Content={msg.get('Content', '')!r}",
          flush=True)

    if not _allowed(openid):
        # 白名单拦截时返回空success(无回复无报错), 打印原因便于排查
        print(f"[wechat] ⚠️ 白名单拦截: openid={openid!r} 不在 ALLOWED_OPENIDS={ALLOWED_OPENIDS!r}")
        return "success"

    # 关注事件
    if msg_type == "event" and msg.get("Event") == "subscribe":
        return wechat.to_text_reply(msg.get("ToUserName", ""), openid, WELCOME)

    # 微信超时重试时重放上次完整回复；不能只返回 success，否则用户收不到回复。
    # 内存缓存优先(秒回, 不受网络盘影响), 命中不了再查库(跨重启保留)
    msg_id = msg.get("MsgId", "")
    if msg_id:
        cached_reply = _mem_reply(msg_id) or db.get_wechat_reply(msg_id)
        if cached_reply:
            print(f"[wechat] 重放已缓存回复 MsgId={msg_id}", flush=True)
            return cached_reply

    # 提取文本: 文字消息
    text, source = "", "text"
    if msg_type == "text":
        text = msg.get("Content", "").strip()
    elif msg_type == "voice":
        # 未认证公众号不提供语音识别, 语音记账已停用, 提示后不进入记账流程
        return wechat.to_text_reply(
            msg.get("ToUserName", ""), openid,
            "🎤 语音记账功能已停用。\n请直接打字发送记账内容, 例如「午饭35」")
    else:
        return "success"

    if not text:
        print(f"[wechat] ⚠️ 空文本消息忽略: Content={msg.get('Content','')!r}")
        return "success"

    # 紧凑日期规范化: 20250902/2025920 -> 2025年09月02日 (供查/补记/删除命令使用)
    m_c = re.match(r"^查\s*(\d{7,8})$", text)
    if m_c:
        try:
            y, mo, dy = _compact_ymd(m_c.group(1))
            text = f"查{y}年{mo}月{dy}日"
        except ValueError:
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        "❌ 日期无效, 示例: 查20250902 或 查9月5日")
    m_c = re.match(r"^(删除|补记)\s*(\d{7,8})\s+(.+)$", text)
    if m_c:
        try:
            y, mo, dy = _compact_ymd(m_c.group(2))
            text = f"{m_c.group(1)}{y}年{mo}月{dy}日 {m_c.group(3)}"
        except ValueError:
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        "❌ 日期无效, 示例: 补记20250902 午饭30块")

    # 快捷命令: 只查数据库, 同步回复(远快于5秒限制)
    today = now().strftime("%Y-%m-%d")
    if text in ("今日", "今天"):
        t = db.today_stats(openid, today)
        reply = f"📊 今日支出: ¥{t['支出']:.2f}\n📊 今日收入: ¥{t['收入']:.2f}"
        print(f"[wechat] 今日回复: {reply!r}")
        return wechat.to_text_reply(msg.get("ToUserName", ""), openid, reply)
    if text in ("本月", "月报"):
        month = now().strftime("%Y-%m")
        m = db.month_stats(openid, month)
        cats = db.month_by_category(openid, month)
        top = "、".join(f"{c} ¥{v}" for c, v in cats[:5]) if cats else "暂无"
        return wechat.to_text_reply(
            msg.get("ToUserName", ""), openid,
            f"📅 本月支出: ¥{m['支出']:.2f}\n📅 本月收入: ¥{m['收入']:.2f}\n"
            f"分类支出: {top}")
    if text in ("最近", "账单"):
        rows = db.recent_records(openid)
        if not rows:
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        "📭 还没有任何记账记录")
        lines = [f"{r['tx_date']} {_cat_disp(r)} ¥{r['amount']:.2f} "
                 f"{r['note'] if r['note'] != r['category'] else ''} [{r['type']}]"
                 .replace("  [", " [") for r in rows]
        return wechat.to_text_reply(
            msg.get("ToUserName", ""), openid,
            "📋 最近5笔:\n" + "\n".join(lines))
    # 周报: 仅每周日可查询
    if text in ("周报", "本周"):
        if now().weekday() != 6:     # weekday(): 周一=0 ... 周日=6
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        "🕐 周报仅每周日可查询, 到时再发「周报」~")
        return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                    build_weekly_report(openid))
    # 年报: 仅每年12月31日可查询(查询当年)
    if text in ("年报", "今年"):
        if now().month != 12 or now().day != 31:
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        "🕐 年报仅每年12月31日可查询, 到时再发「年报」~")
        year = now().year
        text_r, s, cats, months, top, top_month = build_annual_report(openid, year)
        if LLM_API_KEY and (s["支出"] or s["收入"]):
            bal = round(s["收入"] - s["支出"], 2)
            bal_str = f"+{bal:.2f}" if bal > 0 else f"{bal:.2f}"
            data = {"year": year, "expense": s["支出"], "income": s["收入"],
                    "balance": bal_str, "top": top or "暂无", "top_month": top_month or "无",
                    "分类支出": dict(cats),
                    "逐月支出": {m["month"][5:] + "月": m["支出"] for m in months}}
            ai_text = annual_report(str(year), data)
            if ai_text:
                text_r = f"🤖 AI年度报告 {year}\n{ai_text}"
        return wechat.to_text_reply(msg.get("ToUserName", ""), openid, text_r)
    # 查某天的账: 查9月5日(默认今年) / 查2025年9月5日
    m_q = re.match(r"^查\s*(?:(\d{4})年)?\s*(\d{1,2})月(\d{1,2})[日号]$", text)
    if m_q:
        year = int(m_q.group(1)) if m_q.group(1) else now().year
        try:
            day = f"{year}-{int(m_q.group(2)):02d}-{int(m_q.group(3)):02d}"
        except ValueError:
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        "❌ 日期无效, 示例: 查9月5日 或 查2025年9月5日")
        rows = db.day_records(openid, day)
        if not rows:
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        f"📭 {day} 没有账目记录")
        total = sum(r["amount"] for r in rows)
        lines = [f"{r['type']} {_cat_disp(r)} ¥{r['amount']:.2f}"
                 + (f" {r['note']}" if r["note"] and r["note"] != r["category"] else "")
                 for r in rows]
        return wechat.to_text_reply(
            msg.get("ToUserName", ""), openid,
            f"📅 {day} 共{len(rows)}笔, 合计 ¥{total:.2f}:\n"
            + "\n".join(lines)
            + f"\n(删除某条: 删除{day} 分类 金额)")
    # 补记某天的账: 补记9月5日午饭30块(默认今年) / 补记2025年12月24日买礼物88元
    m_bu = re.match(r"^补记\s*(?:(\d{4})年)?\s*(\d{1,2})月(\d{1,2})[日号]\s*(.+)$", text) or \
           re.match(r"^补记\s*(?:(\d{4})-)?(\d{1,2})-(\d{1,2})\s+(.+)$", text)
    if m_bu:
        year = int(m_bu.group(1)) if m_bu.group(1) else now().year
        try:
            day = f"{year}-{int(m_bu.group(2)):02d}-{int(m_bu.group(3)):02d}"
        except ValueError:
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        "❌ 日期无效, 示例: 补记9月5日午饭30块")
        reply = handle_record(openid, m_bu.group(4).strip(), source, day)
        return wechat.to_text_reply(msg.get("ToUserName", ""), openid, reply)
    # 修改命令(方案A: 定位旧记录后删除, 再记入新内容): 支持多种写法
    #   修改9月7日 餐饮 35 打车 40 / 修改9月7日餐饮35打车40
    #   修改9月7日 餐饮 35 → 打车 40 / 改成 / 换成 / 替换为
    #   修改9月7日 餐饮 35 → 9月8日 打车 45 (含新日期)
    #   修改昨天 打车 20 打车 30
    m_mod = re.match(
        r"^\s*(?:修改|更正|变更|改账)\s*"
        r"(?:"
        r"(?P<rel>今天|昨天)|"
        r"(?:(?P<y>\d{4})年)?\s*(?P<m>\d{1,2})月(?P<d>\d{1,2})[日号]|"
        r"(?P<y2>\d{4})[年/\-]?(?P<m2>\d{1,2})[月/\-]?(?P<d2>\d{1,2})[日号]?"
        r")"
        r"\s*[：:，,]?\s*"
        r"(?P<ocat>\S+?)\s*[=：:，,]?\s*"
        r"(?P<oamt>\d+(?:\.\d+)?)[块元¥￥票]?\s*"
        r"(?P<rest>.+)$",
        text)
    if m_mod:
        if m_mod.group("rel"):
            back = 1 if m_mod.group("rel") == "昨天" else 0
            old_day = (now() - timedelta(days=back)).strftime("%Y-%m-%d")
        else:
            try:
                if m_mod.group("m"):
                    year = int(m_mod.group("y")) if m_mod.group("y") else now().year
                    month, day = int(m_mod.group("m")), int(m_mod.group("d"))
                else:
                    year = int(m_mod.group("y2"))
                    month, day = int(m_mod.group("m2")), int(m_mod.group("d2"))
                old_day = f"{year}-{month:02d}-{day:02d}"
            except ValueError:
                return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                            "❌ 日期无效, 示例: 修改9月7日 餐饮 35 打车 40")
        old_cat = m_mod.group("ocat")
        old_amt = round(float(m_mod.group("oamt")), 2)
        # 拆分 old 之后的新内容: 去掉分隔符(改成/→等) 与可选新日期
        rest = m_mod.group("rest").strip()
        for sepch in ("→", "->", "=>", "改成", "改为", "换成", "替换为",
                      "变更为", "成", "为", "换", "，", ",", "：", ":", "="):
            if rest.startswith(sepch):
                rest = rest[len(sepch):].strip()
                break
        new_day, src = _strip_date(rest)
        new_day = new_day or old_day
        # 定位并删除旧记录
        rec, more = db.delete_record_by_match(openid, old_day, old_cat, old_amt)
        if rec is None:
            return wechat.to_text_reply(
                msg.get("ToUserName", ""), openid,
                f"❌ 没有找到匹配记录: {old_day} {old_cat} ¥{old_amt:.2f}\n"
                f"可先发【查{old_cat}】/【最近】核对账目")
        # 删除成功后记入新内容(day 覆盖新日期, handle_record 会据此记入)
        new_reply = handle_record(openid, src, source, new_day)
        reply = (f"🗑 已删除旧账 {rec['tx_date']} {rec['category']} "
                 f"¥{rec['amount']:.2f} {rec['note']}\n{new_reply}")
        if more:
            reply += f"\n⚠️ 还有{more}条相同旧账, 未被删除"
        return wechat.to_text_reply(msg.get("ToUserName", ""), openid, reply)
    # 删除: 删除9月7日 餐饮 35 / 删除9月7日餐饮35
    #   / 删除2026-09-07 餐饮 35 / 删除昨天 打车 20块 / 删除9月7日 餐饮:35
    #   / 批量: 删除9月7日 奶茶6 洗澡2 喝水3
    m_del = re.match(
        r"^\s*删除\s*"
        r"(?:"                                    # 日期可选, 缺省默认今天
        r"(?P<rel>今天|昨天)|"                     # 删除今天/昨天 …
        r"(?:(?P<y>\d{4})年)?\s*(?P<m>\d{1,2})月(?P<d>\d{1,2})[日号]|"  # 删除[YYYY年]M月D日 …
        r"(?P<y2>\d{4})[年/\-]?(?P<m2>\d{1,2})[月/\-]?(?P<d2>\d{1,2})[日号]?"  # 删除2026-09-07 / 2026/9/7 / 20260907
        r")?"
        r"\s*[：:，,]?\s*"
        r"(?P<body>.+)$",
        text)
    if m_del:
        if m_del.group("rel"):                       # 今天/昨天
            back = 1 if m_del.group("rel") == "昨天" else 0
            day_s = (now() - timedelta(days=back)).strftime("%Y-%m-%d")
        elif m_del.group("m") or m_del.group("y"):   # M月D日 / YYYY年M月D日
            year = int(m_del.group("y")) if m_del.group("y") else now().year
            month, day = int(m_del.group("m")), int(m_del.group("d"))
            day_s = f"{year}-{month:02d}-{day:02d}"
        elif m_del.group("y2"):                      # YYYY-MM-DD / YYYY/MM/DD / YYYYMMDD
            month, day = int(m_del.group("m2")), int(m_del.group("d2"))
            day_s = f"{int(m_del.group('y2'))}-{month:02d}-{day:02d}"
        else:                                        # 未写日期, 默认删除今天
            day_s = now().strftime("%Y-%m-%d")
        try:
            datetime.strptime(day_s, "%Y-%m-%d")     # 校验日期合法(如2月30日会抛错)
        except ValueError:
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        "❌ 日期无效, 示例: 删除9月7日 餐饮 35 或 删除其他0")
        items = tokenize_amounts(m_del.group("body"))
        if not items:
            return wechat.to_text_reply(
                msg.get("ToUserName", ""), openid,
                "❌ 无法识别要删除的账目, 示例:\n"
                "  删除9月7日 餐饮 35\n  删除9月7日 奶茶6 洗澡2 喝水3")
        if len(items) == 1:                          # 单笔删除(分类+金额)
            kind, amount = items[0]
            rec, more = db.delete_record_by_match(openid, day_s, kind, amount)
            if rec is None:
                return wechat.to_text_reply(
                    msg.get("ToUserName", ""), openid,
                    f"❌ 没有找到匹配记录: {day_s} {kind} ¥{amount:.2f}\n"
                    f"可先发【查9月7日】核对该天的账目")
            reply = f"🗑 已删除 {rec['tx_date']} {rec['category']} ¥{rec['amount']:.2f} {rec['note']}"
            if more:
                reply += f"\n⚠️ 还有{more}条相同记录, 再次发送本命令可继续删除"
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid, reply)
        # 批量删除: 每个"名称 金额"项按用户输入名称精确定位
        deleted_lines, missing = [], []
        for note, amount in items:
            rec, _ = db.delete_record_by_match(openid, day_s, note, amount, note)
            if rec is not None:
                deleted_lines.append(f"  {note} ¥{amount:.2f}")
            else:
                missing.append(f"  {note} ¥{amount:.2f}")
        reply = f"🗑 已删除 {day_s} {len(deleted_lines)}笔:\n" + "\n".join(deleted_lines)
        if missing:
            reply += "\n❌ 未找到:\n" + "\n".join(missing)
        return wechat.to_text_reply(msg.get("ToUserName", ""), openid, reply)
    # AI分析: 分析(本月) / 分析总结 / 分析上月 / 分析本周 / 分析上周 / 分析今年 / 分析去年
    ana_periods = {"分析": "本月", "分析总结": "本月", "月分析": "本月", "分析本月": "本月",
                   "分析上月": "上月", "分析本周": "本周", "分析上周": "上周",
                   "分析今年": "今年", "分析去年": "去年"}
    if text in ana_periods:
        period = ana_periods[text]
        label, plabel, data = _analysis_data(openid, period)
        if not data["expense"] and not data["income"]:
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        f"📭 {label} 暂无收支记录, 记账后再来分析~")
        # 硬超时4s: 无论如何, 主流程都会在4秒内降级返回(微信5秒被动回复)
        try:
            fut = _llm_pool.submit(ai_summary, plabel, data)
            summary = fut.result(timeout=4.0)
        except Exception as e:
            print(f"[analysis] LLM超时/失败, 降级本地文字分析: {e}")
            summary = None
        if summary:
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        f"🤖 AI分析总结 {label}\n{summary}")
        # AI超时/失败: 降级为本地生成的"一段文字分析", 仍给出对整体情况的文字解读
        plain = _build_plain_analysis(label, plabel, data)
        return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                    f"🤖 AI分析总结 {label}\n{plain}")
    if text in ("帮助", "help", "指令"):
        return wechat.to_text_reply(msg.get("ToUserName", ""), openid, HELP)

    # ===== 清晰"名称金额"格式先走正则秒记: 不耗LLM额度/不依赖网络 =====
    # 开头"今天/昨天"就地换算日期; 含改账/删除意图或"帮我记"类前缀的交给Agent理解
    _FAST_SKIP = ("改成", "改为", "换成", "替换为", "变更为", "→", "->", "=>",
                  "帮我", "请帮", "记一下", "记一笔", "删除", "删掉")
    fast_date, fast_body = None, text
    if fast_body.startswith("昨天"):
        fast_date = (now() - timedelta(days=1)).strftime("%Y-%m-%d")
        fast_body = fast_body[2:].strip()
    elif fast_body.startswith("今天"):
        fast_date = now().strftime("%Y-%m-%d")
        fast_body = fast_body[2:].strip()
    if tokenize_amounts(fast_body) and not any(k in fast_body for k in _FAST_SKIP):
        batch = parse_batch(fast_body)
        if batch:                                # ≥2笔: 批量秒记(一次提交, 减少网络盘落盘次数)
            tx_date = fast_date or now().strftime("%Y-%m-%d")
            subs = _pick_subcategories([(c, "支出") for _, _, c in batch])
            rows, lines, total = [], [], 0.0
            for (note, amt, cat), (sub, src) in zip(batch, subs):
                rows.append(("支出", cat, amt, note, sub, src))
                total += amt
                lines.append(f"  {note} ¥{amt:.2f}")
            db.add_records_bulk(openid, tx_date, rows, source)
            return wechat.to_text_reply(
                msg.get("ToUserName", ""), openid,
                f"💸 已批量记账 {len(batch)}笔, 共 ¥{total:.2f}:\n"
                + "\n".join(lines))
        reply = handle_record(openid, fast_body, source, day=fast_date or "")
        return wechat.to_text_reply(msg.get("ToUserName", ""), openid, reply)

    # ===== AI Agent 层: 正则快路径未命中的自由表达, 交给LLM决定意图与参数 =====
    # 硬超时3.8s -> 工具校验执行; 超时/失败 -> 转下方正则兜底(不再二次调LLM);
    # LLM选了工具但执行失败 -> 提示未理解, 不再盲记垃圾账(避免"分析本月"误记账类问题)
    agent_attempted, agent_misroute = False, False
    if LLM_API_KEY:
        agent_attempted = True
        try:
            fut = _llm_pool.submit(agent.route, text)
            tool_name, tool_arg = fut.result(timeout=agent.AGENT_TIMEOUT + 0.3)
        except Exception as e:
            print(f"[agent] 路由超时/失败, 转正则兜底: {e}")
            tool_name, tool_arg = None, None
        if tool_name == "chitchat":
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        "👋 我是记账助手, 直接发「午饭35」就能记账\n"
                                        "发送【帮助】查看全部命令~")
        if tool_name and tool_name in AGENT_HANDLERS:
            try:
                reply = AGENT_HANDLERS[tool_name](openid, tool_arg or {}, source, text)
                if reply:
                    print(f"[agent] LLM路由 -> {tool_name} {tool_arg}")
                    return wechat.to_text_reply(msg.get("ToUserName", ""), openid, reply)
            except Exception as e:
                agent_misroute = True
                print(f"[agent] {tool_name} 执行失败({e}), 不盲记垃圾账")
        elif tool_name is None and tool_arg:
            # 模型判定与记账无关: 直接回复模型文本, 避免被当成账目误记
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        str(tool_arg)[:300])
    if agent_misroute:
        return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                    "🤔 没太理解这条消息~\n"
                                    "记账直接发「午饭35」; 其他操作可发【帮助】查看命令")

    # Agent路由失败/超时的兜底: 正则解析入库(不再二次调LLM, 避免超微信5秒)
    # 含删改意图的文本正则兜不住(会被误记成一笔账), AI又没理解时让用户重试
    if agent_attempted and any(k in text for k in
                               ("删除", "删掉", "去掉", "改成", "改为", "换成",
                                "替换为", "变更为", "改到", "挪到", "移到",
                                "修改", "改账")):
        return wechat.to_text_reply(
            msg.get("ToUserName", ""), openid,
            "⏳ AI处理超时了, 请再发一次试试~\n删改账也可直接用命令:\n"
            "  删除9月7日 餐饮 35\n  修改9月7日 餐饮 35 打车 40")
    reply = handle_record(openid, text, source)
    return wechat.to_text_reply(msg.get("ToUserName", ""), openid, reply)


@app.route("/push/daily", methods=["POST", "GET"])
def push_daily_endpoint():
    """手动/外部定时触发每日推送: /push/daily?token=xxx"""
    if request.args.get("token") != PUSH_TOKEN:
        return "forbidden", 403
    with db._session() as conn:
        openids = [r["openid"] for r in
                   conn.execute("SELECT DISTINCT openid FROM records")]
    results = {}
    for openid in openids or list(ALLOWED_OPENIDS):
        try:
            push_daily(openid)
            results[openid] = "ok"
        except Exception as e:
            results[openid] = str(e)
    return {"pushed": results}


@app.route("/push/weekly", methods=["POST", "GET"])
def push_weekly_endpoint():
    """手动/外部定时触发周报推送: /push/weekly?token=xxx"""
    if request.args.get("token") != PUSH_TOKEN:
        return "forbidden", 403
    with db._session() as conn:
        openids = [r["openid"] for r in
                   conn.execute("SELECT DISTINCT openid FROM records")]
    results = {}
    for openid in openids or list(ALLOWED_OPENIDS):
        try:
            push_weekly(openid)
            results[openid] = "ok"
        except Exception as e:
            results[openid] = str(e)
    return {"pushed": results}


@app.route("/push/annual", methods=["POST", "GET"])
def push_annual_endpoint():
    """手动/外部定时触发年度报告推送: /push/annual?token=xxx&year=2026"""
    if request.args.get("token") != PUSH_TOKEN:
        return "forbidden", 403
    year = None
    if request.args.get("year"):
        try:
            year = int(request.args.get("year"))
        except ValueError:
            return "bad year", 400
    with db._session() as conn:
        openids = [r["openid"] for r in
                   conn.execute("SELECT DISTINCT openid FROM records")]
    results = {}
    for openid in openids or list(ALLOWED_OPENIDS):
        try:
            push_annual(openid, year)
            results[openid] = "ok"
        except Exception as e:
            results[openid] = str(e)
    return {"pushed": results}


def _start_scheduler():
    """内置定时推送(本机长期运行时启用: RUN_SCHEDULER=1)"""
    from apscheduler.schedulers.background import BackgroundScheduler
    sched = BackgroundScheduler(timezone="Asia/Shanghai")
    sched.add_job(push_weekly_endpoint, "cron", day_of_week="sun",
                  hour=PUSH_HOUR, minute=PUSH_MINUTE)
    # 年度报告: 每年12月31日 21:00
    sched.add_job(push_annual_endpoint, "cron", month=12, day=31,
                  hour=PUSH_HOUR, minute=PUSH_MINUTE)
    sched.start()
    print(f"[scheduler] 每周日 {PUSH_HOUR:02d}:{PUSH_MINUTE:02d} 周报 / "
          f"每年12月31日 {PUSH_HOUR:02d}:{PUSH_MINUTE:02d} 年报 已启动")


if __name__ == "__main__":
    print(f"[config] 本次启动使用的Token: {WECHAT_TOKEN!r}  "
          f"(测试号页面填的Token必须与之一致)")
    if RUN_SCHEDULER:
        _start_scheduler()
    app.run(host="0.0.0.0", port=8000)
