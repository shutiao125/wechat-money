"""微信测试号回调服务入口: 消息回调 + 记账
运行: python app.py  (开发时配合内网穿透暴露公网HTTPS)
"""
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from flask import Flask, request

from config import (WECHAT_TOKEN, ALLOWED_OPENIDS, PUSH_TOKEN,
                    RUN_SCHEDULER, PUSH_HOUR, PUSH_MINUTE, LLM_API_KEY)
import wechat
import db
from llm_parser import (parse_record, ai_report, parse_batch,
                        tokenize_amounts, annual_report, ai_summary)
from report import (build_daily_report, build_weekly_report,
                    build_annual_report, push_daily, push_weekly, push_annual)

app = Flask(__name__)
db.init_db()

# MsgId去重(微信5秒无响应会重试推送)
_seen_msgs = set()
_seen_lock = threading.Lock()

# AI分析专用线程池: 用 result(timeout) 强制"总时长"硬超时(requests的read
# timeout只看两次读间隔, 无法限制总时长, 曾导致被动回复超过微信5秒上限)
_llm_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="llm")

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
        day = (datetime.now() - timedelta(days=back)).strftime("%Y-%m-%d")
        return day, s[m.end():].strip()
    m = re.match(r"^(?:(?P<y>\d{4})年)?(?P<m>\d{1,2})月(?P<d>\d{1,2})[日号]", s)
    if m:
        year = int(m.group("y")) if m.group("y") else datetime.now().year
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
    """显示用分类: 大分类·细分类"""
    return r["category"] + (f"·{r.get('subcategory')}" if r.get("subcategory") else "")


def _analysis_data(openid: str, period: str):
    """统计指定周期的收支供AI分析, period为 本月/上月/本周/上周/今年/去年
    返回 (展示标题, 周期标签, data字典); data含 expense/income/balance/分类支出/上期支出/上期收入"""
    now = datetime.now()
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


def handle_record(openid: str, text: str, source: str, day: str = "") -> str:
    """同步解析并入库, 返回确认文本(作为被动回复, 未认证订阅号无客服消息接口)
    day 非空时覆盖解析出的日期(供补记/修改指定日期使用)"""
    r = parse_record(text)
    sub = r.get("subcategory", "")
    tx_date = day or r["date"]
    db.add_record(openid, tx_date, r["type"], r["category"],
                  r["amount"], r["note"], source, sub)
    icon = "💸" if r["type"] == "支出" else "💰"
    return (f"{icon} 已记账 {tx_date}\n"
            f"{_cat_disp(r)} ¥{r['amount']:.2f} | {r['note']}\n"
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
    if not wechat.check_signature(WECHAT_TOKEN, request.args.get("signature", ""),
                                  request.args.get("timestamp", ""),
                                  request.args.get("nonce", "")):
        return "success"
    msg = wechat.parse_xml(request.data)
    if not msg:
        return "success"

    openid = msg.get("FromUserName", "")
    msg_type = msg.get("MsgType", "")
    print(f"[msg] openid={openid!r} MsgType={msg_type!r} Content={msg.get('Content','')!r}")
    if not _allowed(openid):
        return "success"

    # 关注事件
    if msg_type == "event" and msg.get("Event") == "subscribe":
        return wechat.to_text_reply(msg.get("ToUserName", ""), openid, WELCOME)

    # 按MsgId去重
    if msg.get("MsgId") and _is_duplicate(msg["MsgId"]):
        return "success"

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
    today = datetime.now().strftime("%Y-%m-%d")
    if text in ("今日", "今天"):
        t = db.today_stats(openid, today)
        return wechat.to_text_reply(
            msg.get("ToUserName", ""), openid,
            f"📊 今日支出: ¥{t['支出']:.2f}\n📊 今日收入: ¥{t['收入']:.2f}")
    if text in ("本月", "月报"):
        month = datetime.now().strftime("%Y-%m")
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
                 f"{r['note']} [{r['type']}]"
                 for r in rows]
        return wechat.to_text_reply(
            msg.get("ToUserName", ""), openid,
            "📋 最近5笔:\n" + "\n".join(lines))
    # 周报: 仅每周日可查询
    if text in ("周报", "本周"):
        if datetime.now().weekday() != 6:     # weekday(): 周一=0 ... 周日=6
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        "🕐 周报仅每周日可查询, 到时再发「周报」~")
        return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                    build_weekly_report(openid))
    # 年报: 仅每年12月31日可查询(查询当年)
    if text in ("年报", "今年"):
        if datetime.now().month != 12 or datetime.now().day != 31:
            return wechat.to_text_reply(msg.get("ToUserName", ""), openid,
                                        "🕐 年报仅每年12月31日可查询, 到时再发「年报」~")
        year = datetime.now().year
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
        year = int(m_q.group(1)) if m_q.group(1) else datetime.now().year
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
        lines = [f"{r['type']} {_cat_disp(r)} ¥{r['amount']:.2f} {r['note']}"
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
        year = int(m_bu.group(1)) if m_bu.group(1) else datetime.now().year
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
            old_day = (datetime.now() - timedelta(days=back)).strftime("%Y-%m-%d")
        else:
            try:
                if m_mod.group("m"):
                    year = int(m_mod.group("y")) if m_mod.group("y") else datetime.now().year
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
            day_s = (datetime.now() - timedelta(days=back)).strftime("%Y-%m-%d")
        elif m_del.group("m") or m_del.group("y"):   # M月D日 / YYYY年M月D日
            year = int(m_del.group("y")) if m_del.group("y") else datetime.now().year
            month, day = int(m_del.group("m")), int(m_del.group("d"))
            day_s = f"{year}-{month:02d}-{day:02d}"
        elif m_del.group("y2"):                      # YYYY-MM-DD / YYYY/MM/DD / YYYYMMDD
            month, day = int(m_del.group("m2")), int(m_del.group("d2"))
            day_s = f"{int(m_del.group('y2'))}-{month:02d}-{day:02d}"
        else:                                        # 未写日期, 默认删除今天
            day_s = datetime.now().strftime("%Y-%m-%d")
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

    # 批量记账: "奶茶6 洗澡2 喝水3" -> 一次记入多笔, 名称自动分类入库
    batch = parse_batch(text)
    if batch:
        tx_date = datetime.now().strftime("%Y-%m-%d")
        lines, total = [], 0.0
        for note, amt, cat in batch:
            db.add_record(openid, tx_date, "支出", cat, amt, note, source, note)
            total += amt
            lines.append(f"  {cat}·{note} ¥{amt:.2f}")
        return wechat.to_text_reply(
            msg.get("ToUserName", ""), openid,
            f"💸 已批量记账 {len(batch)}笔, 共 ¥{total:.2f}:\n"
            + "\n".join(lines))

    # 记账: 同步解析, 结果作为被动回复(未认证订阅号无客服消息接口)
    reply = handle_record(openid, text, source)
    return wechat.to_text_reply(msg.get("ToUserName", ""), openid, reply)


@app.route("/push/daily", methods=["POST", "GET"])
def push_daily_endpoint():
    """手动/外部定时触发每日推送: /push/daily?token=xxx"""
    if request.args.get("token") != PUSH_TOKEN:
        return "forbidden", 403
    with db._conn() as conn:
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
    with db._conn() as conn:
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
    with db._conn() as conn:
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
