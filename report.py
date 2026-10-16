# -*- coding: utf-8 -*-
"""每日收支汇总: 生成报告文本 + 微信模板消息/客服消息推送"""
import datetime

import db
from config import DAILY_TEMPLATE_ID, LLM_API_KEY, now
from llm_parser import ai_report, annual_report
from wechat import send_custom_text, send_template, send_mass_text


def build_daily_report(openid: str) -> str:
    today = now().date()
    month = today.strftime("%Y-%m")

    t = db.today_stats(openid, str(today))
    m = db.month_stats(openid, month)
    cats = db.month_by_category(openid, month)

    top = "、".join(f"{c} ¥{v}" for c, v in cats[:3]) if cats else "暂无"
    lines = [
        f"📊 记账日报 {today}",
        f"────────────",
        f"今日支出: ¥{t['支出']:.2f}",
        f"今日收入: ¥{t['收入']:.2f}",
        f"────────────",
        f"本月支出: ¥{m['支出']:.2f}",
        f"本月收入: ¥{m['收入']:.2f}",
        f"本月支出Top3: {top}",
    ]
    return "\n".join(lines)


def _week_range(today: datetime.date = None):
    """返回本周(周一~周日)的 (起 YYYY-MM-DD, 止 YYYY-MM-DD)"""
    today = today or now().date()
    start = today - datetime.timedelta(days=today.weekday())   # 周一=0
    end = start + datetime.timedelta(days=6)
    return start, end


def build_weekly_report(openid: str) -> str:
    start, end = _week_range()
    s, e = start.isoformat(), end.isoformat()
    w = db.week_stats(openid, s, e)
    cats = db.week_by_category(openid, s, e)
    daily = db.week_daily(openid, s, e)
    top = "、".join(f"{c} ¥{v}" for c, v in cats[:3]) if cats else "暂无"
    lines = [
        f"📊 记账周报 {start} ~ {end}",
        f"────────────",
        f"本周支出: ¥{w['支出']:.2f}",
        f"本周收入: ¥{w['收入']:.2f}",
        f"本周结余: ¥{w['收入'] - w['支出']:.2f}",
        f"支出Top3: {top}",
    ]
    if daily:
        lines.append(f"────────────")
        for d in daily:
            lines.append(f"{d['date'][5:]} 支出¥{d['支出']:.2f}")
    return "\n".join(lines)


def _year_range(year: int):
    start = datetime.date(year, 1, 1)
    end = datetime.date(year, 12, 31)
    return start, end


def build_annual_report(openid: str, year: int) -> str:
    """生成年度报告纯文本(供无AI时的兜底用)"""
    y = str(year)
    s = db.year_stats(openid, y)
    cats = db.year_by_category(openid, y)
    months = db.year_monthly(openid, y)
    top = "、".join(f"{c} ¥{v}" for c, v in cats[:3]) if cats else "暂无"
    top_month = ""
    if months:
        top_month = max(months, key=lambda m: m["支出"])["month"][5:]
    return ("\n".join([
        f"📊 记账年报 {year}",
        f"────────────",
        f"全年支出: ¥{s['支出']:.2f}",
        f"全年收入: ¥{s['收入']:.2f}",
        f"全年结余: ¥{s['收入'] - s['支出']:.2f}",
        f"支出Top3: {top}",
        f"支出最多月: {top_month}月",
    ]), s, cats, months, top, top_month)


def push_annual(openid: str, year: int = None):
    """推送年度报告: 默认今年, 配置了LLM则生成AI年度总结, 否则纯文本"""
    year = year or now().date().year
    text, s, cats, months, top, top_month = build_annual_report(openid, year)
    # AI增强: 有任意收支记录才调大模型
    if LLM_API_KEY and (s["支出"] or s["收入"]):
        bal = round(s["收入"] - s["支出"], 2)
        bal_str = f"+{bal:.2f}" if bal > 0 else f"{bal:.2f}"
        data = {"year": year, "expense": s["支出"], "income": s["收入"],
                "balance": bal_str, "top": top or "暂无", "top_month": top_month or "无",
                "分类支出": dict(cats),
                "逐月支出": {m["month"][5:] + "月": m["支出"] for m in months}}
        ai_text = annual_report(str(year), data)
        if ai_text:
            text = f"🤖 AI年度报告 {year}\n{ai_text}"
    if DAILY_TEMPLATE_ID:
        send_template(openid, DAILY_TEMPLATE_ID, data={
            "expense": {"value": f"{year}年支出 ¥{s['支出']:.2f}"},
            "income":  {"value": f"{year}年收入 ¥{s['收入']:.2f}"},
            "remark":  {"value": f"结余 ¥{s['收入'] - s['支出']:.2f} · 最大支出{top_month}月"},
        })
    else:
        send_custom_text(openid, text)


def push_weekly(openid: str):
    """推送周报: 配置了模板ID用模板消息, 否则用群发文本"""
    start, end = _week_range()
    s, e = start.isoformat(), end.isoformat()
    w = db.week_stats(openid, s, e)
    text = build_weekly_report(openid)
    if DAILY_TEMPLATE_ID:
        send_template(openid, DAILY_TEMPLATE_ID, data={
            "expense": {"value": f"本周支出 ¥{w['支出']:.2f}"},
            "income":  {"value": f"本周收入 ¥{w['收入']:.2f}"},
            "remark":  {"value": f"周报 {start}~{end} 结余 ¥{w['收入'] - w['支出']:.2f}"},
        })
    else:
        send_custom_text(openid, text)


def push_daily(openid: str):
    """推送到微信: 配置了模板ID用模板消息, 否则用群发文本(个人订阅号每天1次)"""
    today = now().date()
    t = db.today_stats(openid, str(today))
    m = db.month_stats(openid, today.strftime("%Y-%m"))
    cats = db.month_by_category(openid, month:=today.strftime("%Y-%m"))
    text = build_daily_report(openid)

    # AI增强: 配置了LLM Key则生成简洁解读版日报, 失败自动退化纯文本
    if LLM_API_KEY:
        bal = round(m["收入"] - m["支出"], 2)
        bal_str = f"+{bal:.2f}" if bal > 0 else f"{bal:.2f}"
        data = {"月份": today.strftime("%Y-%m"), "expense": m["支出"],
                "income": m["收入"], "balance": bal_str,
                "分类支出": dict(cats)}
        ai_text = ai_report("今日收支日报" + today.strftime("%Y-%m"), data)
        if ai_text:
            text = f"📊 AI记账日报 {today}\n{ai_text}"

    if DAILY_TEMPLATE_ID:
        send_template(openid, DAILY_TEMPLATE_ID, data={
            "expense": {"value": f"¥{t['支出']:.2f}"},
            "income":  {"value": f"¥{t['收入']:.2f}"},
            "remark":  {"value": f"本月支出 ¥{m['支出']:.2f} / 收入 ¥{m['收入']:.2f}"},
        })
    else:
        send_custom_text(openid, text)
