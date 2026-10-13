# -*- coding: utf-8 -*-
"""AI Agent 意图路由层: Function Calling 工具编排
把 记账/删除/修改/查账/AI分析 注册为工具(tools), 由大模型根据用户消息
自主决定调用哪个工具并抽取参数, 用于理解正则快路径覆盖不到的自由表达。
未配置凭据/超时/未调用工具时返回 None, 调用方转正则兜底, 保证可用性。"""
import json
from datetime import date

import requests

from config import CF_ACCOUNT_ID, CF_API_TOKEN, CF_MODEL

AGENT_TIMEOUT = 3.5   # 单次路由硬超时(秒): 微信被动回复限时5秒, 必须留出执行与回复余量

SYSTEM_TMPL = (
    "你是记账助手的意图路由器。今天是{today}。根据用户消息从工具中选择最合适的一个调用, "
    "参数严格按工具schema填写:\n"
    "- 记一笔或多笔账(含收入)用 record; 删除已有账目用 delete;\n"
    "- 修改已有账目(把某天的某笔换成新内容)用 modify;\n"
    "- 查账(今天/昨天/本月/最近几笔/某个日期)用 query;\n"
    "- 想看消费分析/总结/钱花在哪/收支情况用 analyze。\n"
    "日期一律换算成 YYYY-MM-DD(以今天为基准推算昨天/上周/上月等)。\n"
    "类别直接用用户原话中的词(如 咖啡/奶茶/午饭/打车), 不要归类成大类。\n"
    "金额只填纯数字; 用户说工资/到账/收到红包/退款用 type=收入, 其余为支出。\n"
    "删除或修改时, 用户没提到的金额不要自己猜。\n"
    "若消息与记账查账完全无关(如闲聊问好), 调用 chitchat 工具, 不要调用其他工具。")

_TOOLS = [
    ("chitchat", "用户消息与记账/查账完全无关时调用(如闲聊问好、聊天)",
     {"type": "object", "properties": {}}),
    ("record", "记账: 把用户提到的消费或收入写入账本, 支持一次记多笔",
     {"type": "object", "properties": {
         "date": {"type": "string", "description": "记账日期YYYY-MM-DD; 没说日期则填今天的日期"},
         "items": {"type": "array", "description": "账目条目列表", "items": {
             "type": "object", "properties": {
                 "type": {"type": "string", "enum": ["支出", "收入"]},
                 "category": {"type": "string", "description": "类别, 用用户原话的词"},
                 "amount": {"type": "number", "description": "金额纯数字"},
                 "note": {"type": "string", "description": "简短备注"}},
             "required": ["category", "amount"]}}},
      "required": ["items"]}),
    ("delete", "删除账本里已有的记录, 按日期+类别+金额匹配",
     {"type": "object", "properties": {
         "date": {"type": "string", "description": "YYYY-MM-DD; 没说日期默认今天"},
         "items": {"type": "array", "description": "要删除的条目列表", "items": {
             "type": "object", "properties": {
                 "category": {"type": "string"},
                 "amount": {"type": "number", "description": "用户说了金额才填, 没说不要猜"}},
             "required": ["category"]}}},
      "required": ["items"]}),
    ("modify", "修改一条已有记录: 把某日期某类别的账目改成新内容",
     {"type": "object", "properties": {
         "date": {"type": "string", "description": "原记录日期YYYY-MM-DD"},
         "old_category": {"type": "string", "description": "原记录类别"},
         "old_amount": {"type": "number", "description": "原金额, 用户说了才填, 没说不要猜"},
         "new_category": {"type": "string"}, "new_amount": {"type": "number"},
         "new_date": {"type": "string", "description": "改到新日期才填, 可选"}},
      "required": ["date", "old_category", "new_category", "new_amount"]}),
    ("query", "查账: 今日/昨日/本月/最近几笔/指定日期的收支情况",
     {"type": "object", "properties": {
         "period": {"type": "string", "enum": ["today", "yesterday", "this_month", "recent", "date"]},
         "date": {"type": "string", "description": "period=date时的具体日期YYYY-MM-DD"}},
      "required": ["period"]}),
    ("analyze", "分析消费情况并给总结建议, 支持六个周期",
     {"type": "object", "properties": {
         "period": {"type": "string", "enum": ["本月", "上月", "本周", "上周", "今年", "去年"]}},
      "required": ["period"]}),
]

TOOLS = [{"type": "function", "function": {"name": n, "description": d,
                                           "parameters": p}}
         for n, d, p in _TOOLS]


def _maybe_json(v):
    """8B小模型常把嵌套参数输出成JSON字符串, 能解析就还原成对象"""
    if isinstance(v, str):
        try:
            return json.loads(v)
        except Exception:
            return v
    return v


def _num(v):
    """字符串金额("25"/"25元"/"25块")转数字"""
    if isinstance(v, str):
        try:
            return float(v.replace("元", "").replace("块", "").strip())
        except ValueError:
            return v
    return v


def _coerce(name: str, args):
    """规范化工具参数: JSON字符串还原 / 单条items包装成列表 / 字符串金额转数字"""
    if not isinstance(args, dict):
        return {}
    for k in ("items", "date", "period", "new_date", "old_amount", "new_amount"):
        if k in args:
            args[k] = _maybe_json(args[k])
    if isinstance(args.get("items"), dict):          # 单条误输出为对象
        args["items"] = [args["items"]]
    if name in ("record", "delete") and isinstance(args.get("items"), list):
        fixed = []
        for it in args["items"]:
            it = _maybe_json(it)
            if isinstance(it, dict):
                it["amount"] = _num(it.get("amount"))
                fixed.append(it)
        args["items"] = fixed
    for k in ("old_amount", "new_amount"):
        args[k] = _num(args.get(k)) if k in args else args.get(k)
    return args


def route(text: str):
    """调用LLM做意图路由。
    返回 (工具名, 参数dict)         —— 模型决定调用某工具
         (None, 非空文本)          —— 模型判定与记账无关, 文本为闲聊回复
         (None, None)              —— 未配置凭据/超时/失败, 调用方转正则兜底"""
    if not (CF_API_TOKEN and CF_ACCOUNT_ID):
        return None, None
    url = (f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
           f"/ai/run/{CF_MODEL}")
    body = {"messages": [{"role": "system",
                          "content": SYSTEM_TMPL.format(today=date.today())},
                         {"role": "user", "content": text}],
            "tools": TOOLS, "tool_choice": "auto",
            "max_tokens": 300, "temperature": 0}
    try:
        r = requests.post(url,
                          headers={"Authorization": f"Bearer {CF_API_TOKEN}",
                                   "Content-Type": "application/json"},
                          data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                          timeout=AGENT_TIMEOUT)
    except Exception as e:
        print(f"[agent] 请求失败: {e}")
        return None, None
    if r.status_code != 200:
        print(f"[agent] HTTP {r.status_code}: {r.text[:200]}")
        return None, None
    j = r.json().get("result") or {}
    calls = j.get("tool_calls") or []
    text_out = j.get("response")
    if not calls:                                    # OpenAI兼容形态兜底
        m = (j.get("choices") or [{}])[0].get("message") or {}
        calls = m.get("tool_calls") or []
        text_out = text_out or m.get("content")
    for c in calls:
        if not isinstance(c, dict):
            continue
        name, args = c.get("name"), c.get("arguments")
        fn = c.get("function")
        if fn:
            name = name or fn.get("name")
            args = args if args is not None else fn.get("arguments")
        if not name:
            continue
        if isinstance(args, str):                    # 有的模型返回JSON字符串
            try:
                args = json.loads(args)
            except Exception:
                args = {}
        return str(name), _coerce(str(name), args if isinstance(args, dict) else {})
    return None, (text_out or "").strip() or None
