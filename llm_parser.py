# -*- coding: utf-8 -*-
"""大模型解析记账文本: "午饭花了35块" -> {date, type, category, amount, note}
LLM未配置时自动退化为正则解析(只提取金额, 分类为其他)"""
import json
import re
import datetime

import requests

from config import LLM_API_KEY, CF_ACCOUNT_ID, CF_API_TOKEN, CF_MODEL

CATEGORIES = "餐饮/交通/购物/娱乐/居住/医疗/教育/人情/工资/理财/其他"

PROMPT = """你是记账助手。从用户消息中解析出一笔记账, 只输出如下JSON, 不要任何其他内容:
{{"date":"YYYY-MM-DD","type":"支出|收入","category":"细分类","subcategory":"大类","amount":数字,"note":"简要备注"}}

规则:
1. "今天"是{today}, "昨天"是{yesterday}; 未提到日期默认今天
2. category 是用户消费的具体名称(如咖啡/打车/房租/发工资), 不要归并成大分类
3. subcategory 是消费大类, 只能从 [餐饮/出行/购物/居住/其他] 中选择; 收入类记录 subcategory 填"收入"
4. amount 为正数, 只保留数字部分; 未提到金额则amount填0
5. note 用不超过10个字概括消费内容
6. 收款/收到红包/到账 => type为收入; 其余默认支出"""

# ==================== 大类映射(精简体系) ====================
# 细分类(用户输入即类别) -> 大类(subcategory), 用于消费统计与AI分析聚合
# 支出5类 + 收入1类; 本地匹配不到的大类交由 _cf_classify 用AI兜底
CATEGORY_MAP = [
    ("餐饮", ["早饭", "早餐", "午饭", "午餐", "晚饭", "晚餐", "夜宵", "外卖", "食堂",
              "饭", "面", "粉", "奶茶", "咖啡", "饮料", "零食", "水果", "火锅", "烧烤",
              "吃", "喝", "酒", "菜", "餐", "蛋糕", "面包", "甜点"]),
    ("出行", ["打车", "出租", "滴滴", "地铁", "公交", "高铁", "火车", "飞机", "机票",
              "加油", "停车", "充电", "高速", "过路", "共享单车", "骑行", "车票",
              "油费", "出行", "交通"]),
    ("购物", ["淘宝", "京东", "拼多多", "超市", "商场", "网购", "衣服", "鞋", "裤",
              "化妆品", "日用", "快递", "便利店", "文具", "数码", "手机", "家电",
              "购物", "买", "玩具", "包包", "首饰", "护肤"]),
    ("居住", ["房租", "水费", "电费", "燃气", "物业", "宽带", "网费", "话费",
              "维修", "家具", "家居", "水电"]),
]
INCOME_KEYWORDS = ("工资", "到账", "收入", "进账", "收到红包", "退款", "报销", "奖金",
                   "利息", "理财", "收益", "兼职")


def classify_local(name: str, rtype: str = "支出") -> str | None:
    """本地关键词快速归类: 返回大类; 判断不了返回None(交AI兜底)"""
    if rtype == "收入":
        return "收入"
    for cat, kws in CATEGORY_MAP:
        if any(k in name for k in kws):
            return cat
    return None


def _cf_classify(name: str, rtype: str = "支出", timeout: float = 3.0) -> str | None:
    """AI优先判断大类: 收入直接归"收入"; 输出含多个大类词(不确定)视为失败,
    由调用方本地兜底; 失败/未配凭据返回None"""
    if rtype == "收入":
        return "收入"
    cats = "餐饮/出行/购物/居住/其他"
    system = (f"你是记账分类助手。把消费项目归入以下大类之一: {cats}。"
              f"只输出一个大类名称, 不要解释、不要标点、不要列举多个。")
    out = _cf_chat(system, f"消费项目: {name}", timeout=timeout,
                   max_tokens=10, temperature=0)
    if not out:
        return None
    found = [c for c in ("餐饮", "出行", "购物", "居住", "其他", "收入") if c in out]
    return found[0] if len(found) == 1 else None


# 分类关键词兜底映射 (LLM不可用时使用)
KEYWORD_CATEGORY = [
    ("外卖|吃饭|午饭|晚饭|早饭|餐|奶茶|咖啡|食堂", "餐饮"),
    ("打车|地铁|公交|加油|停车|高铁|火车|机票|出租", "交通"),
    ("淘宝|京东|拼多多|购物|超市|买", "购物"),
    ("电影|游戏|KTV|旅游|会员", "娱乐"),
    ("房租|水电|燃气|物业", "居住"),
    ("医院|药|看病", "医疗"),
    ("书|课程|学费", "教育"),
    ("红包|礼金|随礼", "人情"),
    ("工资|到账|收入|进账|红包", "工资"),
]


def _cf_chat(system: str, user: str, timeout: float = 4.0,
             max_tokens: int = 600, temperature: float = 0.7) -> str | None:
    """Cloudflare Workers AI 文本生成(免费10k neurons/天); 失败/未配凭据返回None"""
    if not (CF_API_TOKEN and CF_ACCOUNT_ID):
        print("[cf] 未配置 CF_ACCOUNT_ID / CF_API_TOKEN")
        return None
    url = (f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
           f"/ai/run/{CF_MODEL}")
    body = {"messages": [{"role": "system", "content": system},
                         {"role": "user", "content": user}],
            "max_tokens": max_tokens, "temperature": temperature}
    try:
        r = requests.post(url,
                          headers={"Authorization": f"Bearer {CF_API_TOKEN}",
                                   "Content-Type": "application/json"},
                          data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                          timeout=timeout)
        if r.status_code != 200:
            print(f"[cf] HTTP {r.status_code}: {r.text[:200]}")
            return None
        j = r.json().get("result", {})
        txt = j.get("response")
        if not txt and isinstance(j.get("choices"), list) and j["choices"]:
            txt = j["choices"][0].get("message", {}).get("content")
        return (txt or "").strip() or None
    except Exception as e:
        print(f"[cf] 生成失败: {e}")
        return None


VALID_SUBS = ("餐饮", "出行", "购物", "居住", "其他", "收入")


def _llm_parse(text: str) -> dict | None:
    today = datetime.date.today()
    ctx = {"today": str(today), "yesterday": str(today - datetime.timedelta(days=1)),
           "cats": CATEGORIES}
    content = _cf_chat(PROMPT.format(**ctx), text, timeout=3.0, max_tokens=300, temperature=0)
    if not content:
        raise RuntimeError("CF无返回")
    # 提取JSON(模型偶尔会带```json包裹)
    m = re.search(r"\{.*\}", content, re.S)
    data = json.loads(m.group())
    sub = (data.get("subcategory") or "").strip()
    sub = sub if sub in VALID_SUBS else ""
    return {
        "date": data.get("date") or str(today),
        "type": data.get("type", "支出"),
        "category": data.get("category", "其他"),
        "subcategory": sub,
        "amount": round(float(data.get("amount", 0)), 2),
        "note": data.get("note", text[:20]),
    }


def _regex_parse(text: str) -> dict:
    """无LLM时的兜底: 只保证金额和日期, 分类按关键词粗略匹配"""
    today = datetime.date.today()
    d = today - datetime.timedelta(days=1) if "昨天" in text else today
    m = re.search(r"(\d+(?:\.\d+)?)\s*[块元]", text) or re.search(r"[¥￥](\d+(?:\.\d+)?)", text) \
        or re.search(r"(\d+(?:\.\d+)?)", text)
    amount = round(float(m.group(1)), 2) if m else 0.0
    rtype = "收入" if any(k in text for k in INCOME_KEYWORDS) else "支出"
    # 类别直接用用户输入的文字, 不再归类到预设大分类
    category = text[:20]
    return {"date": str(d), "type": rtype, "category": category,
            "subcategory": "", "amount": amount, "note": text[:20]}


REPORT_PROMPT = """你是记账助手。以下是用户{month}月的收支统计(JSON):
{data}

请用中文简洁输出(60字以内)，只引用数据中的数字，不要新增数字，不要给任何建议，格式:
本月支出：¥{expense}
本月收入：¥{income}
结余：¥{balance}"""

ANNUAL_PROMPT = """你是记账助手。以下是用户{year}年度账单统计(JSON):
{data}

请用中文生成一份年度总结(120字以内)，只引用数据中的数字，不要新增数字，不要给建议，格式:
{year}年总支出：¥{expense}
{year}年总收入：¥{income}
{year}年结余：¥{balance}
支出最多的分类：{top}
支出最多的月份：{top_month}"""

SUMMARY_PROMPT = """你是记账助手。以下是用户{period}的记账统计(JSON):
{data}

请用中文写一段自然的总结(约120字)，像朋友帮你复盘账单一样，一段话，不要列清单、不要用分点。
写到位这几点：
1. {period}支出/收入/结余大概什么水平，跟上期比是增是减
2. 支出最多的分类是哪个，合不合理，简单聊聊
3. 给1条贴合本数据的、可执行的小建议(不要空泛说教)"""


def ai_summary(period: str, data: dict, timeout: float = 4.0) -> str | None:
    """生成自然语气的周期总结(AI解读+建议), period 为"本月/上周/今年"等; 失败/未配Key返回None
    timeout 默认 4s: 微信被动回复限时5秒, 必须在此内返回或降级, 否则会触发 write error"""
    if not LLM_API_KEY:
        return None
    fmt = dict(data) | {"period": period, "data": json.dumps(data, ensure_ascii=False)}
    return _cf_chat(SUMMARY_PROMPT.format(**fmt), "请写总结", timeout=timeout, max_tokens=600)


def ai_report(kind: str, data: dict) -> str | None:
    """把数据库统计好的数据交给大模型生成解读报告; 失败/未配Key返回None"""
    if not LLM_API_KEY:
        return None
    fmt = dict(data) | {"month": kind.split("(")[0].replace("月度收支分析", ""),
                        "data": json.dumps(data, ensure_ascii=False)}
    return _cf_chat(REPORT_PROMPT.format(**fmt), "请生成报告", timeout=15, max_tokens=500)


def annual_report(year: str, data: dict) -> str | None:
    """用大模型生成年度账单总结; 失败/未配Key返回None(调用方退化纯文本)"""
    if not LLM_API_KEY:
        return None
    fmt = dict(data) | {"data": json.dumps(data, ensure_ascii=False)}
    return _cf_chat(ANNUAL_PROMPT.format(year=year, **fmt), "请生成年度总结",
                    timeout=20, max_tokens=700)


def parse_record(text: str, use_llm: bool = True) -> dict:
    """入口: 优先按"名称 金额"取类别(用户输入什么类别就记什么, 如"咖啡6"->咖啡);
    无法干净拆分时再用LLM/正则兜底; use_llm=False 跳过大模型只用正则
    subcategory 由调用方统一决定: LLM路径已带回大类(ai), 其余留空由app层AI优先判断"""
    items = tokenize_amounts(text)
    if items and len(items) == 1:
        name, amt = items[0]
        today = str(datetime.date.today())
        rtype = "收入" if any(k in name for k in INCOME_KEYWORDS) else "支出"
        r = {"date": today, "type": rtype, "category": name,
             "subcategory": "", "amount": amt, "note": name}
    elif LLM_API_KEY and use_llm:
        try:
            r = _llm_parse(text)
        except Exception as e:
            print(f"[llm_parser] LLM解析失败, 使用正则兜底: {e}")
            r = _regex_parse(text)
    else:
        r = _regex_parse(text)
    r.setdefault("subcategory", "")
    return r


def tokenize_amounts(body: str):
    """把"名称 金额"连续片段拆成 [(名称, 金额)]
    "奶茶6 洗澡2 喝水3" -> [('奶茶',6.0),('洗澡',2.0),('喝水',3.0)]
    "餐饮 35" -> [('餐饮',35.0)]; 中间夹了无关文字则返回 None"""
    tok = re.compile(
        r"([\u4e00-\u9fa5a-zA-Z]+)\s*[：:，,=\-、】~]?\s*(\d+(?:\.\d+)?)\s*[块元毛¥￥票]?")
    items, pos = [], 0
    for m in tok.finditer(body):
        gap = body[pos:m.start()]
        if re.search(r"[A-Za-z0-9\u4e00-\u9fa5]", gap):   # 两项之间有无关文字(如"花了")
            return None
        items.append((m.group(1).strip(), round(float(m.group(2)), 2)))
        pos = m.end()
    if re.search(r"[A-Za-z0-9\u4e00-\u9fa5]", body[pos:]):  # 结尾还有多余文字
        return None
    return items or None


def parse_batch(text: str):
    """连续多笔"名称+金额"记账: "奶茶6 洗澡2 喝水3"
    返回 [(名称, 金额, 分类)]; 不是≥2笔的连续批量时返回 None(走单笔逻辑)
    类别直接用用户输入的名称, 不做预设大分类"""
    items = tokenize_amounts(text)
    if not items or len(items) < 2:
        return None
    return [(n, a, n) for n, a in items]
