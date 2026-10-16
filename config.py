# -*- coding: utf-8 -*-
"""全局配置: 全部支持环境变量覆盖, 也可直接改这里的默认值"""
import os
try:
    from dotenv import load_dotenv
    load_dotenv()          # 优先读取项目根目录 .env; 未安装/失败则静默跳过(仍可用系统环境变量)
except Exception:
    pass

# ---------- 微信测试号配置 (申请地址: https://mp.weixin.qq.com/debug/cgi-bin/sandbox) ----------
WECHAT_TOKEN = os.getenv("WECHAT_TOKEN", "")          # 敏感: 接口配置里的Token, 用环境变量设置
WECHAT_APPID = os.getenv("WECHAT_APPID", "wx2bc8c88a76b0cf50")               # 测试号信息的appID
WECHAT_SECRET = os.getenv("WECHAT_SECRET", "")        # 敏感: 测试号appsecret, 用环境变量设置

# 每日推送用的测试号模板消息ID (测试号页面"模板消息接口"->新增测试模板后获得)
# 模板内容建议: 支出:{{expense.DATA}}  收入:{{income.DATA}}  {{remark.DATA}}
# 留空则退化为客服文本消息推送
DAILY_TEMPLATE_ID = os.getenv("DAILY_TEMPLATE_ID", "ifAqL7SDeoE8NIxKr4I9WZqMXrawdSHvJghNZbovito")

# 允许使用的openid白名单, 逗号分隔; 留空=不限制(测试号本来就只有你自己)
ALLOWED_OPENIDS = set(filter(None, os.getenv("ALLOWED_OPENIDS", "").split(",")))

# ---------- 大模型配置 (任意OpenAI兼容接口: 智谱/DeepSeek/通义/Kimi/SiliconFlow等) ----------
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://open.bigmodel.cn/api/paas/v4")
LLM_API_KEY = os.getenv("LLM_API_KEY", "on")   # AI功能开关(非敏感): 非空即启用; 推理实际走Cloudflare
LLM_MODEL = os.getenv("LLM_MODEL", "glm-4-flash")

# ---------- Cloudflare Workers AI (AI分析/日报/年报的真实推理后端, 免费10k neurons/天) ----------
CF_ACCOUNT_ID = os.getenv("CF_ACCOUNT_ID", "ba4e567c939362ab8ef9be8dbd95d351")
CF_API_TOKEN  = os.getenv("CF_API_TOKEN", "")  # 敏感凭据, 勿提交git, 请用环境变量设置(见README/部署)
CF_MODEL      = os.getenv("CF_MODEL", "@cf/meta/llama-3.1-8b-instruct-fp8-fast")

# ---------- 其他 ----------
# 数据库后端: 填了 MYSQL_HOST/USER/DB 即用 MySQL(PythonAnywhere 推荐, 本机磁盘写入快);
# 未配置则回退 SQLite(DB_PATH, 部署在 PythonAnywhere 网络盘上, 写入可能很慢)
MYSQL_HOST = os.getenv("MYSQL_HOST", "")            # 例: ggglll77.mysql.pythonanywhere-services.com
MYSQL_USER = os.getenv("MYSQL_USER", "")            # 例: ggglll77 (PA Databases页的账号)
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")    # 敏感: PA Databases页设置/重置的密码
MYSQL_DB = os.getenv("MYSQL_DB", "")                # 例: ggglll77$default
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
# SQLite文件路径: 相对路径锚定到本项目目录, 避免WSGI启动目录不同而读写到别处的空库
DB_PATH = os.getenv("LEDGER_DB", "ledger.db")
if not os.path.isabs(DB_PATH):
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_PATH)
PUSH_TOKEN = os.getenv("PUSH_TOKEN", "")                        # 敏感: 手动触发推送口令, 用环境变量设置
RUN_SCHEDULER = os.getenv("RUN_SCHEDULER", "1") == "1"                # 内置定时推送开关(本机运行用)
# 后台AI大类复核开关: 默认关闭。它会常驻线程反复读写账本库, 在PythonAnywhere
# 这类网络盘上极易卡住DB锁导致微信请求超时; 需要时设 SUBCAT_AI=1 打开
SUBCAT_AI = os.getenv("SUBCAT_AI", "0") == "1"
PUSH_HOUR = int(os.getenv("PUSH_HOUR", "21"))                         # 每日推送时间: 点
PUSH_MINUTE = int(os.getenv("PUSH_MINUTE", "30"))                     # 每日推送时间: 分
