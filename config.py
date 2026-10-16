# -*- coding: utf-8 -*-
"""全局配置: 全部支持环境变量覆盖, 也可直接改这里的默认值"""
import os
try:
    from dotenv import load_dotenv
    load_dotenv()          # 优先读取项目根目录 .env; 未安装/失败则静默跳过(仍可用系统环境变量)
except Exception:
    pass

# 统一时区为北京时间(Asia/Shanghai)。PythonAnywhere服务器默认UTC(比北京慢8小时),
# 不设的话 datetime.now() / SQLite datetime('now','localtime') / C库localtime
# 都会用UTC, 导致"今天"记错日期、今日统计错一天。
# 必须在任何取时间操作之前设置; config 被所有模块最先import, 故放这里。
import time as _time
os.environ.setdefault("TZ", "Asia/Shanghai")
try:
    _time.tzset()
except AttributeError:
    pass                  # Windows 无 tzset, 可忽略

from datetime import datetime as _datetime
try:
    from zoneinfo import ZoneInfo
except ImportError:       # Python<3.9 无 zoneinfo, 退回依赖上面设置好的TZ
    ZoneInfo = None


def now() -> _datetime:
    """当前北京时间(UTC+8)的 aware datetime。

    服务器(如PythonAnywhere)默认UTC, 光设 TZ 环境变量在 uWSGI/某些部署下
    可能不生效, 导致"今天/昨天/本月"错一天。这里显式用 zoneinfo 取北京
    时间, 业务代码统一调用本函数, 不直接使用 datetime.now()/date.today()。
    """
    if ZoneInfo is not None:
        try:
            return _datetime.now(ZoneInfo("Asia/Shanghai"))
        except Exception:
            pass
    return _datetime.now()

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
# 存储: 见 db.py。运行期用内存SQLite主库(微信秒回), 后台线程每
# DB_BACKUP_INTERVAL 秒把数据备份到磁盘 DB_PATH; 进程重启时自动从磁盘恢复。
# 背景: PythonAnywhere免费版(2026-01-15后新注册)不提供MySQL, 其网络盘上
# SQLite每次提交实测卡5~15秒, 远超微信5秒被动回复窗口, 故采用内存库+后台备份。
# SQLite文件路径: 相对路径锚定到本项目目录, 避免WSGI启动目录不同而读写到别处的空库
DB_PATH = os.getenv("LEDGER_DB", "ledger.db")
if not os.path.isabs(DB_PATH):
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), DB_PATH)
# DB_BACKUP_INTERVAL = 30   # 内存库->磁盘备份间隔(秒), 可在 .env 覆盖
PUSH_TOKEN = os.getenv("PUSH_TOKEN", "")                        # 敏感: 手动触发推送口令, 用环境变量设置
RUN_SCHEDULER = os.getenv("RUN_SCHEDULER", "1") == "1"                # 内置定时推送开关(本机运行用)
# 后台AI大类复核开关: 默认关闭。它会常驻线程反复读写账本库, 在PythonAnywhere
# 这类网络盘上极易卡住DB锁导致微信请求超时; 需要时设 SUBCAT_AI=1 打开
SUBCAT_AI = os.getenv("SUBCAT_AI", "0") == "1"
PUSH_HOUR = int(os.getenv("PUSH_HOUR", "21"))                         # 每日推送时间: 点
PUSH_MINUTE = int(os.getenv("PUSH_MINUTE", "30"))                     # 每日推送时间: 分
