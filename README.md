# 微信记账助手

基于 Flask 的微信公众号记账机器人：在聊天窗口用一句自然语言完成记账、查账、删改与 AI 消费分析，数据存储于 SQLite，支持部署到 PythonAnywhere 等平台长期运行。

## 功能特性

- **自然语言记账**：`午饭35`、`昨天买书58`、`发工资12000`，发消息即入账
- **AI Agent 意图理解**：大模型 Function Calling 工具编排，自由表达如「帮我把昨天的奶茶删掉」「上周花了多少」也能正确路由到记账/删除/修改/查询/分析（基于 Cloudflare Workers AI）
- **批量记账**：`奶茶6 洗澡2 喝水3`，一条消息记多笔
- **补记 / 删除 / 修改**：`补记9月5日吃饭30`、`删除9月7日 餐饮 35`、`修改9月7日 餐饮 35 打车 40`
- **多维度查询**：`今日`、`本月`、`最近`、`查20260905`
- **AI 消费分析**：`分析本月` / `分析上周` / `分析去年`，大模型生成消费解读与建议（基于 Cloudflare Workers AI）
- **周报 / 年报**：周日发「周报」、12月31日发「年报」查看统计报告
- **每日定时推送（可选）**：通过 `RUN_SCHEDULER` 开关及 `PUSH_HOUR` / `PUSH_MINUTE` 配置
- **稳定应答保障**：线程池硬超时 + 本地文字分析降级，确保回复在微信 5 秒应答窗口内返回

## 命令一览

| 类型 | 示例 | 说明 |
|---|---|---|
| 记账 | `午饭35` | 直接发文字即可入账 |
| 批量记账 | `奶茶6 洗澡2 喝水3` | 一条消息记多笔 |
| 补记 | `补记9月5日吃饭30` / `补记2026-09-05 吃饭30` | 记入指定日期 |
| 删除 | `删除9月7日 餐饮 35` | 按日期 + 类别 + 金额删除，未写日期默认当天 |
| 修改 | `修改9月7日 餐饮 35 打车 40` | 将旧记录替换为新内容 |
| 查询 | `今日` / `本月` / `最近` / `查20260905` | 收支汇总与明细 |
| 自检 | `诊断` | 查看内存库/磁盘库记录数、日期范围，排查数据不一致 |
| AI 分析 | `分析本月` | 支持 本月 / 上月 / 本周 / 上周 / 今年 / 去年 |
| 周报 | `周报` | 仅每周日可查 |
| 年报 | `年报` | 仅每年 12 月 31 日可查 |
| 帮助 | `帮助` | 查看全部命令 |

## 快速开始

### 1. 环境要求

- Python 3.10+

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

### 3. 配置

复制 `.env.example` 为 `.env`，填入真实值：

```bash
cp .env.example .env
```

| 变量 | 说明 |
|---|---|
| `WECHAT_TOKEN` | 微信测试号「接口配置信息」里的 Token |
| `WECHAT_SECRET` | 微信测试号 appsecret |
| `PUSH_TOKEN` | 手动触发推送接口的口令 |
| `CF_ACCOUNT_ID` / `CF_API_TOKEN` / `CF_MODEL` | Cloudflare Workers AI 凭据与模型，用于 AI 分析；不配置则降级为本地文字分析 |
| `RUN_SCHEDULER` / `PUSH_HOUR` / `PUSH_MINUTE` | 内置定时推送开关与时间（默认每天 21:30） |
| `LEDGER_DB` | SQLite 数据库文件路径，默认 `ledger.db` |

### 4. 运行

```bash
python app.py
```

### 5. 微信测试号配置

1. 申请微信测试号：<https://mp.weixin.qq.com/debug/cgi-bin/sandbox>
2. 在「接口配置信息」中填写服务器地址：`https://你的域名/wechat`，Token 与 `WECHAT_TOKEN` 保持一致
3. 关注测试号后，在聊天窗口发送消息即可开始记账

## 部署到 PythonAnywhere

1. 将代码上传至 PythonAnywhere（或从 GitHub 拉取）
2. Web 页签新建 Manual configuration 应用（Python 3.10），配置 virtualenv 并安装依赖
3. WSGI 配置文件指向 `app`（Flask）
4. Source code 目录下放置 `.env`，Reload 后即可使用

## 项目结构

```
app.py          # Flask 入口: 消息回调、命令解析、AI 分析调度
db.py           # SQLite 数据层: 记录增删改查与统计
llm_parser.py   # 记账文本解析 + Cloudflare Workers AI 调用封装
report.py       # 日报 / 周报 / 年报统计与推送
daily_push.py   # 定时推送脚本（供 cron / 计划任务调用）
wechat.py       # 微信测试号接口封装（签名校验、收发消息）
config.py       # 全局配置（支持 .env 与环境变量覆盖）
```

## License

本项目基于 [MIT License](LICENSE) 开源。
