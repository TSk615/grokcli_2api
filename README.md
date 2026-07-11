# grokcli-2api

把 **Grok OIDC 登录态** 转成 **OpenAI / Anthropic 兼容 API**，并附带 Web 管理台：多 API Key、多账号轮询、设备码 / 导入 / 协议注册。

**当前版本：v1.8.11**

- **独立运行**：不依赖本地 Grok CLI，不调用 `grok login` / 浏览器 OAuth
- **协议注册**：内置 `grok-build-auth`（HTTP 协议，无需 Chromium）
- **中继友好**：兼容 new-api 操练场 / 测速（剥离不支持参数、reasoning 兼容、SSE keepalive）
- **运维增强**：账号搜索、多选批量删除 / 续期 / 导出、多线程批量注册
- **大账号池友好**：Token 自动续期 + 模型健康检查可常开，批处理 / 互斥 / 轻量状态接口

适合 Cherry Studio、NextChat、OpenAI SDK、Anthropic SDK、Claude Code、Cursor、new-api 等工具接入。

---

## 本次更新（v1.8.11）

| 方向 | 内容 |
|------|------|
| 修复 | `think_tag` 兼容态在 reasoning/content 交替输出时，第二次及以后 `<think>` 无法闭合的问题 |
| 说明 | 去掉 `_ReasoningCompatState.closed` 闩锁，只靠 `think_open` 控制开闭，交替流可正确 reopen/reclose |
| 默认 | `GROK2API_REASONING_COMPAT` 仍为 `off`；仅在显式 `think_tag` 时写入正文标签 |

### 相关环境变量（可选）

```bash
# off（默认）: reasoning 走 reasoning_content，正文不带 <think>
# think_tag: 把 reasoning 包进 <think>...</think> 写入 content（兼容只读 content 的中继）
# content: 合并 reasoning 到 content，不带标签
GROK2API_REASONING_COMPAT=off
```

---

## 原理

```
客户端 (OpenAI / Anthropic SDK · new-api · GUI)
        │  POST /v1/chat/completions   (OpenAI)
        │  POST /v1/messages           (Anthropic)
        │  Authorization: Bearer …  或  x-api-key: …
        ▼
  grokcli-2api  (FastAPI)
        │  管理台 /admin
        │  Anthropic ↔ OpenAI 协议转换
        │  读取 data/auth.json（多账号池）
        │  轮询 + 失败切换 + 对话粘性
        │  参数清洗 / reasoning 兼容 / 流式 keepalive
        │  附加 X-XAI-Token-Auth / x-grok-client-*
        ▼
  cli-chat-proxy.grok.com
```

凭证与配置保存在项目 `data/`（或 `GROK2API_DATA_DIR`）。

---

## 功能

| 功能 | 说明 |
|------|------|
| OpenAI 兼容 | `GET /v1/models` · `POST /v1/chat/completions` · 流式 SSE |
| Anthropic 兼容 | `POST /v1/messages` · 流式 event SSE · tools/tool_use · `count_tokens` |
| Tools / 函数调用 | OpenAI `tool_calls` · Anthropic `tool_use` / `tool_result` |
| 管理台 | `http://127.0.0.1:3000/admin` 账号 / Key / 注册 / 轮询 / 指南 |
| 多 API Key | 创建、停用、删除；哈希存储 |
| 多账号轮询 | `round_robin` / `least_used` / `random` |
| 对话粘性 | 同会话固定账号；失败才 failover |
| 失败切换 | 上游 401/429/5xx 冷却并换号 |
| 额度查询 | 管理台查询 `/v1/billing`；耗尽自动移出轮询 |
| Token 自动续期 | 后台维护线程自适应刷新 |
| 模型探测 | 单账号 / 全量探测；异常自动屏蔽 |
| **授权方式** | 设备码登录 · 导入 auth.json / JWT / SSO · **协议注册** |
| **协议注册** | MoeMail 临时邮箱 + YesCaptcha Turnstile + 自动导入账号池 |
| **多线程注册** | `count` / `concurrency` / `stagger_ms` 批量并发 |
| **账号运维** | 搜索 · 多选 · 批量删除 |
| 中继兼容 | new-api 测速 / 操练场流式；自动剥离不支持采样参数 |

---

## 前置条件

1. Python **3.10+**（推荐 3.12）
2. 可访问：
   - `cli-chat-proxy.grok.com`（对话上游）
   - `auth.x.ai` / `accounts.x.ai`（设备码、刷新、协议注册）
3. 若使用协议注册：
   - **MoeMail** API Key（临时邮箱）
   - **YesCaptcha** API Key（Turnstile）

---

## 安装与启动

### Windows

```powershell
cd $env:USERPROFILE\Desktop\grokcli-2api
pip install -r requirements.txt
python app.py
# 或
.\start.ps1
```

### Linux

```bash
cd /opt/grokcli-2api   # 或你的部署目录
python3 -m pip install -r requirements.txt

export GROK2API_HOST=0.0.0.0
export GROK2API_PORT=3000
export GROK2API_OPEN_BROWSER=0
export GROK2API_ADMIN_PASSWORD='your-strong-password'
export GROK2API_ACCOUNT_MODE=round_robin
export GROK2API_REASONING_COMPAT=off   # sub2api/Claude Code 推荐；仅 content 中继才用 think_tag

# 可选：协议注册
export GROK2API_MOEMAIL_API_KEY='mk_xxx'
export GROK2API_MOEMAIL_BASE_URL='https://moemail.521884.xyz'
export GROK2API_MOEMAIL_DOMAIN='lolicc.online'
export GROK2API_YESCAPTCHA_KEY='your-yescaptcha-key'

chmod +x start.sh
./start.sh
# 后台
nohup ./start.sh > grok2api.log 2>&1 &
```

### Docker

```bash
docker compose up -d --build
# 或
./docker-rebuild.sh
```

镜像基于协议注册栈（`curl_cffi` / `requests`）。

### 默认地址

| 地址 | 说明 |
|------|------|
| http://127.0.0.1:3000/admin | Web 管理台 |
| http://127.0.0.1:3000/docs | Swagger |
| http://127.0.0.1:3000/health | 健康检查（含 registration 状态） |
| http://127.0.0.1:3000/v1 | OpenAI Base URL |
| http://127.0.0.1:3000/v1/messages | Anthropic Messages API |
| http://127.0.0.1:3000 | Anthropic SDK `base_url`（根地址） |

---

## 如何授权

### 方式 A：设备码登录（推荐日常）

1. 管理台 → **账号 / 轮询**
2. 点 **设备码登录**
3. 浏览器打开验证链接并输入设备码
4. 刷新账号列表

### 方式 B：导入

管理台支持：

- 完整 `auth.json` 文件上传（可合并）
- JWT 访问令牌
- **SSO Cookie** 批量导入（Device Flow 自动换 token）

### 方式 C：协议注册（MoeMail + YesCaptcha）

基于内置 `grok-build-auth` HTTP 协议，**无需浏览器**：

1. 配置 MoeMail + YesCaptcha（环境变量或管理台表单）
2. 管理台 → 协议注册
3. 设置：
   - **注册数量**（批量）
   - **并发数**（多线程）
   - **启动间隔 ms**（错峰，减限流）
4. 启动后自动：建号 → 提取 SSO → 转 token → 导入账号池

> 注册成功与否依赖邮箱域名信誉、Turnstile 打码质量与 xAI 风控。若出现 `wke=email:invalid-validation-code`，多为验证码时效问题（当前版本已改为“先打码再收码”）。

#### 注册 API 示例

```bash
curl -X POST http://127.0.0.1:3000/admin/api/accounts/register-email \
  -H "X-Admin-Token: <admin-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "count": 5,
    "concurrency": 2,
    "stagger_ms": 500,
    "yescaptcha_key": "YOUR_YESCAPTCHA_KEY",
    "api_key": "YOUR_MOEMAIL_KEY",
    "domain": "lolicc.online"
  }'
```

查询：

- `GET /admin/api/accounts/register-email/sessions`
- `GET /admin/api/accounts/register-email/batches/{batch_id}`

---

## 多账号轮询

| 模式 | 行为 |
|------|------|
| `round_robin` | 顺序轮流（默认，推荐） |
| `least_used` | 优先请求更少的账号 |
| `random` | 随机 |

- 账号完全对等，无主账号
- 可单独启用 / 禁用
- 失败冷却：401 约 5 分钟，429/5xx 约 1 分钟
- 额度耗尽自动移出轮询；恢复后可手动启用

### 账号运维（管理台）

- **搜索**：邮箱 / 账号 ID / 状态关键字
- **多选**：本页全选 / 筛选结果全选
- **批量删除**：`POST /admin/api/accounts/delete-batch`

---

## 快速上手

1. 打开管理台 → 设置管理员密码  
2. **账号** 页：设备码登录 / 导入 / 协议注册  
3. 选择轮询策略（默认 `round_robin`）  
4. **API Keys** 页创建 `sk-g2a-...`  
5. 客户端：Base URL + Key + 模型 `grok-4.5`

### curl

```bash
curl http://127.0.0.1:3000/v1/models -H "Authorization: Bearer sk-g2a-YOUR_KEY"

curl http://127.0.0.1:3000/v1/chat/completions \
  -H "Authorization: Bearer sk-g2a-YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"grok-4.5","messages":[{"role":"user","content":"你好"}],"stream":false}'
```

### OpenAI Python SDK

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:3000/v1",
    api_key="sk-g2a-YOUR_KEY",
)
r = client.chat.completions.create(
    model="grok-4.5",
    messages=[{"role": "user", "content": "Hello"}],
)
print(r.choices[0].message.content)
```

### Anthropic Python SDK

```python
from anthropic import Anthropic

client = Anthropic(base_url="http://127.0.0.1:3000", api_key="sk-g2a-YOUR_KEY")
msg = client.messages.create(
    model="grok-4.5",  # 或 claude-sonnet-4 等别名
    max_tokens=1024,
    messages=[{"role": "user", "content": "Hello"}],
)
print(msg.content[0].text)
```

`claude-*` 模型名会自动映射到默认 Grok 模型。

### Tools / Function Calling

```python
import json
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:3000/v1", api_key="sk-g2a-YOUR_KEY")

tools = [{
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Get weather for a city",
        "parameters": {
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
    },
}]

messages = [{"role": "user", "content": "北京天气怎么样？"}]
r = client.chat.completions.create(
    model="grok-4.5",
    messages=messages,
    tools=tools,
    tool_choice="auto",
)
msg = r.choices[0].message
if msg.tool_calls:
    messages.append(msg)
    for tc in msg.tool_calls:
        args = json.loads(tc.function.arguments)
        result = {"city": args["city"], "temp_c": 26, "condition": "晴"}
        messages.append({
            "role": "tool",
            "tool_call_id": tc.id,
            "content": json.dumps(result, ensure_ascii=False),
        })
    r2 = client.chat.completions.create(model="grok-4.5", messages=messages, tools=tools)
    print(r2.choices[0].message.content)
else:
    print(msg.content)
```

---

## 接入 new-api / 中继

推荐渠道配置：

| 项 | 建议 |
|----|------|
| 类型 | OpenAI 兼容 |
| Base URL | `http://host:40081` 或 Docker 内网 `http://172.17.0.1:40081` |
| 模型 | `grok-4.5` |
| `thinking_to_content` | **建议关闭**（默认不再把 reasoning 写入 content；需要时再开） |
| `auto_ban` | 建议关闭或谨慎开启 |

说明：

- 上游 **不支持** `presence_penalty` / `frequency_penalty` 等参数；本服务会自动剥离，避免操练场空白回复
- 默认把 reasoning 放在 `reasoning_content`，**不**写入正文；需要中继可见思考时设 `GROK2API_REASONING_COMPAT=think_tag`
- 长思考间隔会发 SSE keepalive，降低中继空闲断流

相关环境变量：

- `GROK2API_REASONING_COMPAT=off|think_tag|content`（默认 `off`）
- `GROK2API_SSE_KEEPALIVE=8`（秒）

---

## 环境变量

### 基础

| 变量 | 默认 | 说明 |
|------|------|------|
| `GROK2API_HOST` | `127.0.0.1` | 监听地址（服务器用 `0.0.0.0`） |
| `GROK2API_PORT` | `3000` | 端口 |
| `GROK2API_PUBLIC_BASE_URL` | 空 | 公网访问根地址（如 `https://api.example.com`）；管理台/接入指南优先用它，避免显示 127.0.0.1 |
| `GROK2API_ADMIN_PASSWORD` | 空 | 管理台密码 |
| `GROK2API_API_KEY` | 空 | 遗留单 Key |
| `GROK2API_REQUIRE_API_KEY` | `auto` | `auto` / `1` / `0` |
| `GROK2API_DEFAULT_MODEL` | `grok-4.5` | 默认模型 |
| `GROK2API_ACCOUNT_MODE` | 空（UI 默认 round_robin） | 轮询策略 |
| `GROK2API_DATA_DIR` | `./data` | 运行时数据目录 |
| `GROK2API_AUTH_FILE` | `$DATA_DIR/auth.json` | 凭证路径 |
| `GROK2API_OPEN_BROWSER` | Linux 无头默认 `0` | 是否自动开浏览器 |
| `GROK2API_TIMEOUT` | `600` | 超时（秒） |
| `GROK2API_FORCE_STREAM` | `1` | 上游强制 stream |
| `GROK_CLI_CHAT_PROXY_BASE_URL` | `https://cli-chat-proxy.grok.com/v1` | 上游 |

### 粘性 / 维护 / 探测

| 变量 | 默认 | 说明 |
|------|------|------|
| `GROK2API_CONVERSATION_AFFINITY` | `1` | 对话粘性 |
| `GROK2API_AFFINITY_TTL` | `7200` | 粘性 TTL（秒） |
| `GROK2API_AFFINITY_MAX` | `5000` | 最大绑定数 |
| `GROK2API_TOKEN_MAINTAIN` | `1` | 后台刷新 Token |
| `GROK2API_TOKEN_MAINTAIN_INTERVAL` | `300` | 维护周期（秒） |
| `GROK2API_TOKEN_REFRESH_SKEW` | `120` | 过期前多少秒刷新 |
| `GROK2API_MODEL_HEALTH` | `1` | 后台模型探测 |
| `GROK2API_MODEL_HEALTH_INTERVAL` | `600` | 探测周期（秒） |
| `GROK2API_MODEL_HEALTH_AUTO_DISABLE` | `1` | 探测失败自动禁用 |
| `GROK2API_PROBE_MODELS` | 默认模型 | 探测模型列表 |

### 协议注册

| 变量 | 默认 | 说明 |
|------|------|------|
| `GROK2API_MOEMAIL_API_KEY` | 空 | MoeMail API Key |
| `GROK2API_MOEMAIL_BASE_URL` | `https://moemail.521884.xyz` | MoeMail 服务地址 |
| `GROK2API_MOEMAIL_DOMAIN` | `lolicc.online` | 临时邮箱域名 |
| `GROK2API_MOEMAIL_EXPIRY_MS` | `3600000` | 邮箱有效期 |
| `GROK2API_YESCAPTCHA_KEY` / `YESCAPTCHA_API_KEY` | 空 | YesCaptcha Key |
| `GROK2API_YESCAPTCHA_ENDPOINT` | 官方默认 | 打码接口 |
| `GROK2API_YESCAPTCHA_TIMEOUT` | `180` | 打码超时（秒） |
| `GROK2API_REG_MAX_COUNT` | `50` | 批量注册数量上限 |
| `GROK2API_REG_MAX_CONCURRENCY` | `10` | 并发上限 |
| `GROK2API_REG_CONCURRENCY` | `3` | 默认并发 |
| `GROK2API_XAI_PROXY` | 空 | 注册/上游可选代理 |

### 中继兼容

| 变量 | 默认 | 说明 |
|------|------|------|
| `GROK2API_REASONING_COMPAT` | `off` | `off` / `think_tag` / `content` |
| `GROK2API_SSE_KEEPALIVE` | `8` | 空闲 keepalive 秒数 |

---

## 管理 API 摘要

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/admin/api/status` | 公开状态 |
| POST | `/admin/api/setup` | 首次设密码 |
| POST | `/admin/api/login` | 登录拿 token |
| GET | `/admin/api/dashboard` | 仪表盘 |
| CRUD | `/admin/api/keys` | API Key |
| GET/POST/DELETE | `/admin/api/accounts*` | 账号 |
| POST | `/admin/api/accounts/login` | 设备码登录 |
| POST | `/admin/api/accounts/import` | 导入 JSON 体 |
| POST | `/admin/api/accounts/import-file` | 上传文件导入 |
| POST | `/admin/api/accounts/import-sso` | SSO cookie 导入 |
| GET | `/admin/api/accounts/export` | 导出 auth.json |
| POST | `/admin/api/accounts/delete-batch` | **批量删除账号** |
| PATCH | `/admin/api/accounts/{id}/enabled` | 启用/禁用 |
| POST | `/admin/api/accounts/{id}/probe` | 单账号探测 |
| POST | `/admin/api/accounts/probe-all` | 全部探测 |
| GET | `/admin/api/accounts/quota` | 全部额度 |
| POST | `/admin/api/accounts/register-email` | **协议注册（支持 count/concurrency）** |
| GET | `/admin/api/accounts/register-email/sessions` | 注册会话列表 |
| GET | `/admin/api/accounts/register-email/batches/{id}` | 批量注册进度 |
| PUT | `/admin/api/settings/account-mode` | 轮询策略 |

管理请求头：`X-Admin-Token: <token>`。

---

## 常见问题

### 401 / auth_error

- **客户端 401**：API Key 错误
- **上游 auth_error**：会话过期 → 重新设备码登录 / 导入 / 注册

### new-api 测速正常、操练场空白

通常是客户端默认带了 `presence_penalty` / `frequency_penalty`，上游拒绝。  
**v1.8.2+ 已自动剥离**这些参数；请确保运行的是新版本。

### 注册报 `wke=email:invalid-validation-code`

邮箱验证码失效/单次使用冲突。当前流程已改为 **先 Turnstile、后收码并立即建号**。若仍失败：

- 换 MoeMail 域名
- 降低并发
- 检查代理与打码成功率

### 多账号不轮询

确认账号未被禁用 / 额度禁用；token 未过期。管理台「查询额度」后可手动重新启用。

### Token 有效期

Session token 会过期。有 `refresh_token` 时后台会自动续期；否则重新登录/导入。

---

## 安全提示

- 默认只绑 `127.0.0.1`；公网务必设管理密码 + API Key，并配合防火墙 / reverse proxy
- `data/auth.json` 与 `data/keys.json` 含敏感信息，勿分享
- 管理台 token 存在浏览器 localStorage
- 协议注册仅限你有权操作的环境；请遵守 xAI / 邮箱服务条款

---

## 目录结构

```
grokcli-2api/
  app.py                 # 主服务 + 流式兼容 + failover
  admin_routes.py        # 管理 API
  grok_build_adapter.py  # 协议注册适配（多线程批量）
  moemail.py             # MoeMail / 代理工具
  grok-build-auth/       # 内置协议注册引擎（vendored）
  auth.py / auth_store.py
  accounts.py            # 设备码 / 导入 / 删除
  account_pool.py        # 轮询 / 冷却 / 统计
  oidc_auth.py           # 原生 OIDC 设备码 + refresh
  sso_to_auth_json.py    # SSO → auth.json
  token_maintainer.py    # 后台 Token 维护
  model_health.py        # 后台模型探测
  anthropic_compat.py    # Anthropic 协议转换
  apikeys.py / settings_store.py / models.py / config.py
  static/index.html      # 管理台 UI
  Dockerfile / docker-compose.yml / docker-rebuild.sh
  start.sh / start.ps1 / start.bat
  data/                  # 运行时数据（auth / keys / settings）
```

---

## 协议与免责

仅供个人学习与自用。请遵守 xAI / Grok、邮箱服务与打码服务的条款与用量限制。  
注册、批量操作等高风险能力请仅在你有权使用的环境中运行。
