# grokcli-2api

把 **Grok OIDC 登录态** 转成 **OpenAI / Anthropic 兼容 API**，并附带 Web 管理台：多 API Key、多账号轮询、设备码 / SSO / JSON 导入导出。

**当前版本：v1.9.92** · 移除协议注册 · 精简镜像依赖 · 保留 SSO/JSON 导入与号池能力

[![GHCR](https://img.shields.io/badge/ghcr.io-hm2899%2Fgrokcli--2api-blue)](https://github.com/users/HM2899/packages/container/package/grokcli-2api)
[![Release](https://img.shields.io/github/v/release/HM2899/grokcli-2api?display_name=tag)](https://github.com/HM2899/grokcli-2api/releases)

| 镜像（全小写） | 说明 |
|----------------|------|
| `ghcr.io/hm2899/grokcli-2api:1.9.92` | 当前版本 |
| `ghcr.io/hm2899/grokcli-2api:latest` | 最近 `v*` tag |
| `ghcr.io/hm2899/grokcli-2api:edge` | `main` 最新 |

- **独立运行**：不依赖本地 Grok CLI / 浏览器 OAuth
- **Hybrid 存储（默认强制）**：PostgreSQL 持久 + Redis 热状态 + 多 Worker
- **大账号池轮询**：`round_robin` / `least_used` / `random`；**没额度立即冷却踢出**；pick-time inflight 负载分散
- **会话粘性 / Prompt Cache**：`prompt_cache_key` / Claude session / messages hash；model 隔离绑定；TTL 可热改
- **中继友好**：兼容 new-api / sub2api / CLIProxyAPI / Claude Code / Codex；`Update`/`StrReplace` → `Edit`；**后到完整参数覆盖错误路径**
- **秒开流 + 可观测**：early SSE 信封；用量明细含 `ttft_ms` / `latency_ms` / **思考强度**；任务日志 + 终态帧

---

## 架构

```
客户端 (OpenAI / Anthropic SDK · new-api · Claude Code / sub2api)
        │  /v1/chat/completions  ·  /v1/responses  ·  /v1/messages
        ▼
  grokcli-2api  (FastAPI · multi-worker · TZ=Asia/Shanghai)
        │  管理台 /admin
        │  账号轮询 · 冷却/过期踢出 · inflight 分散 · Prompt Cache 会话粘性
        │  任务日志（SSO / JSON / 测活 / 续期）
        │  PostgreSQL（账号 / Key / 设置 / 冷却 / 任务日志）—— 容器内网
        │  Redis（粘性 / 计数 / 锁 / 会话 / 任务进度）—— 容器内网
        ▼
  cli-chat-proxy.grok.com
```

> `data/*.json` **仅作旧版迁移源与管理台导入导出**，运行时权威数据在 PostgreSQL / Redis，不再写本地 JSON 镜像。

---

## 功能一览

| 功能 | 说明 |
|------|------|
| OpenAI 兼容 | `/v1/models` · `/v1/chat/completions` · `/v1/responses` · SSE |
| Anthropic 兼容 | `/v1/messages` · tools / tool_use · `count_tokens` |
| Claude Code 工具 | Grok `Update`/`StrReplace` → 客户端 `Edit`；**后到完整参数覆盖错误路径（含 both-complete）**；`target_file` 等别名归一；残缺编辑不下发 |
| 管理台 | 账号、Key、测活、续期、任务日志、用量、**系统设置（维护/压缩/探测/sub2api · CLIProxyAPI）** |
| 多账号轮询 | `round_robin` / `least_used` / `random`；**pick-time inflight 分散**；可选**出站代理池** |
| 会话粘性 | `prompt_cache_key` / `previous_response_id` 粘同一账号；**TTL 可热改** |
| 冷却状态 | **没额度立即冷却踢出**（任意轮询策略）；live 硬排除；仅测活成功 / 手动解除才回池 |
| Token 过期 / 续期失败 | access token 过期立刻 `pool_status=expired` 移出轮询；连续 2 次 RT 失败 → 有 SSO 则重转，**无 SSO 则硬删账号** |
| 号池统计 | 总量 / 可轮询 / 冷却 / 过期 / 封禁 **互斥分类**（`pool_status` 权威） |
| Token 续期 | 后台 leader 维护；**维护间隔 / 提前刷新窗口可配置** |
| 模型探测 | 单账号 / 多选批量 / 全量；**探测模型列表 / 间隔 / 自动踢出可配置** |
| SSO / JSON / CPA | 后台任务 + 实时进度；JSON 多文件导入；**一键推送 sub2api**；**一键同步 CLIProxyAPI auth 目录**；CPA/auth 文件双向格式兼容 |
| 任务日志 | SSO、JSON、测活、续期等结果落 PG |
| 用量统计 | 代理侧 token / 请求：今日·近 N 天·累计；按 Key / 账号 / 模型；**首字 TTFT / 完成耗时 / 思考强度** |
| 流式可靠性 | early SSE 信封；**假阳性 client_gone 不再丢中间 tool/text 帧**；错误/断开仍发终态帧 |
| 容器时区 | 默认 `TZ=Asia/Shanghai`（日志与本地时间） |

---

## 本版本重点（v1.9.92）

| 能力 | 行为 |
|------|------|
| **移除协议注册** | 删除管理台协议注册入口、`register-email` API、`grok-build-auth` 引擎与内联过盾 |
| **精简运行时** | Docker 镜像不再内置 Turnstile/Camoufox；入口脚本仅启动 API |
| **账号导入保留** | 设备码、SSO Cookie、JSON / CLIProxyAPI 导入导出与号池轮询不变 |
| **兼容历史数据** | 账号上已有 `register_password` / SSO 元数据仍可展示与导出 |

继承 v1.9.91：CLIProxyAPI 同步 · 号池统计修正 · 无 SSO 续期失败硬删 · 历史压缩默认关。


## 快速开始

### 方式 A：Docker Compose（推荐）

```bash
git clone https://github.com/HM2899/grokcli-2api.git
cd grokcli-2api
cp .env.example .env
# 编辑 .env：至少改 GROK2API_ADMIN_PASSWORD；生产请改 Postgres 密码

docker compose up -d --build
curl -fsS http://127.0.0.1:3000/health
```

浏览器打开：`http://127.0.0.1:3000/admin`


**默认只映射应用端口 `3000`（内联部署）。**
栈内 **PostgreSQL / Redis** 都不绑定宿主机端口：

| 服务 | 容器内地址 | 是否映射到宿主机 |
|------|------------|------------------|
| app | `0.0.0.0:3000` | 是 → `127.0.0.1:3000` |
| postgres | `postgres:5432` | **否**（compose 内网） |
| redis | `redis:6379` | **否**（compose 内网） |

因此 compose 里应用环境变量应使用服务名，而不是 `127.0.0.1`：

```env
REDIS_URL=redis://redis:6379/0
DATABASE_URL=postgresql://grok2api:grok2api@postgres:5432/grok2api
```

> `.env.example` 中的 `127.0.0.1` 仅适用于「本机直接跑 Python、自己起 DB」的场景。
> `docker compose` 启动时会用 `docker-compose.yml` 中的服务名覆盖，无需改成宿主机端口。

若你**确实**需要从宿主机连库调试，可在本地 `docker-compose.override.yml` 临时加 `ports`（该文件已 gitignore，勿提交）。

### 方式 B：GHCR 镜像（注意小写）

Docker / GHCR **镜像名必须全小写**。仓库 owner 可能是 `HM2899`，但拉取时要用：

```text
ghcr.io/hm2899/grokcli-2api
```

**错误示例（会拉失败）：** `ghcr.io/HM2899/grokcli-2api`
**正确示例：**

```bash
docker pull ghcr.io/hm2899/grokcli-2api:1.9.92
# 或
docker pull ghcr.io/hm2899/grokcli-2api:latest
```

最小 compose 示例（内联 redis + postgres，**不要**给 DB 映射宿主机端口）：

```yaml
services:
  redis:
    image: redis:7-alpine
    # 不要 ports —— 仅容器网络内访问
    environment:
      TZ: Asia/Shanghai
    command: ["redis-server", "--save", "", "--appendonly", "no"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 10

  postgres:
    image: postgres:16-alpine
    environment:
      TZ: Asia/Shanghai
      PGTZ: Asia/Shanghai
      POSTGRES_USER: grok2api
      POSTGRES_PASSWORD: change-me
      POSTGRES_DB: grok2api
    volumes:
      - grok2api_pg:/var/lib/postgresql/data
    # 不要 ports —— 仅容器网络内访问
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U grok2api -d grok2api"]
      interval: 5s
      timeout: 5s
      retries: 10

  grokcli-2api:
    image: ghcr.io/hm2899/grokcli-2api:1.9.92
    ports:
      # 只映射应用；不要给 postgres/redis 加 ports
      - "3000:3000"
    environment:
      TZ: Asia/Shanghai
      GROK2API_HOST: "0.0.0.0"
      GROK2API_PORT: "3000"
      GROK2API_ADMIN_PASSWORD: "change-me"
      GROK2API_STORE_BACKEND: "hybrid"
      GROK2API_REQUIRE_SHARED_STORES: "1"
      GROK2API_WORKERS: "4"
      # 内联本地过盾（主容器 loopback，无需对外端口）
      GROK2API_CAPTCHA_PROVIDER: "local"
      GROK2API_INLINE_SOLVER: "1"
      REDIS_URL: "redis://redis:6379/0"
      DATABASE_URL: "postgresql://grok2api:change-me@postgres:5432/grok2api"
    volumes:
      - ./data:/app/data
    depends_on:
      redis:
        condition: service_healthy
      postgres:
        condition: service_healthy

volumes:
  grok2api_pg:
```

若包为 private，需先登录：

```bash
echo "$GITHUB_TOKEN" | docker login ghcr.io -u YOUR_GITHUB_USERNAME --password-stdin
```

### 必要环境变量

| 变量 | 说明 |
|------|------|
| `GROK2API_ADMIN_PASSWORD` | 管理台密码**首次种子**（无库内哈希时导入；之后以数据库为准） |
| `GROK2API_STORE_BACKEND=hybrid` | 生产模式 |
| `GROK2API_REQUIRE_SHARED_STORES=1` | Redis/PG 不可用则拒绝启动 |
| `REDIS_URL` | Compose 内：`redis://redis:6379/0` |
| `DATABASE_URL` | Compose 内：`postgresql://…@postgres:5432/…` |
| `GROK2API_WORKERS` | 建议 ≥2（按 CPU） |
| `TZ` | 容器时区，默认 `Asia/Shanghai` |
| `GROK2API_RELOAD` | 开发热更新：`1` 开启（强制单 worker）；生产保持 `0` |

完整模板见 [`.env.example`](./.env.example)。**生产请修改默认数据库密码。**

### 会话粘性（Prompt Cache）

多轮请求尽量固定同一 Grok 账号，避免池轮转打断缓存局部性。管理台「会话粘性」默认开启。

上游（Grok / cli-chat-proxy）的 prompt cache 是 **自动 prefix cache**：同一账号 + 相同 messages/tools 前缀 → usage 里出现 `prompt_tokens_details.cached_tokens`。本项目对齐 [superagent-ai/grok-cli](https://github.com/superagent-ai/grok-cli) 的做法，并吸收 [CLIProxyAPI (CPA)](https://github.com/router-for-me/CLIProxyAPI) 的 session-affinity 思路，**主动创造命中条件**：

1. **粘账号**（affinity：`prompt_cache_key` / conversation / response 链 / messages hash）
2. **按 model 隔离绑定**（同 session 换模型不会复用旧账号绑定）
3. **出站前缀稳定**（tools schema 规范化 + name 排序；messages 字段/参数 JSON 规范化；system 文本形态统一）
4. **历史压缩前缀稳定**（`HISTORY_PREFIX_STABLE`：旧 tool 结果确定性 placeholder，不反复改写）
5. **账号失效清绑定**（disable / quota 踢号时 `clear_affinity_for_account`）
6. **可观测**（响应字段 / header 回传 cache 命中量）

> 注意：CPA 对官方 `api.x.ai` 会透传 `prompt_cache_key` + `x-grok-conv-id`。本项目上游是 cookie SSO → cli-chat-proxy，**不认**这些字段——我们只把 `prompt_cache_key` 当 **本地粘性指纹**，出站前会剥掉。

| 客户端提示 | 行为 |
|------------|------|
| `prompt_cache_key`（body 或 `x-prompt-cache-key` header） | 作为稳定指纹；**不**再拼接 conversation root |
| Anthropic `cache_control` / metadata 缓存键 | 映射为粘性 key |
| Claude Code `metadata.user_id` 内嵌 `session_<uuid>` | 提取为 conversation id（CPA 同款） |
| Responses `previous_response_id` | 用上轮发出的 `response_id` 找回账号（不再误当 conversation_id） |
| 显式 `conversation_id` / `x-session-id` / `Session_id` / `x-amp-thread-id` / `x-client-request-id` | 最高优先 |
| 无任何 session 标识 | messages 内容 hash 兜底（首轮 short / 多轮 full） |

成功响应可观察：

| 字段 / Header | 含义 |
|---------------|------|
| `X-Grok2API-Affinity: 1` / `x_grok2api_affinity` | 本轮命中会话粘性 |
| `X-Grok2API-Affinity-Source` / `x_grok2api_affinity_source` | 粘性来源：`previous_response_id` / `prompt_cache_key` / `conversation_id` / `root` 等 |
| `x_grok2api_account` | 实际使用的账号（跨轮应一致） |
| `x_grok2api_cache_read_tokens` / `X-Grok2API-Cache-Read-Tokens` | 上游返回的 cache 读 token |
| `x_grok2api_cache_hit_ratio` / `X-Grok2API-Cache-Hit-Ratio` | `cached / prompt`（0–1） |
| `usage.prompt_tokens_details.cached_tokens` | 标准 usage 字段（OpenAI 兼容） |
| `X-Grok2API-Prompt-Stable: 1` | 本轮已做 tools/messages 出站稳定化 |

管理台 **用量** 页会汇总：

- **token 命中率** = `Σ cache_read_tokens / Σ prompt_tokens`
- **请求命中率** = 成功且 `cache_read_tokens > 0` 的请求占比
数据来自 `usage_events`（不是日汇总表），今日 / 近 N 天 / 累计三档。

历史压缩（`GROK2API_HISTORY_COMPACT=1`）开启时，默认 `GROK2API_HISTORY_PREFIX_STABLE=1`：旧 tool 结果用 **确定性 placeholder**（含内容 hash），后续轮次不再反复改写已压缩前缀，避免打断 prefix cache。

**客户端配合（提高命中率）：**

- 始终传稳定的 `prompt_cache_key`（或 Anthropic metadata / `x-prompt-cache-key`）
- 不要每轮改 system / tools schema
- 多轮用同一 API Key；观察 `X-Grok2API-Affinity: 1` 且账号字段跨轮不变
- 第二轮起看 `cached_tokens > 0`；若 affinity=1 仍为 0，则是上游未回 cache，不是粘性失败

### 本地开发热更新

生产默认 `reload=False` + 多 worker。改代码后要自动重启：

```bash
# 仅起 Redis/Postgres（若尚未运行）
docker compose up -d postgres redis

# 宿主机 Python 热更新（监听 .py / static/js / static/admin）
./dev.sh
# 或
GROK2API_RELOAD=1 GROK2API_WORKERS=1 python app.py
```

说明：
- `GROK2API_RELOAD=1` 时强制 **1 worker**（uvicorn 限制）
- 默认忽略 `data/`、`static/dist/`、`__pycache__/`，避免写库/打包触发无意义重启
- 管理台 `static/js` 源文件变更会触发进程重启；带 hash 的 `static/dist` 仍建议跑 `python scripts/build_admin_assets.py`
- Docker 镜像内一般不挂源码，热更新请用宿主机 `./dev.sh`，或 bind-mount 代码后再设 `GROK2API_RELOAD=1`

---

## 从旧版（JSON 文件）升级

详见 **[docs/UPGRADE.md](./docs/UPGRADE.md)**。

```bash
# 备份 data/ 后
chmod +x scripts/upgrade_from_file_backend.sh
./scripts/upgrade_from_file_backend.sh --data-dir ./data

# 或
docker compose up -d redis postgres
docker compose run --rm \
  -e DATABASE_URL=postgresql://grok2api:grok2api@postgres:5432/grok2api \
  grokcli-2api \
  python scripts/migrate_json_to_pg.py --data-dir /app/data --merge-pool
```

迁移内容：`auth.json` / `keys.json` / `settings.json`（含账号池状态）→ PostgreSQL。
不迁移：Redis 热状态、管理台登录会话。

已是 hybrid 时，拉新镜像即可；表结构由 `grok2api/store/pg.py` 启动时幂等升级。

---

## 客户端接入

### OpenAI 兼容

```bash
export OPENAI_BASE_URL=http://127.0.0.1:3000/v1
export OPENAI_API_KEY=你的管理台API_Key

curl "$OPENAI_BASE_URL/chat/completions" \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}]}'
```

### Anthropic 兼容

```bash
curl http://127.0.0.1:3000/v1/messages \
  -H "x-api-key: 你的管理台API_Key" \
  -H "anthropic-version: 2023-06-01" \
  -H "Content-Type: application/json" \
  -d '{"model":"grok-4.5","max_tokens":256,"messages":[{"role":"user","content":"hi"}]}'
```

Claude Code / Cursor / Cherry Studio：Base URL 填服务地址（通常带 `/v1`），Key 用管理台创建的 API Key。

---

## 管理台

| 页面 | 用途 |
|------|------|
| 概览 | 池规模、续期/探测状态、今日用量 |
| 账号 / 轮询 | 设备码、**SSO 导入（进度）**、**JSON 导入/导出（进度）**、测活、续期 |
| API Keys | 客户端密钥 |
| 用量 | Token / 请求：今日·近 N 天·累计；Key / 账号 / 模型；请求明细 |
| 任务日志 | SSO、JSON 导入导出、测活、Token 续期等后台任务结果 |
| 设置 | 轮询与冷却策略、账号导入与推送默认项等 |

### 账号导入 / 导出

| 方式 | 说明 |
|------|------|
| SSO Cookie | 粘贴或上传；后台 Device Flow 换 token，页面显示进度条与明细 |
| JSON 文件 | 支持多文件合并导入；解析 → 入库全程进度 |
| 导出全部 / 选中 | 后台打包，完成后自动下载；大池不阻塞页面 |
| **导入 sub2api** | 设置页配置 URL/登录/分组；账号页支持**选中/全部在线推送**；亦可导出官方 `sub2api-data` 备份供手动导入 |

导入导出、测活、续期等完成后，可在 **任务日志** 按类型 / 状态 / 关键词查询历史结果。


### 导入到 sub2api

1. **系统设置 → sub2api 导入**：填写 sub2api URL、管理员邮箱/密码，设置默认分组（ID 或名称；可不存在时自动创建）
2. 点 **测试连接** / **刷新分组** 确认可达
3. **账号 / 轮询** 页：
   - 勾选账号 → **导入 sub2api（选中）**
   - 或 **导入 sub2api（全部）**
   - **导出 sub2api 数据**：下载官方 `type=sub2api-data` 备份 JSON（含 token），可在 sub2api「导入数据」直接上传
4. 推送优先使用本地 access/refresh token 创建 `platform=grok` + `type=oauth` 账号；无 token 时回退 SSO→OAuth

API：
- `PUT /admin/api/settings/sub2api`
- `POST /admin/api/settings/sub2api/test`
- `GET  /admin/api/settings/sub2api/groups`
- `POST /admin/api/accounts/push-sub2api`  body: `{ "account_ids": ["..."] }` 或 `{ "all": true }`
- `POST /admin/api/accounts/export-sub2api-format`


## 运维

```bash
curl -fsS http://127.0.0.1:3000/health
curl -fsS http://127.0.0.1:3000/metrics | head
docker compose logs -f grokcli-2api
# 时区
docker exec grokcli-2api sh -c 'echo TZ=$TZ; date'
```

- 仅 **leader** worker 跑 Token 续期与模型健康任务（Redis 选主）
- 备份重点：**PostgreSQL 卷**（`grok2api_pg`）；Redis 可丢
- 本地低停机重建：`./docker-rebuild.sh`
- Postgres / Redis **默认不暴露宿主机端口**
- 任务日志表 `task_logs` 在 hybrid 启动时幂等创建
- 默认时区 **Asia/Shanghai**（`TZ` / Dockerfile `tzdata`）

### 发布镜像（GHCR）

```bash
# 1) grok2api/app.py 中 APP_VERSION 必须与 git tag 一致（镜像路径全小写）
# 2) 推 main → edge + 版本号；推 v* tag → 额外 latest + GitHub Release
git add -A && git commit -m "release: v1.9.92 remove protocol registration"
git push origin main
git tag -a v1.9.92 -m "v1.9.92 remove protocol registration"
git push origin v1.9.92
gh release create v1.9.92 --title "v1.9.92 移除协议注册，精简镜像与管理台" --notes-file - <<'EOF'
## Highlights
- 移除协议注册：管理台入口、register-email API、grok-build-auth、内联过盾
- Docker / 启动脚本精简，仅保留 API + hybrid 存储依赖
- 保留设备码 / SSO / JSON / CLIProxyAPI 导入导出与号池轮询
- README 更新为当前能力描述
EOF
# 监视构建
gh run list --workflow=docker-publish.yml --limit 3
```

成功后拉取（**必须小写**）：

```bash
docker pull ghcr.io/hm2899/grokcli-2api:1.9.92
docker pull ghcr.io/hm2899/grokcli-2api:latest
```

CI 会把 `github.repository` 强制转成小写后再推送，避免 `HM2899` 大小写导致 `docker pull` 失败。
`docker-publish.yml` 在 tag 推送时还会校验 `v*` 与 `APP_VERSION` 一致。

**不要提交**：`.env`、`data/`、`docker-compose.override.yml`、`docker-rebuild.local.sh`、密钥与本地 SSO 备份。

---

## 目录提示

```
# 运行时入口
app.py                                   # 兼容启动包装（真实应用在 grok2api/app.py）
grok2api/app.py                          # FastAPI 应用、OpenAI/Anthropic 路由入口

# 业务包（新代码请从这里 import）
grok2api/admin/                          # 管理台路由、API Key、设置、任务日志、用量统计
grok2api/pool/                           # 账号池、认证、配额、模型健康、token 维护、会话粘性
grok2api/protocol/                       # Claude / Responses / OpenAI 协议兼容与历史压缩
grok2api/upstream/                       # Grok 上游、OIDC、sub2api、CLIProxyAPI、代理、模型目录
grok2api/store/                          # Redis + PostgreSQL 适配（PG 表、Redis 热状态、锁、指标）

# 兼容层（旧导入仍可用）
account_pool.py / admin_routes.py / ...  # root shim → grok2api.*
store/                                   # root store wrapper → grok2api/store
migrate_json_to_pg.py                    # 兼容包装；推荐 scripts/migrate_json_to_pg.py
sso_to_auth_json.py                      # 兼容包装；推荐 scripts/sso_to_auth_json.py

# 脚本（见 scripts/README.md）
scripts/build_admin_assets.py            # 管理台静态资源打包
scripts/upgrade_from_file_backend.sh     # file → hybrid 升级
scripts/migrate_json_to_pg.py            # JSON → PG 迁移
scripts/_test_*.py                       # 本地回归（粘性 / 冷却 / 踢出 / tool parse / 终态帧）

# 前端 / 部署
static/                                  # 管理台前端
docs/UPGRADE.md                          # 升级说明
docker-compose.yml                       # redis + postgres（内网）+ app
.github/workflows/docker-publish.yml     # GHCR 多架构（小写镜像名）
```

---

## 安全与免责

- 勿将 `.env`、`data/`、真实 Token / SSO 备份提交到 Git（`data/` 下本地备份已 gitignore）
- 生产务必修改 Postgres 密码与管理员密码
- 默认不映射 DB/Redis 端口；调试用本地 override，勿对公网暴露
- 导出 JSON / SSO 含完整凭证，请妥善保管

---

## 版本

- **v1.9.92**（当前）
  - **移除协议注册**：删除管理台协议注册、相关 API、`grok-build-auth` 与 `turnstile-solver`
  - **精简部署**：Dockerfile / entrypoint 不再内置过盾浏览器；启动脚本去掉注册引擎路径
  - **保留导入能力**：设备码、SSO Cookie、JSON / CLIProxyAPI 导入导出、号池轮询与测活续期不变
  - README 同步更新为当前产品描述
- **v1.9.91**
  - CLIProxyAPI 号池同步 · 号池统计修正 · 无 SSO 续期失败硬删 · 历史压缩默认关
- **v1.9.89**
  - **使用明细 · 思考强度**：管理台「使用明细」直接展示英文标签 low / medium / high / xhigh
  - 从 OpenAI `reasoning_effort`、Anthropic `thinking`/`budget_tokens`、Responses `reasoning.effort` 提取并写入 usage detail
  - 列表接口透出 `reasoning_effort`；点击行可看完整字段
  - 继承 v1.9.87：Token 过期移出轮询 · SSO 续期自愈 · 首页状态统计
- **v1.9.87**
  - **Token 过期移出轮询**：access token 过期 / 续期失败立刻 `pool_status=expired`，请求轮询硬排除；凭证保留
  - **连续 2 次 RT 续期失败**：第 1 次仅软过期；第 2 次有 SSO 则 `sso_to_auth_json` 重转回池；无 SSO 则移出号池
  - **续期成功回池**：清失败计数；此前因无 SSO 被踢也会自动 re-enable
  - **首页状态统计**：冷却 / 过期 / 禁用 / 轮询中直接数 `account_pool.pool_status` 等字段，不再用墙钟推算
  - 管理台账号列表「过期」标签 + 概览过期数量
  - 继承 v1.9.86：没额度直接冷却踢出
- **v1.9.86**
  - **没额度直接冷却踢出**：free-usage / 额度耗尽 / 429 rate-limit 命中后立即 `pool_status=cooldown` 踢出 live 轮询（与 round_robin/least_used/random 无关）
  - 清 affinity、放 inflight、软封模型；恢复仅测活成功或管理员解除
  - 识别范围扩大（中文额度/配额、quota exceeded、rate limit 等）
  - 回归：`scripts/_test_free_usage_hard_kick.py`
  - 继承 v1.9.85：轮询负载分散
- **v1.9.85**
  - **轮询负载分散**：pick 时 soft-mark（in-flight + soft last_used），并发请求不再扎堆同一账号
  - Redis multi-worker 共享 inflight；file/单进程有本地 fallback；success/failure 释放；TTL 防泄漏
  - `least_used` / `random` / `round_robin` 排序均考虑 inflight；failover 切号时补 mark
  - 回归：`scripts/_test_rotation_load_spread.py`
  - 继承 v1.9.84：冷却池严格排除轮询
- **v1.9.84**
  - **冷却池严格排除轮询**：free-usage 用完 / 429 / empty_upstream 进入冷却后，不再被 soft recovery 拉回 live 链
  - 粘账号冷却时也不再注入链尾；全池冷却时报 `Cooling=N`
  - 回归：`scripts/_test_strict_cooldown_rotation.py`
- **v1.9.83**
  - **CPA 风格会话粘性增强**：model 隔离绑定、Claude Code `session_<uuid>` 提取、messages hash 兜底、账号失效清绑定
  - 回归：`scripts/_test_cpa_affinity_improvements.py`
- **v1.9.82**
  - **Update both-complete 路径纠偏（Claude Code → sub2api）**：两边都是完整 Update/Edit 时，**后到完整参数为准**（`path` 可覆盖错误 `file_path`）；不完整后到仍不能覆盖完整先到
  - doubled JSON / 流式 merge 同步；`normalize` 不同路径值时 **later wins**；同值保留 `file_path` 拼写
  - OpenAI Responses 本地 merge 镜像同步；回归覆盖 both-complete / doubled both-complete
  - **注册成功后自动入库 sub2api**（设置项 `auto_push_on_register`）：协议注册导入本地后立刻 `push_account`；失败不拖垮注册
  - 继承 v1.9.81：注册 SSO 入库与备份、系统设置扩展、sub2api 账号容量、进度卡防连环 toast
- **v1.9.81**
  - **Update 偶发失效修复（Claude Code → sub2api）**：后到**完整** Update/Edit 参数覆盖先到的错误 `file_path` 预览；完整包不被不完整 path 别名覆盖；`target_file` 等别名归一
  - OpenAI Responses 本地 merge 镜像同步；回归测试覆盖 doubled JSON / 流式 merge 两种顺序
  - **注册账号持久化 SSO**：导入时写入 `sso`/`sso_cookie` + 密码；`data/register_sso/` 文件备份；账号列表显示 SSO 标记
  - **导出 SSO 可走账号库**：注册会话过期 / 进程重启后仍可从账号 payload 导出
  - **系统设置扩展（热更新）**：Token 维护间隔 / 提前刷新窗口、模型健康间隔 / 自动踢出、探测模型列表、会话粘性 TTL、历史压缩阈值/轮数/tool 结果上限、OpenAI 每轮工具上限
  - **sub2api 推送账号容量**：`account_concurrency` / `priority` / `rate_multiplier` 可配置（在线推送 + 导出格式）
  - **注册进度 UI**：已结束任务刷新不再连环 toast；不再伪造 `running` 占位；活跃批次过滤更严
  - 继承 v1.9.80：注册日志秒级刷新、Watchdog、device-flow 重试
- **v1.9.80**
  - **Update 读文件纠偏（Claude Code → sub2api）**：`file_path` 规范键优先于 `path`/`filepath`/`file` 别名；流式 merge 先保留原始 key 再归一
  - OpenAI Responses / Anthropic 出口同步；`Update`/`StrReplace` 仍 remap 为 `Edit`
  - **注册进度日志秒级刷新**：多 worker 按 Redis `updated_at` 合并最新 session/batch；前端 trailing-edge 轮询
  - 等邮件阶段 `on_tick` 心跳；大批量注册过盾心跳续期
  - 注册批次/会话 Redis TTL 默认 **72h**；**运行中 Watchdog** reclaim + auto-resume
  - MoeMail 创建 429/5xx 重试；device-flow 默认重试 6 次
  - 继承 v1.9.79：启动自愈、全局 inflight、resume/reclaim API
- **v1.9.79**
  - **注册机自愈**：进程重启/发版后自动回收卡在 `solving_turnstile` 等状态的孤儿会话，并 resume 未完成批次
  - **全局并发保护**：`GROK2API_REG_GLOBAL_INFLIGHT` 限制跨批次同时注册数，避免多批×多线程冲垮本地过盾
  - 本地 captcha resume 并发默认 3；过盾超时 / device-flow 失败时 soft-pause
  - API：`POST .../batches/{id}/resume`、`POST .../register-email/reclaim`
  - 继承 v1.9.78：导出注册 SSO
- **v1.9.78**
  - **导出注册 SSO**：管理台 + API 从注册会话导出 cookie（`sso` / `sso=…` / 邮箱+SSO / 邮箱:密码:SSO / JSON）
  - `GET|POST /admin/api/accounts/register-email/export-sso`，支持 batch / status 过滤与下载
  - 继承 v1.9.77：device-flow 限流重试与节流
- **v1.9.77**
  - **注册机 device-flow 限流**：并发换 token 时 xAI `429 slow_down` / `rate_limited` 自动退避重试
  - 全局 device-flow 最小间隔（默认 1.2s，`GROK2API_SSO_DEVICE_GAP_SEC`）
  - 失败文案标明常见原因是并发 rate limit（SSO 已拿到后的转换阶段）
  - 继承 v1.9.76：Update→Edit、防假断流、Codex 思考链不泄漏
- **v1.9.76**
  - **Codex 思考链不泄漏**：停止把 `reasoning` 当 `output_text`（修 v1.9.73 Codex 加速副作用）
  - 空响应不再用思考链充数；走 empty_complete / failover
  - 继承 v1.9.75：假断流修复、Update→Edit、本地过盾 Proxyless
- **v1.9.75**
  - **假阳性 client_gone 不再丢中间帧**（Responses / chat / Anthropic body 始终下发）
  - 断开探测更严：`DISCONNECT_HITS` 默认 5、`SPAN` 2.5s
  - 继承 v1.9.74：Update→Edit 全路径 remap
- **v1.9.74**
  - **Claude Code Update→Edit**：Grok 发明的 `Update`/`StrReplace` 出站统一映射为 `Edit`
  - 参数别名归一化（`path`/`oldString` → `file_path`/`old_string`）；残缺 Update 不下发
  - OpenAI chat / Responses / Anthropic 全路径 + 终端 force-close 覆盖
  - 本地 Camoufox 过盾仅 Proxyless（跳过 YesCaptcha M1 误报）
- **v1.9.73**
  - **用量明细首字/完成时间**：`ttft_ms` + `latency_ms` 落库与管理台展示
  - **Codex 加速**：原生 Responses 多工具/零 gap、大上下文自动压缩、`previous_response_id` sticky 恢复同一 prompt_cache_key
  - **断联加固**：修复 warmup 污染 AsyncClient 导致的 `Event loop is closed`；本地 infra 错误不冷却账号
  - **Update/Edit 修复**：允许 `new_string=""` 删除；不完整 tool 不再空 `{}` 上线
  - **任务状态收尾**：`client_gone` 仍发 content_block_stop + message_delta/stop；terminal_error 补 stop_reason
  - 继承 v1.9.72：early SSE 信封、TTFT 诊断
- **v1.9.72**
  - **early SSE 信封**：上游 HTTP 200 后立即发 `message_start` / `response.created` / role chunk，恢复前几版“秒开流”体感；empty-200 改为干净终态错误（不再静默切号）
  - **TTFT 诊断增强**：日志增加 `early` / `tools` / `held` / `env`，区分信封打开 vs 首 token vs 工具前言 hold
  - 继承 v1.9.71：测活才解冷却、自动 prompt_cache_key、sub2api 终态帧
- **v1.9.71**
  - **严格冷却恢复**：请求失败进入冷却后不再按时间自动恢复，仅测活成功或管理台手动解除才回池
  - 继承 v1.9.70：自动 prompt_cache_key、sub2api 终态帧
- **v1.9.70**
  - **自动 prompt_cache_key**：客户端未传时按 conversation / previous_response_id / session 生成稳定 key，并在响应 body/header 回传（`prompt_cache_key` / `X-Grok2API-Prompt-Cache-Key`）
  - 响应链绑定保存 minted key，仅带 `previous_response_id` 的下一轮也能恢复同一 sticky key
  - 继承 v1.9.69：sub2api 终态帧修复、空 200 冷却降敏
- **v1.9.69**
  - **sub2api 终态帧修复**：`ResponsesLiveStreamer.complete()` 空结果不再 `_closed`，保证后续 `response.failed` + `[DONE]` 可发出，消除 sub2api `missing terminal event` / `upstream stream ended without terminal event`
  - **空 200 冷却降敏**：empty model output 只短冷却 8–20s，避免号池被打空后 sub2api 报 `no available accounts`
  - 推荐 sub2api 上游用 Docker 内网 `http://grokcli-2api:40081/v1`（避免重启瞬间公网 IP connection refused）
  - 继承 v1.9.68：断联 usage 明细补全
- **v1.9.68**
  - **断联明细补全**：`/v1/responses` / chat / Anthropic 失败路径写入真实 `error` + `detail.message`（上游 status/body、空 200、代理异常、全号失败），不再落成裸 `request_failed` + `{}`
  - 失败 usage 行补 `latency_ms`；`_record_usage_safe` 对 `!ok` 强制补 status/message，方便管理台「断联」排查
  - 继承 v1.9.67：模型列表入库、续期永久失败硬删除、断联防抖
- **v1.9.67**
  - **模型列表入库**：`GET /v1/models` 只读 PostgreSQL `models` 表；管理台「同步上游模型」从 cli-chat-proxy 拉取并写入数据库（不再读写 `models_cache.json`）
  - 启动时若表为空，自动灌入默认模型 + 本地 extras；`migrate_json_to_pg.py` 仍可一次性导入旧 `models_cache.json`
  - **运行时不再写本地 JSON 镜像**：hybrid 下账号 / Key / 设置只落 PostgreSQL；会话粘性只走 Redis；`data/*.json` 仅迁移与管理台导入导出
- **v1.9.66**
  - **续期永久失败硬删除**：`invalid_grant` / refresh token revoked 默认直接踢出号池并删除账号（`GROK2API_DELETE_INVALID_REFRESH=1`）
  - 启动与维护周期 purge 清掉已标记的 dead RT；设 `=0` 才回退 soft-disable
- **v1.9.65**
  - **断联防抖**：`is_disconnected` 需连续命中（默认 2，`GROK2API_DISCONNECT_HITS`）才判定 client_gone，避免背压单次 blip 硬切
  - **stream_started 后置**：仅在真正 yield 出站帧后锁定账号/禁止静默切号，假断联不再阻断 failover
  - 继承 v1.9.64：软断开终态帧、工具参数别名/Update 合并、xhigh thinking
- **v1.9.64**
  - **偶发流中断修复**：OpenAI / Anthropic / Responses 软断开不再硬切 SSE；`is_disconnected` 探活异常不再误判 `client_gone`
  - 已开流时始终发出终态帧（finish/`[DONE]`、`message_delta`/`message_stop`、`response.completed`/`failed`），避免 sub2api/Claude Code 停调度
  - **工具参数加固**：Update 双 JSON 合并取更完整对象；`path`/`oldString` 等别名归一为 Claude Code schema；schema 不完整工具不刷出
  - OpenAI chat 默认不限多工具（`GROK2API_OUTBOUND_MAX_TOOLS_OPENAI=0`）；Claude/sub2api 路径仍默认单工具
  - xhigh thinking / `budget_tokens` 映射到 `reasoning_effort=xhigh`
- **v1.9.63**
  - **注册进度 404 停轮询**：浏览器缓存的过期 `batch_*` / `gba_*` 在后端不存在时清理 track 并停止轮询，避免控制台刷 404
  - 停止按钮对已消失 batch/session 做 not-found 降级
- **v1.9.62**
  - **tool_choice 空 200 修复**：sub2api/Claude Code 强制工具时的 `{"type":"function","name":…}` / nested function 形态映射为 `"required"`，避免 cli-chat-proxy 空 body 导致前端 empty envelope
  - 覆盖 Anthropic `tool`/`any` tool_choice 变体
- **v1.9.61**
  - **Responses 失败流修复**：`response.failed` 前必发 `response.created`/`in_progress`；中途失败沿用单调 `sequence_number`（不再回绕到 0）
  - 修复 Claude Code 将 bare/回绕的 failed SSE 误判为 `empty or malformed response (HTTP 200)`
- **v1.9.60**
  - **本地过盾就绪门闩**：注册在本地过盾模式下等待 solver HTTP 就绪后再开跑
  - **Responses 协议修复**：`sequence_number` 单调且 `response.created` 永远先发；`response.completed` 复用流中 item id（不再重新生成 msg_/fc_）
  - 修复 Claude Code / sub2api 将乱序 SSE / id 不一致误判为 `empty or malformed response (HTTP 200)`
- **v1.9.58**
  - **工具参数必填键 hold**：Responses 路径在 `anthropic_compat` 导入失败时仍按 `Read.file_path` / `Bash.command` 等本地规则 hold，避免半成品 tool 提前开 `response.created` 导致 Claude Code 报 empty/malformed HTTP 200
  - 回归测试覆盖 local fallback + 网关拦截 body 分类
- **v1.9.57**
  - **空 200 流式切号**：Anthropic `message_start` / Responses `response.created` 延后到真实模型输出后才开流，空 body / 网关拦截页可静默切号
  - OpenAI chat 流仅在真正发出 content/tool 帧后才锁定账号（忽略不完整 tool 预览）
- **v1.9.56**
  - 本地部署默认时区与 prompt-cache 粘性加固后的版本号 bump
- **v1.9.55**
  - **Prompt Cache 会话粘性**：`prompt_cache_key` 单独指纹（不 fold root）；Responses `response_id` 链绑定 `previous_response_id`
  - chat / messages / responses 统一 `api_key_id` 命名空间；流式/非流式均 bind
  - **默认容器时区** `Asia/Shanghai`（Dockerfile + compose + `.env.example`）
  - 合并待发布：额度冷却自动恢复、出站/注册代理池、大池测活优先扫、空 200 故障切换等（1.9.50–1.9.54）
- **v1.9.54**：free-usage 冷却时间窗（15m→1h）到期回池；billing 恢复自动解禁
- **v1.9.53**：空 200 / 网关拦截页可重试切号
- **v1.9.52**：账号池出站代理池（聊天/测活/续期粘性选代理）
- **v1.9.51**：协议注册代理池多行 + 策略 + 抽测
- **v1.9.50**：大池测活优先扫冷却/未知；限流可复检
- **v1.9.49**：注册任务日志 + 进度硬刷新恢复
- **v1.9.48**：注册进度恢复；Turnstile 空闲回收加固
- **v1.9.47**：Turnstile 懒加载 + 空闲回收；默认 workers=2
- **v1.9.46**：Cloudflare Temp Email；续期软禁用；流式加固
- **v1.9.45–1.9.38**：YYDS 域名、任务日志、JSON/SSO 进度、内联 hybrid 等
- 更早变更见 [GitHub Releases](https://github.com/HM2899/grokcli-2api/releases)

> 镜像 tag 与 `grok2api/app.py` 中 `APP_VERSION` 一致（当前 **1.9.92**）。
> 拉取路径固定 **`ghcr.io/hm2899/grokcli-2api`**（全小写）。

## License

见 [LICENSE](./LICENSE)。
