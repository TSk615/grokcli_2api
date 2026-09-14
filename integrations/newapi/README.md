# Grok Web Video 插件

上传文件：`grok-web-video.js`（当前版本 1.1.0；单个文件，不需要上传本说明）。

针对现有 NewAPI `v1.0.0-rc.33`（构建 `eb99ab1`）的 Task Plugin API v1，
只声明 `openai_video`，不覆盖 Sora、不占用 OpenAI 渠道类型。

## 手动安装

1. 任务插件 → 上传 → 选择 JS 文件，检查源码后启用。
2. 新建独立的「任务插件」渠道，选择 `Grok Web Video`。
3. Base URL 填 Grok 网关地址，例如 `https://grokapi.rainflow.foo`，不加 `/v1`。
4. 渠道密钥填 Grok 网关 API Key；调用客户端用的是 NewAPI 令牌，二者不要混淆。
5. 模型填写 `Web/grok-imagine-video`。从旧 OpenAI/Sora 渠道移除该模型，避免仍被分流过去。
6. 配置此模型计费（按次价格或按 seconds/resolution 的表达式）、渠道分组与令牌权限。
7. 如上传提示协议模型绑定冲突，先检查其他插件是否显式声明该模型，不要直接强制覆盖。

本插件未自动上传、注册或启用，也未改动现有 NewAPI 渠道和价格。

## 请求参数

客户端：`POST /v1/videos`，JSON 或 multipart；查询与下载由 NewAPI 管理：
`GET /v1/videos/:id`、`GET /v1/videos/:id/content`。

- `prompt`：必填。
- `seconds` 或 `duration`：默认 6；为适配当前免费账号，超过 6 会截到 6。
- `resolution`：默认 `480p`，也可传 `720p`（上游是否可用取决于账号和额度）。
- `quality`：可用 `standard`/`high`，分别对应 480p/720p；与 resolution 冲突会报错。
- `aspect_ratio`：默认 `1:1`，支持 `16:9`、`9:16`、`4:3`、`3:4`、`3:2`、`2:3`。
- `size`：可用任意有效的 `WIDTHxHEIGHT`，例如画布常见的 `854x480`。
  明确传入 `aspect_ratio` 时以它为准；否则将尺寸映射到最接近的受支持比例。
  `size` 只参与比例转换，不保证输出像素。
- `mode=first_frame`：上传一个 `first_frame`。
- `mode=last_frame`：上传一个 `last_frame`。
- `mode=loop`：上传一个 `first_frame`，或同时上传 `first_frame` 与 `last_frame`。
- `mode=reference`：用重复的 `image[]` 上传 1–9 张参考图。
- 图片支持 PNG/JPEG/WebP，每张最大 20 MiB；不支持远程图片 URL、视频延长和 remix。

```powershell
curl.exe "https://newapi.rainflow.foo/v1/videos" -H "Authorization: Bearer YOUR_NEWAPI_TOKEN" -F "model=Web/grok-imagine-video" -F "prompt=让图中物体轻轻运动" -F "input_reference=@C:\Pictures\source.png" -F "resolution=480p" -F "seconds=6" -F "aspect_ratio=16:9"
```

不传图片就是文生视频；上传失败或图片参数不合法会明确失败，不会忽略原图。
当前默认比例不自动读取图片尺寸；上游会处理比例变换，可能裁切或调整构图。

## 同步生成和下载

Grok 后端同步等待生成完成，插件将成功结果作为立即完成的 NewAPI 任务保存，
不伪造异步轮询、不在查询时重复生成。提交可能需要数十秒至数分钟，请为客户端、
NewAPI、Nginx 配置足够的超时；插件不能绕过 Cloudflare 的连接超时限制。
如中途超时，先检查后端是否已完成，避免立刻重复提交造成重复消耗。

视频内容通过 NewAPI 的认证下载接口访问。插件只允许读取本渠道网关的内容寻址
`/v1/media/videos/<hash>.mp4` 文件，不将密钥发到上游返回的任意地址，不保存图片
Base64 或密钥到任务结果。当前版本的 NewAPI 可能不在任务 JSON 中显示视频直链，
请使用其 `/content` 接口。
