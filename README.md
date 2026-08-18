# AstrBot NapCat Monitor

面向 AstrBot `aiocqhttp`（OneBot v11）适配器的 NapCat 可用性监控插件。插件会主动调用每个适配器实例的 `get_status`，在连续失败、恢复在线时发送飞书告警。

## 为什么使用主动探测

AstrBot 当前的 `aiocqhttp` 适配器只把消息、通知和请求转换为插件事件，不会把 OneBot `meta_event` 心跳交给普通 AstrBot 插件。因此，本插件不再假装“监听心跳”，而是通过适配器公开的客户端执行真实 API 探测：

1. 枚举 AstrBot 已加载的全部 `aiocqhttp` 平台实例；
2. 并发调用 `platform.get_client().api.call_action("get_status")`；
3. 兼容 OneBot 标准响应包和直接数据响应；
4. 连续失败达到阈值后判定离线；
5. 离线后再次探测成功则判定恢复。

这种方式能够发现 WebSocket 断开、NapCat 进程不可用、OneBot API 超时以及明确返回离线状态等问题。

## 功能

- 支持同一 AstrBot 中的多个 `aiocqhttp` 实例，重复实例 ID 会自动生成稳定的内部后缀键；
- 启动后立即探测，之后按配置周期执行；
- 已监控实例从平台管理器消失时同样累计失败，避免陈旧状态永久显示在线；
- 使用连续失败阈值过滤瞬时网络抖动；
- 首次离线、在线转离线、离线恢复在线通知；
- 飞书 Webhook、签名、超时、重试与业务响应严格校验；
- HTTP 会话复用，插件卸载时完整释放；
- 告警按“平台实例 + 事件类型”独立冷却，同键去重、不同实例并发发送；
- 只有通知发送成功后才写入冷却时间；
- 未配置飞书时仍保留本地状态监控与查询命令；
- 提供手动探测和多实例状态查询命令。

## 安装

将插件目录放入 AstrBot 的插件目录，并在管理面板中加载或重载插件。依赖由 `requirements.txt` 声明：

```text
aiohttp>=3.8.0
```

本插件要求 AstrBot 中已启用至少一个 `aiocqhttp` 平台实例，并由该实例连接 NapCat 的 OneBot v11 反向 WebSocket。

## 配置

| 配置项 | 默认值 | 说明 |
| --- | ---: | --- |
| `feishu_webhook` | 空 | 飞书群机器人 Webhook；留空不影响本地探测 |
| `feishu_secret` | 空 | 飞书机器人签名密钥 |
| `check_interval` | `30` | 主动探测周期，单位秒 |
| `probe_timeout` | `10` | 单次 OneBot API 调用超时，单位秒 |
| `failure_threshold` | `2` | 连续失败达到该次数后判定离线 |
| `alert_cooldown` | `300` | 同实例同类通知冷却时间，单位秒 |
| `alert_at_all` | `true` | 离线通知是否 `@所有人` |
| `alert_on_initial_failure` | `true` | 启动后首次确认离线是否通知 |
| `bot_alias` | `AstrBot-NapCat` | 飞书卡片中的来源名称 |
| `enable_startup_notice` | `true` | 插件启动时是否通知 |
| `enable_recovery_notice` | `true` | 离线恢复后是否通知 |
| `notify_timeout` | `10` | 飞书请求超时，单位秒 |
| `notify_retries` | `2` | 飞书发送失败后的重试次数 |

建议让 `check_interval × failure_threshold` 符合可接受的故障发现时间。例如默认配置通常会在约 30～60 秒的连续故障后确认离线，实际时间还会受单次探测超时影响。

## 命令

| 命令 | 作用 |
| --- | --- |
| `/napcat_status` | 查看所有已探测实例的在线状态、最后成功时间、连续失败次数和最后错误 |
| `/napcat_probe` | 立即对全部 `aiocqhttp` 实例执行一轮探测 |
| `/napcat_test` | 测试飞书通知链路 |
| `/napcat_reset` | 清空告警冷却计时 |

## 状态判定

- `get_status` 成功并返回 `online=true` 或 `good=true`：在线；
- OneBot 返回失败状态或非零 `retcode`：本轮失败；
- API 调用异常或超过 `probe_timeout`：本轮失败；
- 连续失败未达到 `failure_threshold`：保留原在线状态，并在状态命令中显示“异常待确认”；
- 已监控的平台实例从 AstrBot 平台管理器中消失：按一次探测失败处理；
- 连续失败达到阈值：离线；
- 离线后任意一轮成功：恢复在线，并清零失败次数。

`get_login_info` 只用于补充显示 QQ 账号，并每 300 秒刷新一次以识别重新登录或换号。即使该接口失败，只要 `get_status` 成功，实例仍会被判定为在线，并保留上次成功读取的账号。

## 从 1.x 升级

2.0.0 是破坏性重写：

- 删除无效的 `heartbeat_timeout` 配置；
- 新增 `probe_timeout`、`failure_threshold` 和 `alert_on_initial_failure`；
- 监控依据从伪心跳事件改为 OneBot API 主动探测；
- 状态键从 QQ `self_id` 改为 AstrBot 平台实例 ID，多个实例不会互相覆盖；
- 命令统一为 `/napcat_status`、`/napcat_probe`、`/napcat_test`、`/napcat_reset`。

升级后请打开插件配置页，确认新配置项并保存一次。回滚时可恢复 1.x 代码与原配置，但不建议继续使用无法接收心跳事件的旧实现。

## 本地验证

在插件目录执行：

```powershell
python -m pytest -q
python -m py_compile main.py test_main.py
```

测试覆盖多实例发现、响应解析、成功探测、瞬时失败、连续失败、首次离线、恢复通知、登录信息降级与定时刷新、通知失败重试、冷却写入时机、实例间冷却隔离、新事故冷却清理、无 Webhook 启动、手动探测命令、生产探测链路的同步/异步动作兼容、飞书成功/业务错误/空响应/非法 JSON/错误结构、同键并发去重、不同实例并发通知、签名和本地 aiohttp 完整往返。