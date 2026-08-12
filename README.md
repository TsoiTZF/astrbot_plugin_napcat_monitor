# astrbot_plugin_napcat_monitor

NapCat 掉线监控 + 飞书告警插件。监听 AstrBot 接入的 OneBot（NapCat / Lagrange / go-cqhttp 等）心跳事件，发现断连、重连、异常时实时推送到飞书群机器人。

## 功能特性

- ✅ **心跳监控**：基于 OneBot `meta_event.heartbeat` 实时跟踪在线状态
- ✅ **掉线告警**：超过 `heartbeat_timeout` 秒未收到心跳即判定掉线并告警
- ✅ **重连通知**：恢复心跳后自动发送恢复通知
- ✅ **生命周期事件**：监听 `lifecycle.connect / enable / disable` 事件
- ✅ **告警冷却**：同类事件冷却时间内只告警一次，避免刷屏
- ✅ **飞书签名**：支持飞书机器人签名校验
- ✅ **多实例区分**：通过 `bot_alias` 区分不同 NapCat 实例
- ✅ **@所有人**：严重告警时可 `@all` 提醒群成员

## 安装

将本目录放入 AstrBot 的 `data/plugins/` 下，重启 AstrBot 即可。或在 AstrBot WebUI 插件市场安装后启用。

依赖：`aiohttp >= 3.8.0`（一般 AstrBot 已自带）。

## 配置

在 AstrBot WebUI → 插件配置 中填写：

| 字段 | 说明 | 默认值 |
| --- | --- | --- |
| `feishu_webhook` | 飞书群机器人 Webhook 地址 | 内置示例 |
| `feishu_secret` | 飞书签名密钥（启用签名时必填） | 内置示例 |
| `heartbeat_timeout` | 心跳超时秒数（建议为 NapCat 心跳间隔的 2-3 倍） | `60` |
| `check_interval` | 巡检间隔秒数 | `15` |
| `alert_cooldown` | 同类事件冷却秒数 | `300` |
| `alert_at_all` | 严重告警是否 @所有人 | `true` |
| `bot_alias` | 机器人别名 | `AstrBot-NapCat` |
| `enable_startup_notice` | 启动时是否推送上线通知 | `true` |

### 获取飞书 Webhook

1. 在飞书群中点击右上角「设置」→「群机器人」→「添加机器人」→「自定义机器人」
2. 复制 Webhook 地址填入 `feishu_webhook`
3. （可选）开启「签名校验」并将密钥填入 `feishu_secret`

## 指令

| 指令 | 说明 |
| --- | --- |
| `/napcat status` | 查看当前监控状态（最近心跳、在线时长、告警次数） |
| `/napcat test` | 手动触发一次飞书告警测试 |
| `/napcat reset` | 重置告警冷却计时器 |

## 工作原理

1. 插件注册 OneBot `meta_event` 元事件监听器
2. 每收到 `heartbeat` 时记录时间戳
3. 后台异步任务每 `check_interval` 秒检查 `now - last_heartbeat > heartbeat_timeout`
4. 状态切换（在线↔掉线）时触发飞书告警
5. 同类告警在 `alert_cooldown` 内只发送一次

## 告警示例

**掉线告警（红色卡片，@所有人）**
```
🔴 [AstrBot-NapCat] NapCat 掉线告警
最后心跳：2025-05-06 16:42:15
离线时长：83 秒
超时阈值：60 秒
请尽快检查 NapCat 进程 / 网络 / QQ 登录状态
```

**恢复通知（绿色卡片）**
```
🟢 [AstrBot-NapCat] NapCat 已恢复在线
断线时长：2 分 13 秒
恢复时间：2025-05-06 16:44:28
```

## 常见问题

**Q：心跳一直收不到？**  
A：确认 NapCat 配置中 `heartbeat.enable = true`，且 `interval` 不超过 `heartbeat_timeout / 2`。

**Q：飞书提示签名校验失败？**  
A：确认 `feishu_secret` 与机器人「安全设置」中的签名密钥一致，且服务器时间正确。

**Q：如何对接多个 NapCat？**  
A：每个实例独立部署一个 AstrBot，分别配置不同 `bot_alias` 和 `feishu_webhook` 即可。

## 许可证

MIT
