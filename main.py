import asyncio
import base64
import hashlib
import hmac
import time
from typing import Any, Dict, Optional

import aiohttp

from astrbot.api import logger, AstrBotConfig
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register


@register(
    "astrbot_plugin_napcat_monitor",
    "TsoiTZF",
    "NapCat 掉线监控 + 飞书告警插件",
    "1.0.0",
    "https://github.com/TsoiTZF/astrbot_plugin_napcat_monitor",
)
class NapCatMonitor(Star):
    """主动探测 NapCat 在线状态，断连/重连/异常事件实时推送到飞书群机器人。"""

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context)
        self.config = config

        # 配置项
        self.webhook: str = config.get("feishu_webhook", "").strip()
        self.secret: str = config.get("feishu_secret", "").strip()
        self.check_interval: int = int(config.get("check_interval", 30))
        self.alert_cooldown: int = int(config.get("alert_cooldown", 300))
        self.alert_at_all: bool = bool(config.get("alert_at_all", True))
        self.bot_alias: str = config.get("bot_alias", "AstrBot-NapCat")
        self.enable_startup_notice: bool = bool(
            config.get("enable_startup_notice", True)
        )

        # 运行时状态：{platform: {"online": bool, "last_check": ts, "self_id": str}}
        self._state: Dict[str, Dict[str, Any]] = {}
        # 告警冷却：{f"{platform}:{event}": last_alert_ts}
        self._cooldown: Dict[str, float] = {}

        self._monitor_task: Optional[asyncio.Task] = None
        self._stopped: bool = False

    async def initialize(self) -> None:
        """插件加载时启动主动探测任务。"""
        if not self.webhook:
            logger.warning("[NapCatMonitor] 未配置 feishu_webhook，告警功能不可用")
            return

        self._monitor_task = asyncio.create_task(self._active_probe_loop())
        logger.info(
            f"[NapCatMonitor] 启动完成：探测间隔 {self.check_interval}s / 冷却 {self.alert_cooldown}s"
        )

        if self.enable_startup_notice:
            await self._send_feishu(
                title=f"✅ {self.bot_alias} 监控已上线",
                content=(
                    f"插件已启动，正在主动探测 NapCat 状态。\n"
                    f"- 探测间隔：{self.check_interval} 秒\n"
                    f"- 告警冷却：{self.alert_cooldown} 秒"
                ),
                color="green",
                at_all=False,
            )

    async def terminate(self) -> None:
        """插件卸载/重载时停止探测任务。"""
        self._stopped = True
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("[NapCatMonitor] 已停止")

    # ------------------------------------------------------------------ #
    # 主动探测机制
    # ------------------------------------------------------------------ #

    async def _active_probe_loop(self) -> None:
        """周期性主动探测 NapCat 状态。"""
        while not self._stopped:
            try:
                await asyncio.sleep(self.check_interval)
                await self._probe_all_platforms()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"[NapCatMonitor] 探测循环异常：{exc}")

    async def _probe_all_platforms(self) -> None:
        """探测所有已注册平台的 NapCat 状态。"""
        # 获取 aiocqhttp 平台适配器
        try:
            adapter = self.context.platform_manager.get_platform("aiocqhttp")
        except Exception:
            return

        if not adapter:
            return

        platform_name = "aiocqhttp"
        try:
            is_online = await self._probe_onebot_status(adapter)
            self._update_platform_state(platform_name, is_online, adapter)
        except Exception as exc:
            logger.debug(f"[NapCatMonitor] 探测 {platform_name} 失败：{exc}")
            self._update_platform_state(platform_name, False, adapter)

    async def _probe_onebot_status(self, adapter) -> bool:
        """通过 OneBot API 探测 NapCat 是否在线。"""
        try:
            # 调用 get_status API
            if hasattr(adapter, 'call_api'):
                result = await asyncio.wait_for(
                    adapter.call_api("get_status", {}),
                    timeout=10.0
                )
                return result is not None
            # 备用方案：调用 get_login_info
            elif hasattr(adapter, 'get_login_info'):
                result = await asyncio.wait_for(
                    adapter.get_login_info(),
                    timeout=10.0
                )
                return result is not None
        except (asyncio.TimeoutError, Exception):
            pass
        return False

    def _update_platform_state(self, platform_name: str, is_online: bool, adapter) -> None:
        """更新平台状态，必要时触发告警。"""
        now = time.time()
        prev_state = self._state.get(platform_name, {})
        was_online = prev_state.get("online", None)

        # 获取 self_id
        self_id = "unknown"
        try:
            if hasattr(adapter, 'get_self_id'):
                self_id = str(adapter.get_self_id() or "unknown")
        except Exception:
            pass

        self._state[platform_name] = {
            "online": is_online,
            "last_check": now,
            "self_id": self_id,
        }

        # 状态变化时触发告警
        if was_online is None:
            # 首次探测，记录状态但不告警
            return
        elif was_online and not is_online:
            # 在线 -> 离线
            asyncio.create_task(
                self._alert(
                    platform=platform_name,
                    event="offline",
                    title=f"🔴 {self.bot_alias} 掉线",
                    content=(
                        f"NapCat 实例 `{self_id}` 探测失败！\n"
                        f"- 平台：{platform_name}\n"
                        f"- 时间：{self._fmt_time(now)}\n"
                        f"请检查 NapCat 进程、网络连接、QQ 登录状态。"
                    ),
                    color="red",
                    at_all=self.alert_at_all,
                )
            )
        elif not was_online and is_online:
            # 离线 -> 在线
            asyncio.create_task(
                self._alert(
                    platform=platform_name,
                    event="reconnect",
                    title=f"🟢 {self.bot_alias} 已恢复",
                    content=(
                        f"NapCat 实例 `{self_id}` 已重新上线。\n"
                        f"- 平台：{platform_name}\n"
                        f"- 时间：{self._fmt_time(now)}"
                    ),
                    color="green",
                    at_all=False,
                )
            )

    # ------------------------------------------------------------------ #
    # 指令
    # ------------------------------------------------------------------ #

    @filter.command("napcat_status")
    async def cmd_status(self, event: AstrMessageEvent):
        """查看当前监控状态。"""
        if not self._state:
            yield event.plain_result("📭 暂未探测到任何平台，请检查适配器配置。")
            return
        now = time.time()
        lines = [f"📊 {self.bot_alias} 监控状态："]
        for platform, st in self._state.items():
            online = "🟢 在线" if st.get("online") else "🔴 离线"
            delta = int(now - float(st.get("last_check", 0)))
            self_id = st.get("self_id", "?")
            lines.append(
                f"- `{self_id}` ({platform}) {online} | "
                f"上次探测 {delta}s 前"
            )
        yield event.plain_result("\n".join(lines))

    @filter.command("napcat_test")
    async def cmd_test(self, event: AstrMessageEvent):
        """发送一条飞书测试告警。"""
        ok = await self._send_feishu(
            title=f"🧪 {self.bot_alias} 测试告警",
            content="这是一条来自 napcat_monitor 插件的测试消息。",
            color="blue",
            at_all=False,
        )
        if ok:
            yield event.plain_result("✅ 已发送飞书测试告警，请在群里查看。")
        else:
            yield event.plain_result("❌ 飞书告警发送失败，请检查 webhook / secret 配置或后台日志。")

    # ------------------------------------------------------------------ #
    # 飞书告警
    # ------------------------------------------------------------------ #

    async def _alert(
        self,
        platform: str,
        event: str,
        title: str,
        content: str,
        color: str = "red",
        at_all: bool = False,
    ) -> None:
        """带冷却的告警发送。"""
        key = f"{platform}:{event}"
        now = time.time()
        last = self._cooldown.get(key, 0)
        if now - last < self.alert_cooldown:
            logger.debug(f"[NapCatMonitor] 告警冷却中，跳过 {key}")
            return
        self._cooldown[key] = now
        await self._send_feishu(title=title, content=content, color=color, at_all=at_all)

    async def _send_feishu(
        self,
        title: str,
        content: str,
        color: str = "red",
        at_all: bool = False,
    ) -> bool:
        """调用飞书自定义机器人 webhook 发送富文本卡片。"""
        if not self.webhook:
            logger.warning("[NapCatMonitor] 未配置 webhook，丢弃告警")
            return False

        elements = [
            {
                "tag": "div",
                "text": {"tag": "lark_md", "content": content},
            }
        ]
        if at_all:
            elements.append(
                {
                    "tag": "div",
                    "text": {"tag": "lark_md", "content": "<at id=all></at>"},
                }
            )
        elements.append(
            {
                "tag": "note",
                "elements": [
                    {
                        "tag": "plain_text",
                        "content": f"⏱ {self._fmt_time(time.time())}  |  来源：{self.bot_alias}",
                    }
                ],
            }
        )

        payload: Dict[str, Any] = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"tag": "plain_text", "content": title},
                    "template": color,
                },
                "elements": elements,
            },
        }

        if self.secret:
            ts = str(int(time.time()))
            payload["timestamp"] = ts
            payload["sign"] = self._gen_sign(ts, self.secret)

        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(self.webhook, json=payload) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        logger.error(
                            f"[NapCatMonitor] 飞书返回 HTTP {resp.status}: {text}"
                        )
                        return False
                    # 飞书业务错误码
                    try:
                        data = await resp.json(content_type=None)
                    except Exception:
                        data = {}
                    code = data.get("code", data.get("StatusCode", 0))
                    if code not in (0, None):
                        logger.error(f"[NapCatMonitor] 飞书业务错误：{data}")
                        return False
                    return True
        except Exception as exc:
            logger.error(f"[NapCatMonitor] 发送飞书告警异常：{exc}")
            return False

    # ------------------------------------------------------------------ #
    # 工具方法
    # ------------------------------------------------------------------ #

    @staticmethod
    def _gen_sign(timestamp: str, secret: str) -> str:
        """飞书机器人签名算法。"""
        string_to_sign = f"{timestamp}\n{secret}"
        hmac_code = hmac.new(
            string_to_sign.encode("utf-8"), digestmod=hashlib.sha256
        ).digest()
        return base64.b64encode(hmac_code).decode("utf-8")

    @staticmethod
    def _fmt_time(ts: float) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
