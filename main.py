"""NapCat 主动探测与飞书告警插件。"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import inspect
import json
import time
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import aiohttp
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

try:
    from astrbot.api import AstrBotConfig
except ImportError:  # 兼容部分 AstrBot 版本的配置类导出位置
    from astrbot.core.config.astrbot_config import AstrBotConfig


PLATFORM_NAME = "aiocqhttp"
DEFAULT_CHECK_INTERVAL = 30
DEFAULT_PROBE_TIMEOUT = 10
DEFAULT_FAILURE_THRESHOLD = 2
DEFAULT_ALERT_COOLDOWN = 300
DEFAULT_NOTIFY_TIMEOUT = 10
DEFAULT_NOTIFY_RETRIES = 2
LOGIN_INFO_REFRESH_INTERVAL = 300


@dataclass(slots=True)
class MonitorState:
    """单个 aiocqhttp 适配器实例的探测状态。"""

    platform_id: str
    platform_name: str = PLATFORM_NAME
    self_id: str = "未知"
    online: bool | None = None
    last_check_at: float | None = None
    last_success_at: float | None = None
    online_since: float | None = None
    offline_since: float | None = None
    consecutive_failures: int = 0
    success_count: int = 0
    last_error: str = ""
    last_login_lookup_at: float | None = None


class FeishuNotifier:
    """复用 HTTP 会话的飞书机器人通知器。"""

    def __init__(
        self,
        webhook: str,
        secret: str,
        timeout: int = DEFAULT_NOTIFY_TIMEOUT,
        retries: int = DEFAULT_NOTIFY_RETRIES,
    ) -> None:
        self.webhook = webhook.strip()
        self.secret = secret.strip()
        self.timeout = max(1, timeout)
        self.retries = max(0, retries)
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        """创建长生命周期 HTTP 会话。"""
        if not self.webhook:
            return
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            )

    async def close(self) -> None:
        """关闭 HTTP 会话。"""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def send(
        self,
        title: str,
        content: str,
        color: str = "red",
        at_all: bool = False,
        source: str = "AstrBot-NapCat",
    ) -> bool:
        """发送一条飞书卡片，失败时按配置重试。"""
        if not self.webhook:
            return False
        await self.start()
        if self._session is None:
            return False

        payload = self.build_payload(title, content, color, at_all, source)
        for attempt in range(self.retries + 1):
            try:
                async with self._session.post(self.webhook, json=payload) as response:
                    text = await response.text()
                    if response.status != 200:
                        raise RuntimeError(f"HTTP {response.status}: {text[:500]}")
                    _validate_feishu_response(text)
                    return True
            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                RuntimeError,
                TypeError,
            ) as exc:
                if attempt >= self.retries:
                    logger.error(
                        f"[NapCat监控] 飞书发送失败，已重试 {attempt} 次：{exc}"
                    )
                    return False
                delay = min(2**attempt, 8)
                logger.warning(
                    f"[NapCat监控] 飞书发送失败，将在 {delay} 秒后重试：{exc}"
                )
                await asyncio.sleep(delay)
        return False

    def build_payload(
        self,
        title: str,
        content: str,
        color: str,
        at_all: bool,
        source: str,
    ) -> dict[str, Any]:
        """构造飞书交互卡片请求体。"""
        elements: list[dict[str, Any]] = [
            {"tag": "div", "text": {"tag": "lark_md", "content": content}}
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
                        "content": (
                            f"时间：{format_time(time.time())} | 来源：{source}"
                        ),
                    }
                ],
            }
        )
        payload: dict[str, Any] = {
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
            timestamp = str(int(time.time()))
            payload["timestamp"] = timestamp
            payload["sign"] = generate_sign(timestamp, self.secret)
        return payload


@register(
    "astrbot_plugin_napcat_monitor",
    "TsoiTZF",
    "NapCat 主动探测与飞书告警",
    "2.0.0",
    "https://github.com/TsoiTZF/astrbot_plugin_napcat_monitor",
)
class NapCatMonitor(Star):
    """主动探测全部 aiocqhttp 实例，并在状态变化时发送告警。"""

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context)
        self.config = config
        self.webhook = _as_string(config.get("feishu_webhook", ""))
        self.secret = _as_string(config.get("feishu_secret", ""))
        self.check_interval = _positive_int(
            config.get("check_interval", DEFAULT_CHECK_INTERVAL),
            DEFAULT_CHECK_INTERVAL,
        )
        self.probe_timeout = _positive_int(
            config.get("probe_timeout", DEFAULT_PROBE_TIMEOUT),
            DEFAULT_PROBE_TIMEOUT,
        )
        self.failure_threshold = _positive_int(
            config.get("failure_threshold", DEFAULT_FAILURE_THRESHOLD),
            DEFAULT_FAILURE_THRESHOLD,
        )
        self.alert_cooldown = _non_negative_int(
            config.get("alert_cooldown", DEFAULT_ALERT_COOLDOWN),
            DEFAULT_ALERT_COOLDOWN,
        )
        self.alert_at_all = _as_bool(config.get("alert_at_all", True), True)
        self.alert_on_initial_failure = _as_bool(
            config.get("alert_on_initial_failure", True), True
        )
        self.bot_alias = (
            _as_string(config.get("bot_alias", "AstrBot-NapCat")) or "AstrBot-NapCat"
        )
        self.enable_startup_notice = _as_bool(
            config.get("enable_startup_notice", True), True
        )
        self.enable_recovery_notice = _as_bool(
            config.get("enable_recovery_notice", True), True
        )
        notify_timeout = _positive_int(
            config.get("notify_timeout", DEFAULT_NOTIFY_TIMEOUT),
            DEFAULT_NOTIFY_TIMEOUT,
        )
        notify_retries = _non_negative_int(
            config.get("notify_retries", DEFAULT_NOTIFY_RETRIES),
            DEFAULT_NOTIFY_RETRIES,
        )

        self._notifier = FeishuNotifier(
            self.webhook,
            self.secret,
            timeout=notify_timeout,
            retries=notify_retries,
        )
        self._states: dict[str, MonitorState] = {}
        self._platform_identity_keys: dict[int, tuple[Any, str]] = {}
        self._cooldown: dict[str, float] = {}
        self._alerts_in_flight: set[str] = set()
        self._state_lock = asyncio.Lock()
        self._alert_lock = asyncio.Lock()
        self._probe_lock = asyncio.Lock()
        self._monitor_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._missing_platform_warned = False

    async def initialize(self) -> None:
        """启动通知器和主动探测循环。"""
        await self._notifier.start()
        self._stopping.clear()
        if self._monitor_task is None or self._monitor_task.done():
            self._monitor_task = asyncio.create_task(
                self._monitor_loop(), name="napcat-monitor-probe"
            )
        logger.info(
            "[NapCat监控] 已启动主动探测："
            f"间隔 {self.check_interval} 秒，超时 {self.probe_timeout} 秒，"
            f"连续失败阈值 {self.failure_threshold} 次"
        )
        if self.enable_startup_notice:
            await self._send_feishu(
                title="✅ NapCat 监控已启动",
                content=(
                    f"**探测间隔**：{self.check_interval} 秒\n"
                    f"**失败阈值**：{self.failure_threshold} 次\n"
                    "**探测方式**：OneBot `get_status` 主动调用"
                ),
                color="green",
            )

    async def terminate(self) -> None:
        """停止后台任务并释放 HTTP 会话。"""
        self._stopping.set()
        if self._monitor_task is not None and not self._monitor_task.done():
            self._monitor_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._monitor_task
        self._monitor_task = None
        await self._notifier.close()
        logger.info("[NapCat监控] 已停止")

    async def _monitor_loop(self) -> None:
        """按固定周期执行探测，单轮不会与下一轮重叠。"""
        while not self._stopping.is_set():
            started_at = time.monotonic()
            try:
                await self._probe_all_platforms()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception(f"[NapCat监控] 探测循环异常：{exc}")

            elapsed = time.monotonic() - started_at
            delay = max(0.1, self.check_interval - elapsed)
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=delay)
            except asyncio.TimeoutError:
                continue

    def _get_aiocqhttp_platforms(self) -> list[Any]:
        """枚举全部 aiocqhttp 实例，并为旧版 Context 提供单实例回退。"""
        result: list[Any] = []
        seen: set[int] = set()
        for platform in self._read_platform_instances():
            identity = id(platform)
            if identity in seen or not _is_platform_type(platform, PLATFORM_NAME):
                continue
            result.append(platform)
            seen.add(identity)

        if result:
            return result

        legacy_platform = self._get_legacy_platform()
        if legacy_platform is not None and _is_platform_type(
            legacy_platform, PLATFORM_NAME
        ):
            return [legacy_platform]
        return []

    def _read_platform_instances(self) -> list[Any]:
        """读取平台管理器实例列表，公开接口异常时回退内部列表。"""
        manager = getattr(self.context, "platform_manager", None)
        get_insts = getattr(manager, "get_insts", None)
        if callable(get_insts):
            try:
                instances = list(get_insts() or [])
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    f"[NapCat监控] 平台管理器查询失败，回退读取实例列表：{exc}"
                )
            else:
                if instances:
                    return instances
        return list(getattr(manager, "platform_insts", []) or [])

    def _get_legacy_platform(self) -> Any | None:
        """读取旧版 Context 暴露的单个平台实例。"""
        getter = getattr(self.context, "get_platform", None)
        if not callable(getter):
            return None
        try:
            return getter(PLATFORM_NAME)
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"[NapCat监控] 旧版平台查询失败：{exc}")
            return None

    def _assign_platform_keys(self, platforms: list[Any]) -> list[tuple[Any, str]]:
        """为当前平台对象分配稳定键，并允许同逻辑实例重建后复用状态。"""
        previous_records = self._platform_identity_keys
        current_records: dict[int, tuple[Any, str]] = {}
        occupied: set[str] = set()
        entries: list[tuple[Any, str]] = []
        for index, platform in enumerate(platforms):
            identity = id(platform)
            record = previous_records.get(identity)
            if (
                record is not None
                and record[0] is platform
                and record[1] not in occupied
            ):
                platform_key = record[1]
            else:
                base_key = _platform_id(platform, index)
                platform_key = base_key
                suffix = 2
                while platform_key in occupied:
                    platform_key = f"{base_key}#{suffix}"
                    suffix += 1
                if platform_key != base_key:
                    logger.warning(
                        "[NapCat监控] 检测到重复的平台实例 ID "
                        f"{base_key}，内部状态键调整为 {platform_key}"
                    )

            current_records[identity] = (platform, platform_key)
            occupied.add(platform_key)
            entries.append((platform, platform_key))
            if record is None and platform_key in self._states:
                self._states[platform_key].last_login_lookup_at = None

        self._platform_identity_keys = current_records
        return entries

    async def _probe_all_platforms(self) -> int:
        """并发探测当前实例，并把从平台管理器消失的实例计为失败。"""
        async with self._probe_lock:
            platforms = self._get_aiocqhttp_platforms()
            entries = self._assign_platform_keys(platforms)
            current_ids = {platform_id for _, platform_id in entries}
            async with self._state_lock:
                missing_ids = sorted(set(self._states) - current_ids)

            if not platforms:
                if not self._missing_platform_warned:
                    logger.warning("[NapCat监控] 未发现已加载的 aiocqhttp 平台实例")
                    self._missing_platform_warned = True
            else:
                self._missing_platform_warned = False

            tasks = [
                self._probe_platform(platform, platform_id)
                for platform, platform_id in entries
            ]
            tasks.extend(
                self._record_probe_failure(
                    platform_id, "平台实例未出现在 AstrBot 平台管理器中"
                )
                for platform_id in missing_ids
            )
            if tasks:
                await asyncio.gather(*tasks)
            return len(platforms)

    async def _probe_platform(self, platform: Any, platform_id: str) -> None:
        """调用 OneBot get_status，并将结果写入状态机。"""
        try:
            client = platform.get_client()
            call_action = _get_call_action(client)
            raw_status = await _call_action_with_timeout(
                call_action, "get_status", self.probe_timeout
            )
            status_data = _unwrap_action_result(raw_status)
            if not _status_is_online(status_data):
                raise RuntimeError(f"get_status 返回离线状态：{status_data!r}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            await self._record_probe_failure(platform_id, str(exc))
            return

        self_id = await self._resolve_self_id(platform_id, call_action)
        await self._record_probe_success(platform_id, self_id)

    async def _resolve_self_id(self, platform_id: str, call_action: Any) -> str:
        """按较低频率读取登录账号；读取失败不影响在线判定。"""
        now = time.monotonic()
        async with self._state_lock:
            current = self._get_or_create_state(platform_id)
            existing_id = current.self_id
            if (
                current.last_login_lookup_at is not None
                and now - current.last_login_lookup_at < LOGIN_INFO_REFRESH_INTERVAL
            ):
                return existing_id
            current.last_login_lookup_at = now

        try:
            raw_login = await _call_action_with_timeout(
                call_action, "get_login_info", self.probe_timeout
            )
            login_data = _unwrap_action_result(raw_login)
            if isinstance(login_data, dict):
                value = login_data.get("user_id", login_data.get("self_id"))
                if value not in (None, ""):
                    return str(value)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                f"[NapCat监控] 实例 {platform_id} 获取登录账号失败，不影响探测：{exc}"
            )
        return existing_id

    async def _record_probe_success(self, platform_id: str, self_id: str) -> None:
        """记录成功探测，并在离线转在线时发送恢复通知。"""
        now = time.time()
        should_recover = False
        offline_duration: float | None = None
        async with self._state_lock:
            state = self._get_or_create_state(platform_id)
            was_online = state.online
            if self_id != "未知":
                state.self_id = self_id
            state.online = True
            state.last_check_at = now
            state.last_success_at = now
            state.consecutive_failures = 0
            state.success_count += 1
            state.last_error = ""
            if was_online is not True:
                state.online_since = now
            if was_online is False:
                should_recover = True
                if state.offline_since is not None:
                    offline_duration = now - state.offline_since
            state.offline_since = None
            display_id = state.self_id

        if should_recover:
            await self._clear_cooldown(platform_id, "offline")
        if should_recover and self.enable_recovery_notice:
            await self._alert(
                platform_id=platform_id,
                event="recovery",
                title="✅ NapCat 已恢复在线",
                content=(
                    f"**平台实例**：{platform_id}\n"
                    f"**QQ 账号**：{display_id}\n"
                    f"**离线时长**：{format_duration(offline_duration)}"
                ),
                at_all=False,
                color="green",
            )

    async def _record_probe_failure(self, platform_id: str, error: str) -> None:
        """累计失败；达到阈值后重试告警，由冷却控制持续故障频率。"""
        now = time.time()
        should_alert = False
        transitioned_offline = False
        async with self._state_lock:
            state = self._get_or_create_state(platform_id)
            was_online = state.online
            state.last_check_at = now
            state.consecutive_failures += 1
            state.last_error = error
            failures = state.consecutive_failures
            if failures < self.failure_threshold:
                return

            if was_online is not False:
                state.online = False
                state.offline_since = now
                transitioned_offline = True
            should_alert = (
                state.last_success_at is not None or self.alert_on_initial_failure
            )
            display_id = state.self_id

        if transitioned_offline:
            await self._clear_cooldown(platform_id, "recovery")
        if should_alert:
            await self._alert(
                platform_id=platform_id,
                event="offline",
                title="🚨 NapCat 探测失败",
                content=(
                    f"**平台实例**：{platform_id}\n"
                    f"**QQ 账号**：{display_id}\n"
                    f"**连续失败**：{failures} 次\n"
                    f"**最后错误**：{error}"
                ),
                at_all=self.alert_at_all,
                color="red",
            )

    @filter.command("napcat_status")
    async def cmd_status(self, event: AstrMessageEvent):
        """查看全部 NapCat 实例的探测状态。"""
        yield event.plain_result(await self._build_status_text())

    @filter.command("napcat_probe")
    async def cmd_probe(self, event: AstrMessageEvent):
        """立即执行一轮主动探测。"""
        count = await self._probe_all_platforms()
        if count == 0:
            yield event.plain_result("⚠️ 未发现已加载的 aiocqhttp 平台实例。")
            return
        yield event.plain_result(
            f"✅ 已完成 {count} 个 aiocqhttp 实例的主动探测。\n"
            f"{await self._build_status_text()}"
        )

    @filter.command("napcat_test")
    async def cmd_test(self, event: AstrMessageEvent):
        """发送测试通知。"""
        success = await self._send_feishu(
            title="🧪 NapCat 监控测试",
            content="飞书通知链路工作正常。",
            color="blue",
        )
        if success:
            yield event.plain_result("✅ 飞书测试通知发送成功。")
        elif not self.webhook:
            yield event.plain_result("⚠️ 未配置 feishu_webhook，无法发送测试通知。")
        else:
            yield event.plain_result("❌ 飞书测试通知发送失败，请查看 AstrBot 日志。")

    @filter.command("napcat_reset")
    async def cmd_reset(self, event: AstrMessageEvent):
        """清空告警冷却计时。"""
        async with self._alert_lock:
            self._cooldown.clear()
        yield event.plain_result("✅ 已清空 NapCat 告警冷却计时。")

    async def _build_status_text(self) -> str:
        """构造用户可读的状态摘要。"""
        async with self._state_lock:
            states = list(self._states.values())
        if not states:
            return "尚无探测记录。后台会自动探测，也可执行 /napcat_probe 立即探测。"

        now = time.time()
        lines = [f"NapCat 监控状态（{len(states)} 个实例）"]
        for state in sorted(states, key=lambda item: item.platform_id):
            if state.online is True and state.consecutive_failures:
                status = "异常待确认"
            elif state.online is True:
                status = "在线"
            elif state.online is False:
                status = "离线"
            else:
                status = "等待确认"
            lines.extend(
                [
                    "",
                    f"[{state.platform_id}] {status}",
                    f"QQ 账号：{state.self_id}",
                    f"最后检查：{format_time(state.last_check_at)}",
                    f"最后成功：{format_time(state.last_success_at)}",
                    f"距上次成功：{format_duration(_elapsed(now, state.last_success_at))}",
                    f"连续失败：{state.consecutive_failures} 次",
                    f"成功探测：{state.success_count} 次",
                ]
            )
            if state.last_error:
                lines.append(f"最后错误：{state.last_error}")
        return "\n".join(lines)

    async def _clear_cooldown(self, platform_id: str, event: str) -> None:
        """状态转换时清除上一事件的冷却，避免新故障被旧记录压制。"""
        async with self._alert_lock:
            self._cooldown.pop(f"{platform_id}:{event}", None)

    async def _alert(
        self,
        platform_id: str,
        event: str,
        title: str,
        content: str,
        at_all: bool,
        color: str,
    ) -> bool:
        """发送带冷却的告警，只有发送成功后才写入冷却时间。"""
        key = f"{platform_id}:{event}"
        now = time.monotonic()
        async with self._alert_lock:
            last = self._cooldown.get(key)
            if last is not None and now - last < self.alert_cooldown:
                logger.debug(f"[NapCat监控] 告警冷却中，跳过 {key}")
                return False
            if key in self._alerts_in_flight:
                logger.debug(f"[NapCat监控] 同类告警正在发送，跳过重复请求 {key}")
                return False
            self._alerts_in_flight.add(key)

        success = False
        try:
            success = await self._send_feishu(
                title=title,
                content=content,
                color=color,
                at_all=at_all,
            )
            return success
        finally:
            async with self._alert_lock:
                self._alerts_in_flight.discard(key)
                if success:
                    self._cooldown[key] = time.monotonic()

    async def _send_feishu(
        self,
        title: str,
        content: str,
        color: str = "red",
        at_all: bool = False,
    ) -> bool:
        """发送飞书通知；未配置 Webhook 时只保留本地监控。"""
        if not self.webhook:
            logger.debug("[NapCat监控] 未配置 feishu_webhook，跳过飞书通知")
            return False
        try:
            return await self._notifier.send(
                title=title,
                content=content,
                color=color,
                at_all=at_all,
                source=self.bot_alias,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"[NapCat监控] 通知器异常，已隔离本次发送失败：{exc}")
            return False

    def _get_or_create_state(self, platform_id: str) -> MonitorState:
        state = self._states.get(platform_id)
        if state is None:
            state = MonitorState(platform_id=platform_id)
            self._states[platform_id] = state
        return state


def _is_platform_type(platform: Any, expected_name: str) -> bool:
    try:
        metadata = platform.meta()
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"[NapCat监控] 读取平台元数据失败：{exc}")
        return False
    return _as_string(getattr(metadata, "name", "")) == expected_name


def _platform_id(platform: Any, index: int) -> str:
    try:
        metadata = platform.meta()
        value = getattr(metadata, "id", "")
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"[NapCat监控] 读取平台 ID 失败：{exc}")
        value = ""
    return _as_string(value) or f"{PLATFORM_NAME}-{index + 1}"


def _get_call_action(client: Any) -> Any:
    """获取 aiocqhttp 的 API 调用函数，并兼容少量旧版封装。"""
    api = getattr(client, "api", None)
    call_action = getattr(api, "call_action", None)
    if callable(call_action):
        return call_action
    direct_call = getattr(client, "call_action", None)
    if callable(direct_call):
        return direct_call
    raise RuntimeError("aiocqhttp 客户端不支持 call_action")


async def _call_action_with_timeout(call_action: Any, action: str, timeout: int) -> Any:
    """兼容 aiocqhttp 同步或异步动作实现，并只对异步等待施加超时。"""
    result = call_action(action)
    if inspect.isawaitable(result):
        return await asyncio.wait_for(result, timeout=timeout)
    return result


def _validate_feishu_response(text: str) -> dict[str, Any]:
    """校验飞书机器人响应，避免把代理页或空响应误判为发送成功。"""
    if not text.strip():
        raise RuntimeError("飞书返回空响应")
    try:
        result = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"飞书返回非 JSON 响应：{text[:200]}") from exc
    if not isinstance(result, dict):
        raise TypeError(f"飞书响应格式错误：{result!r}")

    if "code" in result:
        code = result["code"]
    elif "StatusCode" in result:
        code = result["StatusCode"]
    else:
        raise RuntimeError(f"飞书响应缺少业务状态码：{result}")
    if code not in (0, "0"):
        raise RuntimeError(f"飞书业务错误：{result}")
    return result


def _unwrap_action_result(result: Any) -> Any:
    """兼容 aiocqhttp 直返数据与 OneBot 标准响应包。"""
    if not isinstance(result, dict):
        return result

    retcode = result.get("retcode")
    if retcode not in (None, 0, "0"):
        raise RuntimeError(f"OneBot 返回错误码 {retcode}：{result}")
    response_status = result.get("status")
    if isinstance(response_status, str) and response_status.lower() in {
        "failed",
        "error",
    }:
        raise RuntimeError(f"OneBot 调用失败：{result}")
    if "data" in result:
        return result["data"]
    return result


def _status_is_online(status: Any) -> bool:
    """仅在 get_status 的全部已知健康字段均为真时判定可用。"""
    if not isinstance(status, dict):
        return False
    checks: list[bool] = []
    if "online" in status:
        checks.append(_as_bool(status["online"], False))
    if "good" in status:
        checks.append(_as_bool(status["good"], False))
    return bool(checks) and all(checks)


def _as_string(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on", "是", "启用"}:
            return True
        if normalized in {"false", "0", "no", "n", "off", "否", "禁用"}:
            return False
    return default


def _positive_int(value: Any, default: int) -> int:
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return default


def _non_negative_int(value: Any, default: int) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def generate_sign(timestamp: str, secret: str) -> str:
    """生成飞书机器人签名。"""
    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def format_time(timestamp: float | None) -> str:
    if timestamp is None:
        return "未知"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))


def format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "未知"
    total = max(0, int(seconds))
    minutes, remainder = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours} 小时 {minutes} 分钟"
    if minutes:
        return f"{minutes} 分钟 {remainder} 秒"
    return f"{remainder} 秒"


def _elapsed(now: float, timestamp: float | None) -> float | None:
    if timestamp is None:
        return None
    return now - timestamp
