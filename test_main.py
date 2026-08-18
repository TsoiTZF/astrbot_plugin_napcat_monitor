"""NapCat 主动探测、状态机与通知冷却测试。"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import importlib.util
import socket
import sys
import types
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web


class Logger:
    def __init__(self) -> None:
        self.records: list[tuple[str, str]] = []

    def debug(self, message: str) -> None:
        self.records.append(("debug", message))

    def info(self, message: str) -> None:
        self.records.append(("info", message))

    def warning(self, message: str) -> None:
        self.records.append(("warning", message))

    def error(self, message: str) -> None:
        self.records.append(("error", message))

    def exception(self, message: str) -> None:
        self.records.append(("exception", message))


class Filter:
    @staticmethod
    def command(*_args, **_kwargs):
        return lambda function: function


class FakePlatformManager:
    def __init__(self, platforms=None) -> None:
        self.platform_insts = list(platforms or [])

    def get_insts(self):
        return list(self.platform_insts)


class Context:
    def __init__(self, platforms=None) -> None:
        self.platform_manager = FakePlatformManager(platforms)

    def get_platform(self, platform_name: str):
        for platform in self.platform_manager.platform_insts:
            if platform.meta().name == platform_name:
                return platform
        return None


class Star:
    def __init__(self, context: Context) -> None:
        self.context = context


class AstrBotConfig(dict):
    pass


class Event:
    def plain_result(self, text: str) -> str:
        return text


class FakeNotifier:
    def __init__(self, outcomes=None) -> None:
        self.outcomes = list(outcomes or [])
        self.calls: list[dict[str, Any]] = []
        self.started = False
        self.closed = False

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.closed = True

    async def send(self, **kwargs) -> bool:
        self.calls.append(kwargs)
        if self.outcomes:
            return self.outcomes.pop(0)
        return True


class FakeAPI:
    def __init__(self, responses: dict[str, list[Any]]) -> None:
        self.responses = {key: list(value) for key, value in responses.items()}
        self.calls: list[str] = []

    async def call_action(self, action: str) -> Any:
        self.calls.append(action)
        values = self.responses.get(action, [])
        if not values:
            raise RuntimeError(f"没有为 {action} 配置测试响应")
        value = values.pop(0) if len(values) > 1 else values[0]
        if isinstance(value, BaseException):
            raise value
        return value


class FakePlatform:
    def __init__(
        self,
        platform_id: str,
        responses: dict[str, list[Any]],
        platform_name: str = "aiocqhttp",
    ) -> None:
        self._metadata = types.SimpleNamespace(name=platform_name, id=platform_id)
        self.client = types.SimpleNamespace(api=FakeAPI(responses))

    def meta(self):
        return self._metadata

    def get_client(self):
        return self.client


def register(*_args, **_kwargs):
    return lambda cls: cls


logger = Logger()
astrbot_module = types.ModuleType("astrbot")
api_module = types.ModuleType("astrbot.api")
event_module = types.ModuleType("astrbot.api.event")
star_module = types.ModuleType("astrbot.api.star")
api_module.logger = logger
api_module.AstrBotConfig = AstrBotConfig
event_module.AstrMessageEvent = Event
event_module.filter = Filter
star_module.Context = Context
star_module.Star = Star
star_module.register = register
astrbot_module.api = api_module
sys.modules.update(
    {
        "astrbot": astrbot_module,
        "astrbot.api": api_module,
        "astrbot.api.event": event_module,
        "astrbot.api.star": star_module,
    }
)

ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("napcat_monitor_main", ROOT / "main.py")
assert SPEC is not None and SPEC.loader is not None
module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = module
SPEC.loader.exec_module(module)


def make_plugin(platforms=None, **overrides):
    config = AstrBotConfig(
        {
            "feishu_webhook": "https://example.invalid/hook",
            "feishu_secret": "",
            "check_interval": 30,
            "probe_timeout": 1,
            "failure_threshold": 2,
            "alert_cooldown": 300,
            "alert_at_all": True,
            "alert_on_initial_failure": True,
            "enable_startup_notice": False,
            "enable_recovery_notice": True,
            "notify_timeout": 1,
            "notify_retries": 0,
        }
    )
    config.update(overrides)
    return module.NapCatMonitor(Context(platforms), config)


def run(coroutine):
    return asyncio.run(coroutine)


async def collect(agen) -> list[str]:
    return [item async for item in agen]


def test_config_normalization() -> None:
    plugin = make_plugin(
        check_interval=0,
        probe_timeout="bad",
        failure_threshold=-9,
        alert_cooldown=-10,
        alert_at_all="false",
        alert_on_initial_failure="否",
    )
    assert plugin.check_interval == 1
    assert plugin.probe_timeout == module.DEFAULT_PROBE_TIMEOUT
    assert plugin.failure_threshold == 1
    assert plugin.alert_cooldown == 0
    assert plugin.alert_at_all is False
    assert plugin.alert_on_initial_failure is False


def test_discovers_all_aiocqhttp_platforms() -> None:
    first = FakePlatform("napcat-a", {"get_status": [{"online": True}]})
    ignored = FakePlatform(
        "discord-a", {"get_status": [{"online": True}]}, platform_name="discord"
    )
    second = FakePlatform("napcat-b", {"get_status": [{"online": True}]})
    plugin = make_plugin([first, ignored, second])
    assert plugin._get_aiocqhttp_platforms() == [first, second]


def test_platform_discovery_falls_back_when_get_insts_fails() -> None:
    platform = FakePlatform("napcat-a", {"get_status": [{"online": True}]})
    plugin = make_plugin()

    class BrokenPlatformManager:
        def __init__(self) -> None:
            self.platform_insts = [platform]

        @staticmethod
        def get_insts():
            raise RuntimeError("平台管理器暂不可用")

    plugin.context.platform_manager = BrokenPlatformManager()
    assert plugin._get_aiocqhttp_platforms() == [platform]


def test_success_probe_supports_envelope_and_reads_login_info() -> None:
    platform = FakePlatform(
        "napcat-a",
        {
            "get_status": [{"status": "ok", "retcode": 0, "data": {"good": True}}],
            "get_login_info": [{"data": {"user_id": 123456, "nickname": "机器人"}}],
        },
    )
    plugin = make_plugin([platform])
    notifier = FakeNotifier()
    plugin._notifier = notifier

    run(plugin._probe_all_platforms())

    state = plugin._states["napcat-a"]
    assert state.online is True
    assert state.self_id == "123456"
    assert state.success_count == 1
    assert state.consecutive_failures == 0
    assert notifier.calls == []


def test_login_info_is_refreshed_after_cache_interval() -> None:
    platform = FakePlatform(
        "napcat-a",
        {
            "get_status": [{"online": True}, {"online": True}],
            "get_login_info": [{"user_id": 10001}, {"user_id": 10002}],
        },
    )
    plugin = make_plugin([platform])
    plugin._notifier = FakeNotifier()

    async def scenario() -> None:
        await plugin._probe_all_platforms()
        state = plugin._states["napcat-a"]
        assert state.self_id == "10001"
        assert state.last_login_lookup_at is not None
        state.last_login_lookup_at -= module.LOGIN_INFO_REFRESH_INTERVAL + 1
        await plugin._probe_all_platforms()

    run(scenario())
    assert plugin._states["napcat-a"].self_id == "10002"
    assert platform.client.api.calls.count("get_login_info") == 2


def test_transient_failure_threshold_offline_and_recovery() -> None:
    platform = FakePlatform(
        "napcat-a",
        {
            "get_status": [
                {"online": True, "good": True},
                asyncio.TimeoutError("探测超时"),
                {"online": False, "good": False},
                {"online": True, "good": True},
            ],
            "get_login_info": [{"user_id": 10001}],
        },
    )
    plugin = make_plugin([platform])
    notifier = FakeNotifier()
    plugin._notifier = notifier

    async def scenario() -> None:
        await plugin._probe_all_platforms()
        await plugin._probe_all_platforms()
        assert plugin._states["napcat-a"].online is True
        assert plugin._states["napcat-a"].consecutive_failures == 1
        await plugin._probe_all_platforms()
        assert plugin._states["napcat-a"].online is False
        await plugin._probe_all_platforms()

    run(scenario())

    state = plugin._states["napcat-a"]
    assert state.online is True
    assert state.consecutive_failures == 0
    assert [call["color"] for call in notifier.calls] == ["red", "green"]
    assert "连续失败" in notifier.calls[0]["content"]
    assert "离线时长" in notifier.calls[1]["content"]


def test_initial_failure_alerts_once_after_threshold() -> None:
    platform = FakePlatform(
        "napcat-a",
        {
            "get_status": [
                ConnectionError("连接失败"),
                ConnectionError("连接失败"),
                ConnectionError("连接失败"),
            ]
        },
    )
    plugin = make_plugin([platform])
    notifier = FakeNotifier()
    plugin._notifier = notifier

    async def scenario() -> None:
        await plugin._probe_all_platforms()
        await plugin._probe_all_platforms()
        await plugin._probe_all_platforms()

    run(scenario())

    state = plugin._states["napcat-a"]
    assert state.online is False
    assert state.consecutive_failures == 3
    assert len(notifier.calls) == 1


def test_login_info_failure_does_not_turn_online_probe_into_failure() -> None:
    platform = FakePlatform(
        "napcat-a",
        {
            "get_status": [{"online": True}],
            "get_login_info": [RuntimeError("接口不可用")],
        },
    )
    plugin = make_plugin([platform])
    run(plugin._probe_all_platforms())
    state = plugin._states["napcat-a"]
    assert state.online is True
    assert state.self_id == "未知"
    assert state.last_error == ""


def test_cooldown_is_written_only_after_success() -> None:
    plugin = make_plugin()
    notifier = FakeNotifier([False, True])
    plugin._notifier = notifier

    async def scenario() -> None:
        first = await plugin._alert("napcat-a", "offline", "标题", "内容", False, "red")
        assert first is False
        assert plugin._cooldown == {}
        second = await plugin._alert(
            "napcat-a", "offline", "标题", "内容", False, "red"
        )
        assert second is True
        assert "napcat-a:offline" in plugin._cooldown

    run(scenario())
    assert len(notifier.calls) == 2


def test_cooldown_isolated_by_platform_id() -> None:
    plugin = make_plugin()
    notifier = FakeNotifier()
    plugin._notifier = notifier

    async def scenario() -> None:
        assert await plugin._alert("napcat-a", "offline", "A", "A", False, "red")
        assert not await plugin._alert("napcat-a", "offline", "A", "A", False, "red")
        assert await plugin._alert("napcat-b", "offline", "B", "B", False, "red")

    run(scenario())
    assert len(notifier.calls) == 2


def test_monitor_starts_without_webhook() -> None:
    plugin = make_plugin(feishu_webhook="")

    async def scenario() -> None:
        await plugin.initialize()
        assert plugin._monitor_task is not None
        assert not plugin._monitor_task.done()
        await asyncio.sleep(0)
        await plugin.terminate()
        assert plugin._monitor_task is None

    run(scenario())


def test_manual_probe_command_reports_multiple_instances() -> None:
    first = FakePlatform(
        "napcat-a",
        {
            "get_status": [{"online": True}],
            "get_login_info": [{"user_id": 10001}],
        },
    )
    second = FakePlatform(
        "napcat-b",
        {
            "get_status": [{"online": True}],
            "get_login_info": [{"user_id": 10002}],
        },
    )
    plugin = make_plugin([first, second])
    results = run(collect(plugin.cmd_probe(Event())))
    assert len(results) == 1
    assert "已完成 2 个" in results[0]
    assert "napcat-a" in results[0]
    assert "10002" in results[0]


def test_onebot_response_parser_rejects_business_error() -> None:
    with pytest.raises(RuntimeError, match="错误码"):
        module._unwrap_action_result({"status": "failed", "retcode": 100, "data": None})
    assert module._status_is_online({"online": "true"}) is True
    assert module._status_is_online({"good": 0}) is False
    assert module._status_is_online({"online": True, "good": False}) is False
    assert module._status_is_online({}) is False
    assert module._status_is_online(None) is False


def test_feishu_signature_matches_reference_algorithm() -> None:
    timestamp = "1720000000"
    secret = "test-secret"
    expected = base64.b64encode(
        hmac.new(f"{timestamp}\n{secret}".encode(), digestmod=hashlib.sha256).digest()
    ).decode()
    assert module.generate_sign(timestamp, secret) == expected


def test_failed_offline_alert_is_retried_on_next_probe() -> None:
    platform = FakePlatform(
        "napcat-a",
        {"get_status": [ConnectionError("连接失败"), ConnectionError("连接失败")]},
    )
    plugin = make_plugin([platform], failure_threshold=1)
    notifier = FakeNotifier([False, True])
    plugin._notifier = notifier

    async def scenario() -> None:
        await plugin._probe_all_platforms()
        assert plugin._cooldown == {}
        await plugin._probe_all_platforms()

    run(scenario())
    assert len(notifier.calls) == 2
    assert "napcat-a:offline" in plugin._cooldown


def test_new_incident_is_not_suppressed_by_previous_offline_cooldown() -> None:
    platform = FakePlatform(
        "napcat-a",
        {
            "get_status": [
                {"online": True},
                {"online": False},
                {"online": True},
                {"online": False},
            ],
            "get_login_info": [{"user_id": 10001}],
        },
    )
    plugin = make_plugin([platform], failure_threshold=1)
    notifier = FakeNotifier()
    plugin._notifier = notifier

    async def scenario() -> None:
        for _ in range(4):
            await plugin._probe_all_platforms()

    run(scenario())
    assert [call["color"] for call in notifier.calls] == ["red", "green", "red"]


def test_feishu_notifier_accepts_success_response_and_reuses_session() -> None:
    class Response:
        status = 200

        async def text(self) -> str:
            return '{"code": 0}'

    class RequestContext:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Session:
        closed = False

        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []

        def post(self, webhook: str, json: dict[str, Any]):
            self.calls.append((webhook, json))
            return RequestContext()

    notifier = module.FeishuNotifier("https://example.invalid/hook", "", retries=0)
    session = Session()
    notifier._session = session
    assert run(notifier.send("标题", "内容", source="测试实例")) is True
    assert len(session.calls) == 1
    assert session.calls[0][1]["card"]["header"]["title"]["content"] == "标题"


def test_feishu_notifier_rejects_business_error() -> None:
    class Response:
        status = 200

        async def text(self) -> str:
            return '{"code": 19001, "msg": "签名错误"}'

    class RequestContext:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Session:
        closed = False

        def post(self, _webhook: str, json: dict[str, Any]):
            assert json["msg_type"] == "interactive"
            return RequestContext()

    notifier = module.FeishuNotifier("https://example.invalid/hook", "", retries=0)
    notifier._session = Session()
    assert run(notifier.send("标题", "内容")) is False


def test_call_action_timeout_helper_accepts_sync_result() -> None:
    result = run(
        module._call_action_with_timeout(
            lambda action: {"action": action, "online": True}, "get_status", 1
        )
    )
    assert result == {"action": "get_status", "online": True}


def test_production_probe_accepts_sync_call_action_result() -> None:
    class SyncAPI:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def call_action(self, action: str) -> Any:
            self.calls.append(action)
            if action == "get_status":
                return {"online": True, "good": True}
            if action == "get_login_info":
                return {"user_id": 9527}
            raise RuntimeError(f"不支持的动作：{action}")

    platform = FakePlatform("napcat-sync", {})
    platform.client = types.SimpleNamespace(api=SyncAPI())
    plugin = make_plugin([platform])
    plugin._notifier = FakeNotifier()

    run(plugin._probe_all_platforms())

    state = plugin._states["napcat-sync"]
    assert state.online is True
    assert state.self_id == "9527"
    assert platform.client.api.calls == ["get_status", "get_login_info"]


@pytest.mark.parametrize("body", ["", "not-json", "[]", "null", "{}"])
def test_feishu_notifier_rejects_malformed_success_response(body: str) -> None:
    class Response:
        status = 200

        async def text(self) -> str:
            return body

    class RequestContext:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Session:
        closed = False

        @staticmethod
        def post(_webhook: str, json: dict[str, Any]):
            assert json["msg_type"] == "interactive"
            return RequestContext()

    notifier = module.FeishuNotifier("https://example.invalid/hook", "", retries=0)
    notifier._session = Session()
    assert run(notifier.send("标题", "内容")) is False


def test_different_alert_keys_send_concurrently() -> None:
    class BlockingNotifier(FakeNotifier):
        def __init__(self) -> None:
            super().__init__()
            self.active = 0
            self.max_active = 0
            self.both_started = asyncio.Event()
            self.release = asyncio.Event()

        async def send(self, **kwargs) -> bool:
            self.calls.append(kwargs)
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            if self.active == 2:
                self.both_started.set()
            try:
                await self.release.wait()
                return True
            finally:
                self.active -= 1

    plugin = make_plugin()
    notifier = BlockingNotifier()
    plugin._notifier = notifier

    async def scenario() -> None:
        first = asyncio.create_task(
            plugin._alert("napcat-a", "offline", "标题A", "内容A", False, "red")
        )
        second = asyncio.create_task(
            plugin._alert("napcat-b", "offline", "标题B", "内容B", False, "red")
        )
        await asyncio.wait_for(notifier.both_started.wait(), timeout=0.2)
        notifier.release.set()
        assert await asyncio.gather(first, second) == [True, True]

    run(scenario())
    assert notifier.max_active == 2


def test_same_alert_key_is_deduplicated_while_sending() -> None:
    class BlockingNotifier(FakeNotifier):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def send(self, **kwargs) -> bool:
            self.calls.append(kwargs)
            self.started.set()
            await self.release.wait()
            return True

    plugin = make_plugin()
    notifier = BlockingNotifier()
    plugin._notifier = notifier

    async def scenario() -> None:
        first = asyncio.create_task(
            plugin._alert("napcat-a", "offline", "标题", "内容", False, "red")
        )
        await notifier.started.wait()
        duplicate = await asyncio.wait_for(
            plugin._alert("napcat-a", "offline", "标题", "内容", False, "red"),
            timeout=0.1,
        )
        notifier.release.set()
        assert duplicate is False
        assert await first is True

    run(scenario())
    assert len(notifier.calls) == 1


def test_feishu_notifier_real_aiohttp_session_round_trip() -> None:
    async def scenario() -> None:
        received: list[dict[str, Any]] = []

        async def handler(request):
            received.append(await request.json())
            return web.json_response({"code": 0})

        app = web.Application()
        app.router.add_post("/hook", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        site = web.SockSite(runner, sock)
        await site.start()
        notifier = module.FeishuNotifier(
            f"http://127.0.0.1:{port}/hook", "", timeout=2, retries=0
        )
        try:
            assert await notifier.send("本地联调", "通知链路正常", color="green")
            assert received[0]["msg_type"] == "interactive"
        finally:
            await notifier.close()
            await runner.cleanup()

    run(scenario())


def test_duplicate_platform_ids_get_stable_unique_state_keys() -> None:
    first = FakePlatform(
        "duplicate",
        {
            "get_status": [{"online": True, "good": True}],
            "get_login_info": [{"user_id": 10001}],
        },
    )
    second = FakePlatform(
        "duplicate",
        {
            "get_status": [{"online": True, "good": True}],
            "get_login_info": [{"user_id": 10002}],
        },
    )
    plugin = make_plugin([first, second])

    assert run(plugin._probe_all_platforms()) == 2
    first_key = plugin._platform_identity_keys[id(first)][1]
    second_key = plugin._platform_identity_keys[id(second)][1]
    assert first_key == "duplicate"
    assert second_key == "duplicate#2"
    assert plugin._states[first_key].self_id == "10001"
    assert plugin._states[second_key].self_id == "10002"

    plugin.context.platform_manager.platform_insts = [second, first]
    assert run(plugin._probe_all_platforms()) == 2
    assert plugin._platform_identity_keys[id(first)][1] == first_key
    assert plugin._platform_identity_keys[id(second)][1] == second_key
    assert plugin._states[first_key].success_count == 2
    assert plugin._states[second_key].success_count == 2


def test_recreated_platform_object_reuses_logical_state_key() -> None:
    original = FakePlatform(
        "napcat-a",
        {
            "get_status": [{"online": True, "good": True}],
            "get_login_info": [{"user_id": 10001}],
        },
    )
    plugin = make_plugin([original], failure_threshold=2)
    plugin._notifier = FakeNotifier()
    assert run(plugin._probe_all_platforms()) == 1

    plugin.context.platform_manager.platform_insts = []
    assert run(plugin._probe_all_platforms()) == 0
    assert plugin._states["napcat-a"].consecutive_failures == 1

    replacement = FakePlatform(
        "napcat-a",
        {
            "get_status": [{"online": True, "good": True}],
            "get_login_info": [{"user_id": 10002}],
        },
    )
    plugin.context.platform_manager.platform_insts = [replacement]
    assert run(plugin._probe_all_platforms()) == 1

    assert set(plugin._states) == {"napcat-a"}
    state = plugin._states["napcat-a"]
    assert state.online is True
    assert state.consecutive_failures == 0
    assert state.success_count == 2
    assert state.self_id == "10002"


def test_missing_platform_enters_degraded_then_offline_state() -> None:
    platform = FakePlatform(
        "napcat-a",
        {
            "get_status": [{"online": True, "good": True}],
            "get_login_info": [{"user_id": 12345}],
        },
    )
    plugin = make_plugin([platform], failure_threshold=2)
    notifier = FakeNotifier()
    plugin._notifier = notifier

    assert run(plugin._probe_all_platforms()) == 1
    plugin.context.platform_manager.platform_insts = []

    assert run(plugin._probe_all_platforms()) == 0
    state = plugin._states["napcat-a"]
    assert state.online is True
    assert state.consecutive_failures == 1
    assert "异常待确认" in run(plugin._build_status_text())

    assert run(plugin._probe_all_platforms()) == 0
    assert state.online is False
    assert state.consecutive_failures == 2
    assert "未出现在 AstrBot 平台管理器" in state.last_error
    assert len(notifier.calls) == 1
    assert notifier.calls[0]["color"] == "red"


def test_unexpected_notifier_exception_does_not_break_probe() -> None:
    class ExplodingNotifier(FakeNotifier):
        async def send(self, **kwargs) -> bool:
            self.calls.append(kwargs)
            raise ValueError("模拟通知器内部异常")

    platform = FakePlatform(
        "napcat-a",
        {"get_status": [RuntimeError("连接已断开")]},
    )
    plugin = make_plugin([platform], failure_threshold=1)
    notifier = ExplodingNotifier()
    plugin._notifier = notifier

    assert run(plugin._probe_all_platforms()) == 1
    assert plugin._states["napcat-a"].online is False
    assert len(notifier.calls) == 1
    assert any(
        level == "exception" and "通知器异常" in message
        for level, message in logger.records
    )
