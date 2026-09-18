"""
AstrBot 定时消息轮播插件。

功能:
- 在指定时间向指定会话发送预设文本消息(每天 / 每周星期几 / 间隔 / cron / 一次性)
- 一个任务可配置多条消息, 触发时按顺序轮换发送
- 任务持久化保存, 重启后自动恢复调度
"""

import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register

try:
    from astrbot.core.utils.astrbot_path import get_astrbot_data_path
except ImportError:  # 兼容旧版本

    def get_astrbot_data_path() -> str:
        return "data"


try:
    from apscheduler.util import astimezone as _aps_astimezone
except ImportError:
    _aps_astimezone = None

PLUGIN_NAME = "astrbot_plugin_scheduled_sender"

_TIME_RE = re.compile(r"^([01]?\d|2[0-3]):([0-5]\d)$")
_DATE_RE = re.compile(r"^\d{4}-\d{1,2}-\d{1,2}$")
_INTERVAL_RE = re.compile(r"^每(\d{1,6})(秒钟?|分钟?|小时|时|天|日)$")

_UNIT_SECONDS = {
    "秒": 1,
    "秒钟": 1,
    "分": 60,
    "分钟": 60,
    "时": 3600,
    "小时": 3600,
    "天": 86400,
    "日": 86400,
}

_WEEKDAY_ALIASES = {
    "一": "mon",
    "二": "tue",
    "三": "wed",
    "四": "thu",
    "五": "fri",
    "六": "sat",
    "日": "sun",
    "天": "sun",
    "1": "mon",
    "2": "tue",
    "3": "wed",
    "4": "thu",
    "5": "fri",
    "6": "sat",
    "7": "sun",
}

_WEEKDAY_CN = {
    "mon": "一",
    "tue": "二",
    "wed": "三",
    "thu": "四",
    "fri": "五",
    "sat": "六",
    "sun": "日",
}

HELP_TEXT = (
    "📖 定时消息指令帮助\n"
    "\n"
    "/定时 添加 <时间> <消息1>|<消息2>|...\n"
    "    在当前会话新建任务, 多条消息轮换发送。\n"
    "    时间格式:\n"
    "      08:30               每天 08:30\n"
    "      每天@21:00          同上\n"
    "      周一三五@08:30      每周一/三/五 08:30\n"
    "                          (支持 周X/星期X/礼拜X/1-7, 逗号分隔)\n"
    "      工作日@09:00        周一至周五\n"
    "      周末@11:30          周六、周日\n"
    "      每30分钟            间隔重复(秒/分钟/小时/天)\n"
    "      cron 0 8 * * *      标准 cron(分 时 日 月 周)\n"
    "      2026-10-01 09:00    一次性, 发送后自动删除\n"
    "\n"
    "/定时 列表                本会话的任务\n"
    "/定时 详情 <ID>           任务详情\n"
    "/定时 消息 <ID> <消息1>|<消息2>  替换消息列表\n"
    "/定时 时间 <ID> <时间>    修改时间规则\n"
    "/定时 启用 <ID> / 停用 <ID>\n"
    "/定时 测试 <ID>           立即发送一条(轮换照常)\n"
    "/定时 删除 <ID>\n"
    "/定时 全部                (管理员)所有会话的任务"
)


@register(
    PLUGIN_NAME,
    "yourname",
    "定时发送预设文本消息, 支持多条消息轮换与每日/星期/间隔/cron 重复",
    "v1.0.0",
)
class ScheduledSenderPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config if isinstance(config, dict) else {}
        self._tasks: Dict[str, Dict] = {}
        self._scheduler: Optional[AsyncIOScheduler] = None
        self._tz_checked = False
        self._tz_value = None
        try:
            data_path = Path(get_astrbot_data_path())
        except Exception:
            data_path = Path("data")
        self._data_file = data_path / "plugin_data" / PLUGIN_NAME / "tasks.json"

    # ==================== 生命周期 ====================

    async def initialize(self):
        """插件加载时恢复任务并启动调度器。"""
        self._load()
        options: Dict = {}
        tz = self._tz()
        if tz is not None:
            options["timezone"] = tz
        self._scheduler = AsyncIOScheduler(**options)
        for task in self._tasks.values():
            self._register_job(task)
        self._scheduler.start()
        self._save()
        logger.info(f"[{PLUGIN_NAME}] 已加载 {len(self._tasks)} 个定时任务")

    async def terminate(self):
        """插件卸载/停用时关闭调度器。"""
        if self._scheduler is not None:
            try:
                self._scheduler.shutdown(wait=False)
            except Exception as e:
                logger.warning(f"[{PLUGIN_NAME}] 关闭调度器异常: {e!r}")
            self._scheduler = None

    # ==================== 配置与工具 ====================

    def _cfg_int(self, key: str, default: int) -> int:
        try:
            value = int(self.config.get(key, default))
            return value if value >= 0 else default
        except (TypeError, ValueError):
            return default

    def _tz(self):
        """把配置中的时区名转换为 APScheduler 可用的时区对象, 无效时回退本地时区。"""
        if self._tz_checked:
            return self._tz_value
        self._tz_checked = True
        self._tz_value = None
        if _aps_astimezone is not None:
            name = str(self.config.get("timezone") or "Asia/Shanghai").strip() or "Asia/Shanghai"
            try:
                self._tz_value = _aps_astimezone(name)
            except Exception:
                logger.warning(f"[{PLUGIN_NAME}] 时区 “{name}” 无效, 使用服务器本地时区")
        return self._tz_value

    @staticmethod
    def _human_seconds(seconds: int) -> str:
        if seconds % 86400 == 0:
            return f"{seconds // 86400} 天"
        if seconds % 3600 == 0:
            return f"{seconds // 3600} 小时"
        if seconds % 60 == 0:
            return f"{seconds // 60} 分钟"
        return f"{seconds} 秒"

    @staticmethod
    def _cn_weekdays(dow: str) -> str:
        if dow == "*":
            return "每天"
        if dow == "mon-fri":
            return "一至五"
        return ",".join(_WEEKDAY_CN.get(w, w) for w in dow.split(","))

    @staticmethod
    def _parse_weekdays(text: str) -> Optional[str]:
        t = text.strip().lower()
        if t in ("*", "每天", "每日", "everyday", "daily", "all"):
            return "*"
        if t in ("工作日", "weekday", "weekdays"):
            return "mon-fri"
        if t in ("周末", "weekend"):
            return "sat,sun"
        t = re.sub(r"^(每星期|每周|星期|礼拜|周)", "", t)
        names: List[str] = []
        for part in re.split(r"[,,、\s]+", t):
            for ch in part:
                name = _WEEKDAY_ALIASES.get(ch)
                if name is None:
                    return None
                if name not in names:
                    names.append(name)
        return ",".join(names) if names else None

    def _check_not_past(self, when: str) -> None:
        dt = datetime.strptime(when, "%Y-%m-%d %H:%M")
        tz = self._tz()
        try:
            if tz is not None:
                aware = tz.localize(dt) if hasattr(tz, "localize") else dt.replace(tzinfo=tz)
                ts = aware.timestamp()
            else:
                ts = dt.timestamp()
        except Exception:
            ts = dt.timestamp()
        if ts <= datetime.now().timestamp():
            raise ValueError(f"时间 {when} 已经过去了")

    # ==================== 时间解析 ====================

    def _parse_time_token(self, token: str) -> Tuple[Dict, str]:
        """解析单个 token 的时间: HH:MM / 周X@HH:MM / 每N分钟。返回 (触发器描述, 可读描述)。"""
        token = token.strip()
        if not token:
            raise ValueError("缺少时间参数")
        m = _INTERVAL_RE.match(token)
        if m:
            seconds = int(m.group(1)) * _UNIT_SECONDS[m.group(2)]
            min_seconds = self._cfg_int("min_interval_seconds", 60)
            if seconds < max(1, min_seconds):
                raise ValueError(f"间隔过短, 最小间隔为 {max(1, min_seconds)} 秒")
            return {"type": "interval", "seconds": seconds}, f"每 {self._human_seconds(seconds)}"
        m = _TIME_RE.match(token)
        if m:
            h, mi = int(m.group(1)), int(m.group(2))
            return {"type": "cron", "expr": f"{mi} {h} * * *"}, f"每天 {h:02d}:{mi:02d}"
        if "@" in token:
            left, _, right = token.partition("@")
            m = _TIME_RE.match(right.strip())
            if not m:
                raise ValueError(f"时间部分不合法: “{right}”, 应为 HH:MM, 例如 周一三五@08:30")
            dow = self._parse_weekdays(left)
            if not dow:
                raise ValueError(
                    f"星期部分不合法: “{left}”, 可用: 每天/工作日/周末/周一三五/1,3,5 等"
                )
            h, mi = int(m.group(1)), int(m.group(2))
            expr = f"{mi} {h} * * {dow}" if dow != "*" else f"{mi} {h} * * *"
            return {"type": "cron", "expr": expr}, f"每周[{self._cn_weekdays(dow)}] {h:02d}:{mi:02d}"
        raise ValueError(
            f"无法识别的时间格式: “{token}”。可用: 08:30 / 每天@08:30 / 周一三五@08:30 "
            "/ 每30分钟 / cron 0 8 * * * / 2026-10-01 08:30"
        )

    def _parse_schedule(self, rest: str) -> Tuple[Dict, str, str]:
        """解析 “<时间> <消息...>” 文本。返回 (触发器描述, 消息文本, 可读时间描述)。"""
        rest = rest.strip()
        if not rest:
            raise ValueError("缺少时间参数")
        tokens = rest.split()
        first = tokens[0]

        if first.lower() == "cron":
            if len(tokens) < 6:
                raise ValueError("cron 格式: /定时 添加 cron <分> <时> <日> <月> <周> <消息>")
            expr = " ".join(tokens[1:6])
            message = " ".join(tokens[6:]).strip()
            try:
                CronTrigger.from_crontab(expr)
            except Exception as e:
                raise ValueError(f"cron 表达式无效: {expr} ({e})")
            return {"type": "cron", "expr": expr}, message, f"cron({expr})"

        if _DATE_RE.match(first):
            if len(tokens) < 3:
                raise ValueError("一次性任务格式: /定时 添加 2026-10-01 08:30 消息内容")
            date_str, time_str = tokens[0], tokens[1]
            mt = _TIME_RE.match(time_str)
            if not mt:
                raise ValueError(f"时间不合法: “{time_str}”, 应为 HH:MM")
            when = f"{date_str} {int(mt.group(1)):02d}:{int(mt.group(2)):02d}"
            self._check_not_past(when)
            message = " ".join(tokens[2:]).strip()
            return {"type": "date", "when": when}, message, f"{when}(一次性)"

        store, desc = self._parse_time_token(first)
        message = rest[len(first):].strip()
        return store, message, desc

    def _build_trigger(self, store: Dict):
        ttype = store.get("type")
        tz = self._tz()
        if ttype == "cron":
            if tz is not None:
                return CronTrigger.from_crontab(store["expr"], timezone=tz)
            return CronTrigger.from_crontab(store["expr"])
        if ttype == "interval":
            return IntervalTrigger(seconds=int(store["seconds"]))
        if ttype == "date":
            run_date = datetime.strptime(store["when"], "%Y-%m-%d %H:%M")
            if tz is not None:
                return DateTrigger(run_date=run_date, timezone=tz)
            return DateTrigger(run_date=run_date)
        raise ValueError(f"未知的触发器类型: {ttype}")

    # ==================== 数据持久化 ====================

    def _load(self):
        self._tasks = {}
        try:
            if self._data_file.exists():
                data = json.loads(self._data_file.read_text(encoding="utf-8"))
                for task in data.get("tasks", []):
                    if isinstance(task, dict) and task.get("id"):
                        task.setdefault("index", 0)
                        task.setdefault("enabled", True)
                        self._tasks[task["id"]] = task
        except Exception as e:
            logger.error(f"[{PLUGIN_NAME}] 读取任务数据失败: {e!r}")

    def _save(self):
        try:
            self._data_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._data_file.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps({"version": 1, "tasks": list(self._tasks.values())},
                           ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp.replace(self._data_file)
        except Exception as e:
            logger.error(f"[{PLUGIN_NAME}] 保存任务数据失败: {e!r}")

    def _new_id(self) -> str:
        while True:
            task_id = uuid.uuid4().hex[:6]
            if task_id not in self._tasks:
                return task_id

    # ==================== 调度 ====================

    def _job_id(self, task_id: str) -> str:
        return f"{PLUGIN_NAME}:{task_id}"

    def _register_job(self, task: Dict) -> bool:
        if self._scheduler is None or not task.get("enabled", True):
            return False
        try:
            if task.get("trigger", {}).get("type") == "date":
                # 一次性任务: 时间已过则不再注册, 直接停用
                self._check_not_past(task["trigger"].get("when", ""))
            trigger = self._build_trigger(task["trigger"])
            self._scheduler.add_job(
                self._fire,
                trigger,
                id=self._job_id(task["id"]),
                args=[task["id"]],
                replace_existing=True,
                misfire_grace_time=self._cfg_int("misfire_grace_time", 300),
                coalesce=True,
                max_instances=1,
            )
            return True
        except ValueError as e:
            task["enabled"] = False
            self._remove_job(task["id"])
            logger.warning(f"[{PLUGIN_NAME}] 任务 {task['id']} 时间规则无效, 已自动停用: {e}")
            return False
        except Exception as e:
            logger.error(f"[{PLUGIN_NAME}] 注册任务 {task.get('id')} 失败: {e!r}")
            return False

    def _remove_job(self, task_id: str):
        if self._scheduler is None:
            return
        try:
            self._scheduler.remove_job(self._job_id(task_id))
        except Exception:
            pass

    def _next_run_str(self, task_id: str) -> Optional[str]:
        if self._scheduler is None:
            return None
        try:
            job = self._scheduler.get_job(self._job_id(task_id))
            if job is not None and job.next_run_time is not None:
                return job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        return None

    async def _send_task_message(self, task: Dict, manual: bool = False) -> Optional[str]:
        """发送任务的当前轮换消息, 推进轮换下标; 一次性任务触发后自动删除(手动测试除外)。"""
        messages = task.get("messages") or []
        if not messages:
            return None
        idx = int(task.get("index", 0) or 0) % len(messages)
        text = messages[idx]
        task["index"] = (idx + 1) % len(messages)
        session = task.get("session", "")
        try:
            ret = await self.context.send_message(session, MessageChain().message(text))
            if ret is False:
                logger.warning(
                    f"[{PLUGIN_NAME}] 任务 {task['id']} 消息发送失败, "
                    f"平台可能不支持主动发消息: {session}"
                )
            else:
                logger.info(f"[{PLUGIN_NAME}] 任务 {task['id']} 已发送至 {session}: {text[:50]}")
        except Exception as e:
            logger.error(f"[{PLUGIN_NAME}] 任务 {task['id']} 发送异常: {e!r}")
        if not manual and task.get("trigger", {}).get("type") == "date" and task.get("enabled", True):
            self._remove_job(task["id"])
            self._tasks.pop(task["id"], None)
        self._save()
        return text

    async def _fire(self, task_id: str):
        task = self._tasks.get(task_id)
        if not task or not task.get("enabled", True):
            return
        try:
            await self._send_task_message(task)
        except Exception as e:
            logger.error(f"[{PLUGIN_NAME}] 任务 {task_id} 执行异常: {e!r}")

    # ==================== 权限与解析辅助 ====================

    def _can_manage(self, event: AstrMessageEvent) -> bool:
        if not self.config.get("admin_only", False):
            return True
        try:
            return bool(event.is_admin())
        except Exception:
            return False

    def _task_accessible(self, event: AstrMessageEvent, task: Dict) -> bool:
        try:
            if event.unified_msg_origin == task.get("session"):
                return True
        except Exception:
            pass
        try:
            return bool(event.is_admin())
        except Exception:
            return False

    def _find_task(self, task_id_input: str) -> Optional[Dict]:
        t = task_id_input.strip().lower()
        if not t:
            return None
        if t in self._tasks:
            return self._tasks[t]
        matches = [v for k, v in self._tasks.items() if k.startswith(t)]
        return matches[0] if len(matches) == 1 else None

    @staticmethod
    def _rest(event: AstrMessageEvent, *keywords: str) -> str:
        """从消息文本中截取子指令之后的内容(按 token 精确匹配, 避免子串误伤)。"""
        text = (getattr(event, "message_str", "") or "").strip()
        tokens = text.split()
        kws = {k.lower() for k in keywords}
        for i, token in enumerate(tokens):
            clean = token.strip().lstrip("/!#").rstrip(",.，;；:：!！?？").lower()
            if clean in kws:
                return " ".join(tokens[i + 1:])
        return ""

    # ==================== 展示 ====================

    def _format_task_brief(self, task: Dict) -> str:
        enabled = task.get("enabled", True)
        parts = [
            f"[{task['id']}] {task.get('time_desc', '')}",
            "✅启用" if enabled else "⏸停用",
            f"{len(task.get('messages', []))}条消息",
        ]
        if enabled:
            nxt = self._next_run_str(task["id"])
            if nxt:
                parts.append(f"下次 {nxt}")
        return " | ".join(parts)

    def _format_task_detail(self, task: Dict) -> str:
        messages = task.get("messages", [])
        current = int(task.get("index", 0) or 0) % max(len(messages), 1)
        lines = [
            f"任务 ID: {task['id']}",
            f"时间规则: {task.get('time_desc', '')}",
            f"状态: {'启用' if task.get('enabled', True) else '停用'}",
            f"会话: {task.get('session', '')}",
            f"创建者: {task.get('created_by') or '未知'}  创建于 {task.get('created_at', '')}",
            f"消息轮换(共 {len(messages)} 条, 下次发送第 {current + 1} 条):",
        ]
        for i, msg in enumerate(messages):
            mark = "▶" if i == current else " "
            lines.append(f" {mark} {i + 1}. {msg}")
        if task.get("enabled", True):
            nxt = self._next_run_str(task["id"])
            if nxt:
                lines.append(f"下次发送: {nxt}")
        return "\n".join(lines)

    # ==================== 指令 ====================

    @filter.command_group("定时", alias={"定时消息", "定时任务"})
    def scheduled_task(self):
        """定时消息指令组, 发送 /定时 帮助 查看用法"""
        pass

    @scheduled_task.command("帮助", alias={"help"})
    async def task_help(self, event: AstrMessageEvent):
        """查看定时消息插件帮助"""
        yield event.plain_result(HELP_TEXT)

    @scheduled_task.command("添加", alias={"add", "new"})
    async def add_task(self, event: AstrMessageEvent):
        """添加定时任务: /定时 添加 <时间> <消息1>|<消息2>|..."""
        if not self._can_manage(event):
            yield event.plain_result("当前配置仅允许管理员管理定时任务。")
            return
        rest = self._rest(event, "添加", "新增", "add", "new")
        if not rest:
            yield event.plain_result(
                "用法: /定时 添加 <时间> <消息1>|<消息2>|...\n"
                "例: /定时 添加 每天@08:30 早安|早上好\n"
                "发送 /定时 帮助 查看全部时间格式。"
            )
            return
        try:
            store, message_part, desc = self._parse_schedule(rest)
            trigger = self._build_trigger(store)
        except ValueError as e:
            yield event.plain_result(f"参数错误: {e}")
            return
        except Exception as e:
            yield event.plain_result(f"时间规则无法解析: {e!r}")
            return
        messages = [m.strip() for m in message_part.split("|") if m.strip()]
        if not messages:
            yield event.plain_result("请至少提供一条消息内容, 多条消息用 | 分隔。")
            return
        session = event.unified_msg_origin
        limit = self._cfg_int("max_tasks_per_session", 30)
        if sum(1 for t in self._tasks.values() if t.get("session") == session) >= limit:
            yield event.plain_result(f"本会话任务数已达上限({limit})。")
            return
        task_id = self._new_id()
        task = {
            "id": task_id,
            "session": session,
            "trigger": store,
            "time_desc": desc,
            "messages": messages,
            "index": 0,
            "enabled": True,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "created_by": event.get_sender_id(),
        }
        self._tasks[task_id] = task
        self._register_job(task)
        self._save()
        nxt = self._next_run_str(task_id)
        reply = (
            f"✅ 任务 {task_id} 已创建: {desc}\n"
            f"共 {len(messages)} 条消息轮换发送, 目标会话: {session}\n"
            f"第 1 条: {messages[0]}"
        )
        if nxt:
            reply += f"\n下次发送: {nxt}"
        yield event.plain_result(reply)

    @scheduled_task.command("列表", alias={"list", "ls"})
    async def list_tasks(self, event: AstrMessageEvent):
        """查看本会话的定时任务列表"""
        tasks = [t for t in self._tasks.values() if t.get("session") == event.unified_msg_origin]
        if not tasks:
            yield event.plain_result("本会话还没有定时任务, 发送 /定时 帮助 查看用法。")
            return
        lines = [f"本会话定时任务({len(tasks)} 个):"]
        lines.extend(self._format_task_brief(t) for t in tasks)
        yield event.plain_result("\n".join(lines))

    @scheduled_task.command("全部", alias={"all"})
    async def list_all_tasks(self, event: AstrMessageEvent):
        """(管理员)查看所有会话的定时任务"""
        if not self._can_manage(event) or not event.is_admin():
            yield event.plain_result("该指令仅管理员可用。")
            return
        if not self._tasks:
            yield event.plain_result("当前没有任何定时任务。")
            return
        by_session: Dict[str, List[Dict]] = {}
        for t in self._tasks.values():
            by_session.setdefault(t.get("session", "?"), []).append(t)
        lines = [f"全部定时任务({len(self._tasks)} 个):"]
        for session, tasks in by_session.items():
            lines.append(f"▸ {session}")
            lines.extend(f"  {self._format_task_brief(t)}" for t in tasks)
        yield event.plain_result("\n".join(lines))

    @scheduled_task.command("详情", alias={"info", "detail", "查看"})
    async def task_detail(self, event: AstrMessageEvent):
        """查看任务详情: /定时 详情 <ID>"""
        rest = self._rest(event, "详情", "info", "detail", "查看")
        task = self._find_task(rest) if rest else None
        if task is None:
            yield event.plain_result("用法: /定时 详情 <ID>, ID 可通过 /定时 列表 查看。")
            return
        if not self._task_accessible(event, task):
            yield event.plain_result("无权查看该任务。")
            return
        yield event.plain_result(self._format_task_detail(task))

    @scheduled_task.command("消息", alias={"msg", "message"})
    async def set_messages(self, event: AstrMessageEvent):
        """替换任务消息: /定时 消息 <ID> <消息1>|<消息2>|..."""
        if not self._can_manage(event):
            yield event.plain_result("当前配置仅允许管理员管理定时任务。")
            return
        rest = self._rest(event, "消息", "msg", "message")
        tokens = rest.split(None, 1)
        if not tokens:
            yield event.plain_result("用法: /定时 消息 <ID> <消息1>|<消息2>|...")
            return
        task = self._find_task(tokens[0])
        if task is None:
            yield event.plain_result(f"未找到任务 {tokens[0]}, ID 可通过 /定时 列表 查看。")
            return
        if not self._task_accessible(event, task):
            yield event.plain_result("无权操作该任务。")
            return
        content = tokens[1] if len(tokens) > 1 else ""
        messages = [m.strip() for m in content.split("|") if m.strip()]
        if not messages:
            yield event.plain_result("请至少提供一条消息内容, 多条消息用 | 分隔。")
            return
        task["messages"] = messages
        task["index"] = min(int(task.get("index", 0) or 0), len(messages) - 1)
        self._save()
        yield event.plain_result(
            f"✅ 任务 {task['id']} 消息已更新, 共 {len(messages)} 条轮换:\n"
            + "\n".join(f" {i + 1}. {m}" for i, m in enumerate(messages))
        )

    @scheduled_task.command("时间", alias={"time"})
    async def set_time(self, event: AstrMessageEvent):
        """修改任务时间: /定时 时间 <ID> <时间>"""
        if not self._can_manage(event):
            yield event.plain_result("当前配置仅允许管理员管理定时任务。")
            return
        rest = self._rest(event, "时间", "time")
        tokens = rest.split(None, 1)
        if not tokens:
            yield event.plain_result("用法: /定时 时间 <ID> <时间>, 时间格式见 /定时 帮助")
            return
        task = self._find_task(tokens[0])
        if task is None:
            yield event.plain_result(f"未找到任务 {tokens[0]}, ID 可通过 /定时 列表 查看。")
            return
        if not self._task_accessible(event, task):
            yield event.plain_result("无权操作该任务。")
            return
        spec = tokens[1] if len(tokens) > 1 else ""
        try:
            store, _, desc = self._parse_schedule(spec)
            self._build_trigger(store)
        except ValueError as e:
            yield event.plain_result(f"参数错误: {e}")
            return
        except Exception as e:
            yield event.plain_result(f"时间规则无法解析: {e!r}")
            return
        task["trigger"] = store
        task["time_desc"] = desc
        self._register_job(task)
        self._save()
        nxt = self._next_run_str(task["id"])
        reply = f"✅ 任务 {task['id']} 时间已更新: {desc}"
        if task.get("enabled", True) and nxt:
            reply += f"\n下次发送: {nxt}"
        yield event.plain_result(reply)

    @scheduled_task.command("启用", alias={"on", "enable"})
    async def enable_task(self, event: AstrMessageEvent):
        """启用任务: /定时 启用 <ID>"""
        if not self._can_manage(event):
            yield event.plain_result("当前配置仅允许管理员管理定时任务。")
            return
        rest = self._rest(event, "启用", "on", "enable")
        task = self._find_task(rest) if rest else None
        if task is None:
            yield event.plain_result("用法: /定时 启用 <ID>")
            return
        if not self._task_accessible(event, task):
            yield event.plain_result("无权操作该任务。")
            return
        task["enabled"] = True
        ok = self._register_job(task)
        self._save()
        if ok:
            nxt = self._next_run_str(task["id"])
            reply = f"✅ 任务 {task['id']} 已启用"
            if nxt:
                reply += f", 下次发送: {nxt}"
            yield event.plain_result(reply)
        else:
            yield event.plain_result(
                f"⚠️ 任务 {task['id']} 已标记启用, 但调度注册失败, 请检查时间规则(详见日志)。"
            )

    @scheduled_task.command("停用", alias={"off", "disable"})
    async def disable_task(self, event: AstrMessageEvent):
        """停用任务: /定时 停用 <ID>"""
        if not self._can_manage(event):
            yield event.plain_result("当前配置仅允许管理员管理定时任务。")
            return
        rest = self._rest(event, "停用", "off", "disable")
        task = self._find_task(rest) if rest else None
        if task is None:
            yield event.plain_result("用法: /定时 停用 <ID>")
            return
        if not self._task_accessible(event, task):
            yield event.plain_result("无权操作该任务。")
            return
        task["enabled"] = False
        self._remove_job(task["id"])
        self._save()
        yield event.plain_result(f"⏸ 任务 {task['id']} 已停用, 发送 /定时 启用 {task['id']} 可恢复。")

    @scheduled_task.command("删除", alias={"del", "delete", "remove", "rm"})
    async def delete_task(self, event: AstrMessageEvent):
        """删除任务: /定时 删除 <ID>"""
        if not self._can_manage(event):
            yield event.plain_result("当前配置仅允许管理员管理定时任务。")
            return
        rest = self._rest(event, "删除", "del", "delete", "remove", "rm")
        task = self._find_task(rest) if rest else None
        if task is None:
            yield event.plain_result("用法: /定时 删除 <ID>")
            return
        if not self._task_accessible(event, task):
            yield event.plain_result("无权操作该任务。")
            return
        task_id = task["id"]
        self._remove_job(task_id)
        self._tasks.pop(task_id, None)
        self._save()
        yield event.plain_result(f"🗑 任务 {task_id} 已删除。")

    @scheduled_task.command("测试", alias={"test"})
    async def test_task(self, event: AstrMessageEvent):
        """立即发送一条测试消息: /定时 测试 <ID>"""
        if not self._can_manage(event):
            yield event.plain_result("当前配置仅允许管理员管理定时任务。")
            return
        rest = self._rest(event, "测试", "test")
        task = self._find_task(rest) if rest else None
        if task is None:
            yield event.plain_result("用法: /定时 测试 <ID>")
            return
        if not self._task_accessible(event, task):
            yield event.plain_result("无权操作该任务。")
            return
        sent = await self._send_task_message(task, manual=True)
        if sent is None:
            yield event.plain_result("该任务没有可发送的消息。")
        else:
            yield event.plain_result(f"🧪 已向任务目标会话发送: {sent}")
