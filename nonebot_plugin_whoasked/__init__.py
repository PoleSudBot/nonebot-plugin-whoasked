import asyncio
from collections.abc import Awaitable, Callable
from functools import wraps
import re
import sys
import time
import traceback
from typing import Any, Dict, List, Optional, Set, Union

from nonebot import get_driver, logger, on_command, on_keyword, on_message, require
from nonebot.adapters.onebot.v11 import (
    Bot,
    GroupMessageEvent,
    Message,
    MessageEvent,
    MessageSegment,
)
from nonebot.exception import FinishedException
from nonebot.matcher import Matcher
from nonebot.plugin import PluginMetadata
from nonebot.rule import Rule, to_me

# 先导入依赖
require("nonebot_plugin_localstore")
# 然后定义插件元数据

from .config import Config, get_plugin_config, plugin_config
from .data_manager import MessageRecorder
from .log_filter import setup_log_filter  # 导入日志过滤器设置函数

__plugin_meta__ = PluginMetadata(
    name="谁问你了？",
    description="查询谁@了你或引用了你的消息",
    usage="""
## ❓ 谁问你了

- **[回复某人消息] 谁问你了** / **whoasked** - 生成谁问你了表情包
""".strip(),
    type="application",
    homepage="https://github.com/enKl03B/nonebot-plugin-whoasked",
    supported_adapters={"~onebot.v11"},
    config=Config,
    extra={
        "author": "enKl03B",
        "version": "0.2.2",
        "menu_type": "一些工具",
    },
)

# --- 全局关闭状态标志 ---
_is_shutting_down = False

# 全局配置
global_config = get_driver().config

# 修改消息记录器初始化
message_recorder = None


async def init_message_recorder():
    global message_recorder
    if message_recorder is None:  # 避免重复初始化
        message_recorder = MessageRecorder()


# --- 定义关闭感知的 Rule ---
async def shutdown_aware_rule() -> bool:
    """如果程序正在关闭，则返回 False"""
    return not _is_shutting_down


# 在插件加载时初始化
from nonebot import get_driver

driver = get_driver()


@driver.on_startup
async def _startup():
    """插件启动时的操作"""
    global _is_shutting_down
    _is_shutting_down = False  # 确保启动时标志为 False
    # 初始化消息记录器
    await init_message_recorder()

    # 设置日志过滤器
    setup_log_filter()


@driver.on_shutdown
async def shutdown_hook():
    """在驱动器关闭时调用"""
    global _is_shutting_down
    logger.debug("检测到关闭信号，设置关闭标志...")
    _is_shutting_down = True  # 立即设置关闭标志

    # 等待一小段时间，让事件循环有机会处理完当前正在执行的任务
    # 并让 Rule 生效，阻止新的 Matcher 运行
    await asyncio.sleep(0.1)

    if message_recorder:
        await message_recorder.shutdown()  # 然后再执行数据保存等清理操作


# 关键词集合
QUERY_KEYWORDS = plugin_config.whoasked_keywords


# 修改错误处理装饰器
def catch_exception(func: Callable) -> Callable:
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except FinishedException:
            raise
        except Exception as e:
            logger.error(f"函数 {func.__name__} 执行出错: {e!s}")
            logger.error(traceback.format_exc())
            if len(args) > 0 and hasattr(args[0], "finish"):
                try:
                    await args[0].finish(f"处理命令时出错: {str(e)[:100]}")
                except FinishedException:
                    raise

    return wrapper


# --- 修改消息记录器注册，加入 Rule ---
record_msg = on_message(rule=shutdown_aware_rule, priority=1, block=False)  # 添加 rule


@record_msg.handle()
async def _handle_record_msg(bot: Bot, event: MessageEvent):
    """记录所有消息"""
    # 无需在此处再次检查 _is_shutting_down，因为 Rule 已经阻止了它
    if message_recorder is None:
        logger.warning("尝试记录消息时，MessageRecorder 未初始化。")
        return
    try:
        # 注意：即使 Rule 阻止了新的匹配，如果 handle 在 Rule 检查前已被调用，
        # 这里的 shutdown 检查仍然是有意义的，防止并发问题。
        if message_recorder._shutting_down:
            logger.debug("MessageRecorder 正在关闭，跳过内部记录逻辑")
            return
        await message_recorder.record_message(bot, event)
    except Exception as e:
        logger.error(f"记录消息失败: {e}")
        logger.error(traceback.format_exc())


# 修改命令处理器
who_at_me = on_command("谁问我了", aliases=QUERY_KEYWORDS, priority=50, block=True)


@who_at_me.handle()
@catch_exception
async def handle_who_at_me(bot: Bot, event: GroupMessageEvent):
    """处理查询@消息的指令"""
    if message_recorder is None:
        await who_at_me.finish("消息记录器未初始化，请稍后再试")
        return

    await process_query(bot, event, who_at_me)


# 修改查询处理函数
async def process_query(bot: Bot, event: GroupMessageEvent, matcher: Matcher):
    """处理查询@消息的请求"""
    user_id = str(event.user_id)
    current_group_id = str(event.group_id)

    try:
        async with asyncio.timeout(10):  # 增加10秒超时控制
            user_id = str(event.user_id)
            current_group_id = str(event.group_id)
        # 添加性能监控
        start_time = time.time()

        messages = await message_recorder.get_at_messages(user_id)

        if not messages:
            await matcher.finish("最近没人问你")
            return

        # 使用生成器表达式优化内存使用
        filtered_messages = (
            msg
            for msg in messages
            if msg.get("group_id") == current_group_id
            and (
                user_id in msg.get("at_list", [])
                or (msg.get("is_reply", False) and msg.get("reply_user_id") == user_id)
            )
        )

        # 转换为列表并检查是否为空
        filtered_messages = list(filtered_messages)
        if not filtered_messages:
            await matcher.finish("最近在本群没有人问你")
            return

        forward_messages = []

        # 定义格式化函数
        def format_message_content(msg_data):
            # 使用原始消息的发送者信息进行伪装
            node_content_message = Message()

            # 保留原始消息的发送者信息
            sender_name = msg_data.get("sender_name", "未知用户")
            sender_id = msg_data.get("user_id", "10000")

            isShowAvatar = plugin_config.whoasked_show_avatar
            if isShowAvatar:
                avatar_size = plugin_config.whoasked_show_avatar_size
                node_content_message.append(MessageSegment.text("该消息由"))
                node_content_message.append(MessageSegment.text(f"【{sender_name}】"))
                node_content_message.append(
                    MessageSegment.image(
                        file=f"https://q1.qlogo.cn/g?b=qq&nk={sender_id}&s={avatar_size}"
                    )
                )
                node_content_message.append(MessageSegment.text("发送"))

            # 处理消息内容
            if msg_data.get("is_reply", False):
                # 对于回复类型的消息
                node_content_message.append(MessageSegment.text("【引用了你的消息】\n"))

                # 处理原始消息内容，去除CQ码
                raw_message = msg_data["raw_message"]
                processed_message = re.sub(r"\[CQ:[^\]]+\]", "", raw_message)
                processed_message = re.sub(r"\s+", " ", processed_message).strip()

                node_content_message.append(MessageSegment.text(processed_message))

                # 添加被引用的消息内容
                if msg_data.get("replied_message_segments"):
                    node_content_message.append(MessageSegment.text("\n------\n"))
                    node_content_message.append(
                        MessageSegment.text("\n被引用的消息：\n")
                    )
                    # 处理被引用的消息内容
                    replied_message_segments = msg_data["replied_message_segments"]
                    # 将字典转换为 MessageSegment
                    replied_message = Message()
                    for segment in replied_message_segments:
                        replied_message.append(
                            MessageSegment(segment["type"], segment["data"])
                        )
                    replied_text = re.sub(r"\[CQ:[^\]]+\]", "", str(replied_message))
                    replied_text = re.sub(r"\s+", " ", replied_text).strip()
                    node_content_message.append(MessageSegment.text(replied_text))
            else:
                # 对于普通@消息
                node_content_message.append(MessageSegment.text("【@了你】\n"))

                raw_message = msg_data["raw_message"]
                processed_message = re.sub(r"\[CQ:[^\]]+\]", "", raw_message)
                processed_message = re.sub(r"\s+", " ", processed_message).strip()

                node_content_message.append(MessageSegment.text(processed_message))

            # 添加时间信息
            msg_time = msg_data["time"]
            time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg_time))
            time_passed = int(time.time()) - msg_time

            # 添加分割线
            node_content_message.append(MessageSegment.text("\n------\n"))

            # 计算时间间隔
            if time_passed < 60:
                elapsed = f"{time_passed}秒前"
            elif time_passed < 3600:
                elapsed = f"{time_passed // 60}分钟前"
            elif time_passed < 86400:
                elapsed = f"{time_passed // 3600}小时前"
            else:
                elapsed = f"{time_passed // 86400}天前"

            node_content_message.append(
                MessageSegment.text(f"\n📅消息发送时间： {time_str} ({elapsed})")
            )

            # 将 Message 对象序列化为 API 需要的列表格式
            node_content_serializable = [
                {"type": seg.type, "data": seg.data} for seg in node_content_message
            ]

            # 构建 node 消息
            node_content = {
                "type": "node",
                "data": {
                    "nickname": sender_name,  # 使用原始发送者昵称
                    "user_id": sender_id,  # 使用原始发送者ID
                    "content": node_content_serializable,
                },
            }

            return node_content

        # 构建转发消息节点
        for msg_data in filtered_messages:
            forward_messages.append(format_message_content(msg_data))

        # 添加性能日志
        logger.info(f"处理查询请求耗时: {time.time() - start_time:.2f}秒")

        await bot.call_api(
            "send_group_forward_msg", group_id=event.group_id, messages=forward_messages
        )
    except FinishedException:
        raise
    except Exception as e:
        logger.error(f"处理查询请求失败: {e}")
        logger.error(traceback.format_exc())
        try:
            await matcher.finish("查询失败，请稍后再试")
        except FinishedException:
            raise
