# TASK.md — AstrBot Bilibili VTuber Monitor

## 0. 任务目标

实现一个 AstrBot 插件 `astrbot_plugin_vtuber_monitor`，面向 Bilibili 虚拟主播（VTuber）提供：

1. **主播订阅管理**
   - 支持订阅 / 取消订阅指定主播。
   - 订阅分为两档：
     - `normal`：普通关注。
     - `special`：特别关注。
   - 普通关注：仅维护直播状态，并在上下播时按配置推送。
   - 特别关注：除直播状态外，还维护该主播本周直播计划（周表）。

2. **特别关注周表维护**
   - 监控特别关注主播的动态。
   - 检测周表更新动态。
   - 抓取周表图片。
   - 解析图片为结构化本周直播日程。
   - 用解析结果替换 / 更新本地主播周表。
   - 保存来源动态 ID、周表图片、解析时间等元信息。

3. **临时调播识别与受控修改**
   - 对特别关注主播的新动态进行正则 / 规则预筛选。
   - 如果疑似包含“延期、提前、改期、取消、追加直播”等调度信息，则交给 LLM 进一步理解。
   - **LLM 不允许直接修改数据库或 JSON。**
   - LLM 只能调用受控 Tool，例如：
     - `reschedule_stream`
     - `cancel_stream`
     - `add_stream`
     - `update_stream_info`
   - Tool 再调用 `ScheduleService` 修改本地结构化周表。

4. **主动推送**
   - 特别关注主播上播时推送。
   - 特别关注主播下播时推送。
   - 周表更新时可推送摘要。
   - 调播生效后可推送变更前后信息。

---

# 1. 总体架构

必须按照以下职责边界实现，不要把所有逻辑写进 `main.py`：

```text
Bilibili
   ↓
BiliClient
   ↓
Listeners
├── LiveListener
└── DynamicListener
   ↓
Services
├── SubscriptionService
├── ScheduleService
├── ScheduleParser
└── AdjustmentAgent
   ↓
DataManager
   ↓
Dispatcher
   ↓
AstrBot context.send_message()
   ↓
QQ / 其他平台
```

临时调播处理链必须为：

```text
动态文本
   ↓
Regex / Rule Candidate Filter
   ↓
AdjustmentAgent
   ↓
LLM
   ↓
Tool Call
   ↓
ScheduleService
   ↓
DataManager
```

禁止：

```text
LLM
↓
直接改 JSON / SQLite
```

---

# 2. 推荐目录结构

```text
astrbot_plugin_vtuber_monitor/
├── main.py
├── metadata.yaml
├── _conf_schema.json
├── requirements.txt
│
├── bili_client.py
│
├── core/
│   ├── models.py
│   ├── constants.py
│   ├── data_manager.py
│   └── utils.py
│
├── services/
│   ├── subscription_service.py
│   ├── live_listener.py
│   ├── dynamic_listener.py
│   ├── schedule_service.py
│   ├── schedule_parser.py
│   ├── adjustment_agent.py
│   └── dispatcher.py
│
├── tools/
│   └── schedule_tools.py
│
└── tests/
    ├── test_subscription_service.py
    ├── test_schedule_service.py
    ├── test_schedule_parser.py
    ├── test_adjustment_rules.py
    └── fixtures/
```

---

# 3. 开发原则

Agent 实现时必须遵守：

- 网络请求使用 `httpx` 或 `aiohttp`，不要使用同步 `requests`。
- 所有后台任务必须支持 `cancel()`，并在插件 `terminate()` 中正确停止。
- 持久化数据不得保存在插件源码目录。
- 小型状态可使用 AstrBot Plugin KV。
- 周表图片、缓存和较大文件存储到：
  `data/plugin_data/astrbot_plugin_vtuber_monitor/`
- 所有 Bilibili API 调用必须：
  - timeout
  - 异常捕获
  - 重试 / backoff
  - 日志
- 不要因为单个主播请求失败导致整个监听任务退出。
- 所有外部数据进入 Service 前先验证。
- Tool 不允许绕过 `ScheduleService`。
- `main.py` 只负责组件组装、命令注册和生命周期管理。

参考：

- AstrBot 插件开发指南  
  https://github.com/AstrBotDevs/AstrBot/wiki/zh-dev-star-plugin-new
- AstrBot 插件存储  
  https://github.com/AstrBotDevs/AstrBot/wiki/en-dev-star-guides-storage

---

# 4. Phase 1 — 建立最小插件骨架

## 4.1 目标

确认：

```text
AstrBot
→ 加载插件
→ QQ 命令
→ 插件响应
```

## 4.2 实现

`main.py` 只添加：

```text
/vt_ping
```

返回：

```text
VTuber Monitor OK
```

## 4.3 验收

- 插件出现在 AstrBot `/plugin` 列表。
- `/vt_ping` 能正常回复。
- 插件 reload 不报错。
- `terminate()` 能正常执行。

参考：

- AstrBot 插件开发指南  
  https://github.com/AstrBotDevs/AstrBot/wiki/zh-dev-star-plugin-new

---

# 5. Phase 2 — 定义领域模型

在 `core/models.py` 定义结构化模型。

## 5.1 FollowLevel

```python
class FollowLevel(str, Enum):
    NORMAL = "normal"
    SPECIAL = "special"
```

## 5.2 Subscription

字段：

```text
uid
umo
level
created_at
updated_at
```

`umo` 保存 AstrBot `event.unified_msg_origin`。

## 5.3 VtuberState

字段：

```text
uid
name
room_id
is_live
latest_dynamic_id
weekly_schedule
last_live_change_at
```

## 5.4 WeeklySchedule

字段：

```text
uid
week_start
source_dynamic_id
source_image_url
parsed_at
updated_at
streams[]
```

## 5.5 StreamPlan

字段：

```text
id
date
start_time
title
status
source
revision
```

`status` 至少支持：

```text
scheduled
postponed
cancelled
completed
unknown
```

`source` 至少支持：

```text
weekly_image
dynamic_adjustment
manual
```

## 验收

- 模型能 JSON 序列化。
- 单元测试覆盖构造、序列化、反序列化。

---

# 6. Phase 3 — DataManager

## 6.1 目标

DataManager 是唯一持久化入口。

不要让 Listener / Agent / Tool 直接操作文件或数据库。

## 6.2 API

至少实现：

```python
add_subscription(...)
remove_subscription(...)
get_subscription(...)
get_subscriptions_by_umo(...)
get_subscriptions_by_uid(...)
get_special_vtubers(...)

get_vtuber_state(uid)
save_vtuber_state(state)

get_weekly_schedule(uid)
save_weekly_schedule(schedule)

append_schedule_revision(...)
```

## 6.3 存储策略

V0.1：

- AstrBot KV：
  - 简单状态
  - last_dynamic_id
  - listener checkpoint

建议 V0.2 改 SQLite：

```text
subscriptions
vtubers
weekly_schedules
stream_plans
schedule_revisions
```

参考：

- AstrBot Plugin KV / plugin_data  
  https://github.com/AstrBotDevs/AstrBot/wiki/en-dev-star-guides-storage
- `astrbot_plugin_bilibili` 的 DataManager 分层思路  
  https://github.com/Soulter/astrbot_plugin_bilibili

---

# 7. Phase 4 — SubscriptionService

## 7.1 命令

实现：

```text
/vt_sub <uid> [normal|special]
/vt_unsub <uid>
/vt_follow_level <uid> <normal|special>
/vt_list
```

## 7.2 行为

`/vt_sub`：

1. 校验 UID。
2. 调 BiliClient 获取主播信息。
3. 保存 `event.unified_msg_origin`。
4. 保存 follow level。
5. 如果为 special：
   - 初始化动态检查状态。
   - 尝试加载当前周表。

## 7.3 重要规则

不要在命令 Handler 内直接写数据库。

命令只调用：

```text
SubscriptionService
```

参考：

- `astrbot_plugin_bilibili` 中 `SubscriptionService`、UMO 和订阅命令的封装方式  
  https://github.com/Soulter/astrbot_plugin_bilibili/blob/master/main.py

---

# 8. Phase 5 — BiliClient

## 8.1 目标

把所有 Bilibili HTTP 请求封装到 `bili_client.py`。

其他模块禁止直接拼 URL。

## 8.2 初始接口

至少提供：

```python
get_user_info(uid)
get_live_room_info(uid)
get_latest_dynamics(uid)
download_image(url)
```

后续可增加：

```python
get_pinned_dynamic(uid)
```

## 8.3 可靠性

每个请求：

```text
timeout
异常捕获
返回值校验
错误日志
退避
```

必须考虑 Bilibili 风控：

```text
412
-352
登录凭据失效
匿名请求限制
```

参考：

- `astrbot_plugin_bilibili` 的 `BiliClient`、凭据维护、动态监听实现  
  https://github.com/Soulter/astrbot_plugin_bilibili

---

# 9. Phase 6 — LiveListener

## 9.1 目标

监听所有需要直播状态通知的主播。

不要每轮推送状态，只推送状态变化：

```text
offline → live
live → offline
```

## 9.2 流程

```text
获取所有订阅 UID
↓
BiliClient.get_live_room_info()
↓
比较 VtuberState.is_live
↓
变化？
├─ 否：忽略
└─ 是：
   ├─ 更新状态
   └─ Dispatcher 推送
```

## 9.3 普通 / 特别关注

默认：

- normal：可推上下播。
- special：必须推上下播。

如果产品需求最终决定普通关注不推下播，可做配置项。

## 9.4 验收

模拟：

```text
offline
→ live
→ live
→ offline
```

只应产生两次通知。

---

# 10. Phase 7 — Dispatcher

## 10.1 目标

所有主动通知由一个模块处理。

接口：

```python
push_live_started(...)
push_live_ended(...)
push_schedule_updated(...)
push_schedule_adjusted(...)
```

## 10.2 AstrBot 主动推送

保存：

```python
event.unified_msg_origin
```

主动发送：

```python
await context.send_message(umo, message_chain)
```

不要写 QQ Official 专属发送逻辑。

参考：

- AstrBot 主动消息文档  
  https://github.com/AstrBotDevs/AstrBot/wiki/zh-dev-star-guides-send-message
- `astrbot_plugin_bilibili` 中 Dispatcher 的分层思路  
  https://github.com/Soulter/astrbot_plugin_bilibili

---

# 11. Phase 8 — DynamicListener（仅特别关注）

## 11.1 目标

只对 `special` 主播扫描动态。

## 11.2 每轮逻辑

```text
获取 special UID
↓
获取最新动态
↓
根据 dynamic_id 去重
↓
新动态
↓
分类
├─ 周表候选
├─ 调播候选
└─ 普通动态
```

## 11.3 去重

必须持久化：

```text
last_dynamic_id
或
recent_dynamic_ids
```

重启 AstrBot 后不得重复推送旧动态。

参考：

- `astrbot_plugin_bilibili` 的 `DynamicListener`
- recent dynamic cache / reconnect silent / background task 处理  
  https://github.com/Soulter/astrbot_plugin_bilibili

---

# 12. Phase 9 — 周表候选识别

## 12.1 初版策略

不要一开始完全依赖 LLM。

优先使用：

```text
置顶动态
图片数量
正文关键词
时间规律
```

候选关键词示例：

```text
周表
本周
schedule
直播安排
本周安排
```

## 12.2 处理

命中候选：

```text
动态
↓
找到图片
↓
下载到 plugin_data
↓
ScheduleParser.parse()
```

保存：

```text
source_dynamic_id
source_image_url
local_image_path
```

---

# 13. Phase 10 — ScheduleParser

## 13.1 接口

```python
async def parse(
    uid: int,
    image_path: str,
) -> WeeklySchedule
```

## 13.2 实现建议

分两步：

### V0.1
视觉 LLM 直接解析图片：

输出严格 JSON：

```json
{
  "week_start": "2026-09-28",
  "streams": [
    {
      "date": "2026-09-28",
      "start_time": "20:00",
      "title": "歌回"
    }
  ]
}
```

### V0.2
OCR + LLM：

```text
OCR
↓
原始文字
↓
LLM 结构化
```

## 13.3 校验

解析后必须校验：

- 日期在合理范围。
- 时间合法。
- 不允许重复 stream id。
- 不允许空周表直接覆盖已有有效周表。

---

# 14. Phase 11 — ScheduleService

这是插件最重要的业务层。

## 14.1 唯一修改入口

所有周表修改只能通过：

```python
replace_weekly_schedule(...)
reschedule_stream(...)
cancel_stream(...)
add_stream(...)
update_stream_info(...)
```

## 14.2 必须完成的职责

每次修改：

```text
参数验证
↓
定位目标直播
↓
保存旧值
↓
执行修改
↓
revision + 1
↓
写 revision history
↓
持久化
```

禁止 Tool 和 Agent 直接修改数据结构。

## 14.3 Revision

建议：

```text
schedule_revisions
├── uid
├── stream_id
├── old_value
├── new_value
├── reason
├── source_dynamic_id
└── created_at
```

---

# 15. Phase 12 — 调播候选预筛选

在 `DynamicListener` 中只做候选识别。

Regex 示例：

```text
改到
改为
改成
延期
推迟
提前
顺延
取消
鸽
补播
加播
临时
```

不要试图仅靠 Regex 完整理解动态。

逻辑：

```text
不命中
→ 不调用 LLM

命中
→ AdjustmentAgent
```

目标是减少 LLM 调用量和误判面。

---

# 16. Phase 13 — Schedule Tools

在 `tools/schedule_tools.py` 定义受控工具。

必须至少实现：

```text
reschedule_stream
cancel_stream
add_stream
update_stream_info
```

每个 Tool 只能调用：

```text
ScheduleService
```

例如：

```text
reschedule_stream
↓
ScheduleService.reschedule_stream()
```

Tool 参数必须结构化，禁止接收“任意 JSON patch”。

参考：

- AstrBot `FunctionTool` / `ToolSet`  
  https://github.com/AstrBotDevs/AstrBot/blob/master/astrbot/core/agent/tool.py
- AstrBot Function Calling 说明  
  https://github.com/AstrBotDevs/AstrBot/wiki/zh-use-function-calling

---

# 17. Phase 14 — AdjustmentAgent

## 17.1 输入

提供给 LLM：

```text
主播 UID / 名称
当前日期
当前 WeeklySchedule
新动态正文
动态发布时间
source_dynamic_id
可用 Tools
```

## 17.2 System Prompt 核心约束

必须明确：

```text
你负责判断动态是否修改本周直播计划。

你不能直接修改本地数据。

只有在动态明确表达新增、取消、延期、提前或其他排期变更时，
才能调用提供的工具。

信息不足时，不调用工具。

不能猜测具体日期或时间。
```

## 17.3 调用权限

不要把这些 Schedule Tool 注册给普通聊天 Agent。

只将 ToolSet 提供给：

```text
AdjustmentAgent
```

以缩小权限范围。

## 17.4 Tool 执行后

返回：

```text
修改是否成功
原计划
新计划
reason
```

之后调用 Dispatcher 推送。

参考：

- AstrBot `ToolSet`
  https://github.com/AstrBotDevs/AstrBot/blob/master/astrbot/core/agent/tool.py

---

# 18. Phase 15 — 周表替换策略

新周表出现时不要直接覆盖。

执行：

```text
解析新周表
↓
validate
↓
与旧周表 diff
↓
生成差异
↓
ScheduleService.replace_weekly_schedule()
↓
保存 revision
```

Diff 至少输出：

```text
新增直播
删除直播
时间变化
标题变化
```

如果解析结果异常：

```text
不要覆盖旧周表
记录错误
保留原图
等待人工检查
```

---

# 19. Phase 16 — Debug / 测试命令

开发阶段必须提供：

```text
/vt_ping
/vt_latest <uid>
/vt_live <uid>
/vt_schedule <uid>
/vt_parse_schedule <uid>
/vt_adjust_test <uid> <text>
/vt_push_test
/vt_status
```

用途：

```text
vt_latest
→ 测 Bilibili API

vt_parse_schedule
→ 测图片解析

vt_adjust_test
→ 测 Regex + LLM + Tool

vt_push_test
→ 测主动推送

vt_status
→ 查看 listener / checkpoint
```

不要依赖主播真实发动态才能调试。

---

# 20. Phase 17 — 单元测试

## 必须覆盖

### Data

- subscription CRUD
- follow level change
- schedule serialization

### ScheduleService

- 正常改期
- 不存在 stream
- 取消直播
- 新增直播
- revision 记录
- 非法时间拒绝

### Dynamic

- 重复 dynamic_id
- 多条新动态
- 非调播动态
- 调播候选

### Listener

模拟：

```text
Bilibili timeout
412
JSON 结构缺失
单主播失败
```

保证 listener 不退出。

### ScheduleParser

保存真实周表图片 / 模拟 JSON fixture。

---

# 21. Phase 18 — 集成测试

测试顺序：

```text
1. /vt_ping
2. /vt_latest UID
3. /vt_sub UID normal
4. /vt_sub UID special
5. 模拟上播
6. 模拟下播
7. 解析一张周表
8. 保存 WeeklySchedule
9. 用测试动态触发调播
10. LLM 选择 Tool
11. Tool 修改 ScheduleService
12. QQ 收到主动通知
```

全部通过后再启用真实后台轮询。

---

# 22. Phase 19 — 后台任务生命周期

`main.py`：

```text
_start_tasks()
```

创建：

```text
live_listener_task
dynamic_listener_task
credential_maintenance_task（如有）
```

`terminate()` 必须：

```text
cancel()
await task
catch asyncio.CancelledError
```

插件 reload 后不得残留旧 task。

参考：

- `astrbot_plugin_bilibili` 的 task 启动 / terminate 实现  
  https://github.com/Soulter/astrbot_plugin_bilibili/blob/master/main.py

---

# 23. Phase 20 — 配置

`_conf_schema.json` 至少提供：

```text
live_poll_interval
dynamic_poll_interval
bilibili_sessdata / credential
schedule_keywords
adjustment_regex
enable_schedule_push
enable_adjustment_push
request_timeout
max_retry
```

secret 配置必须标记为 secret。

---

# 24. MVP 优先级

Agent 不要一次实现所有功能。

严格按以下顺序：

## V0.1

```text
/vt_ping
/vt_sub
/vt_unsub
DataManager
BiliClient
```

## V0.2

```text
LiveListener
上下播推送
```

## V0.3

```text
special follow
DynamicListener
dynamic_id 去重
```

## V0.4

```text
周表动态识别
图片下载
ScheduleParser
WeeklySchedule
```

## V0.5

```text
ScheduleService
revision
```

## V0.6

```text
Regex 调播候选
AdjustmentAgent
Schedule Tools
```

## V0.7

```text
周表 diff
调播通知
稳定性 / retry
```

---

# 25. 不要做的事情

Agent 实现时明确禁止：

- 不要把 1000+ 行全部塞进 `main.py`。
- 不要让 LLM 直接写 JSON / SQLite。
- 不要让 Tool 直接访问数据库。
- 不要给普通聊天 Agent 全局注册“修改周表”权限。
- 不要用同步 `requests`。
- 不要把持久化状态放在插件源码目录。
- 不要依赖真实主播发布动态才能测试。
- 不要每次 polling 都发送直播状态。
- 不要在新周表解析失败时覆盖旧周表。
- 不要因为一个 UID 请求失败让整个循环崩溃。
- 不要依赖 Bilibili API 返回字段永远不变。

---

# 26. 参考来源

## AstrBot 官方

### 插件开发
https://github.com/AstrBotDevs/AstrBot/wiki/zh-dev-star-plugin-new

参考：
- 插件结构
- 热重载
- requirements.txt
- 异步网络请求
- 插件开发规范

### 主动消息
https://github.com/AstrBotDevs/AstrBot/wiki/zh-dev-star-guides-send-message

参考：
- `event.unified_msg_origin`
- `context.send_message()`
- `MessageChain`

### 插件存储
https://github.com/AstrBotDevs/AstrBot/wiki/en-dev-star-guides-storage

参考：
- Plugin KV
- `data/plugin_data/{plugin_name}`

### Function Calling
https://github.com/AstrBotDevs/AstrBot/wiki/zh-use-function-calling

参考：
- LLM Tool 调用机制

### FunctionTool / ToolSet 实现
https://github.com/AstrBotDevs/AstrBot/blob/master/astrbot/core/agent/tool.py

参考：
- `FunctionTool`
- `ToolSet`
- 工具隔离设计

---

## AstrBot Bilibili 插件

https://github.com/Soulter/astrbot_plugin_bilibili

主要参考其工程分层，而不是直接复制整个实现：

```text
BiliClient
DataManager
DynamicListener
SubscriptionService
SubscriptionNotificationDispatcher
Renderer
后台 task 生命周期
动态去重
Bilibili 凭据维护
```

其 `main.py`：
https://github.com/Soulter/astrbot_plugin_bilibili/blob/master/main.py

注意：
如果直接复制 / 修改其代码，必须先确认并遵守该项目许可证与版权要求。
如果只是借鉴架构思路，也应在 README 中标注 inspiration/source。

AstrBot 官方开发规范也明确建议标注参考来源：
https://github.com/AstrBotDevs/AstrBot/wiki/zh-dev-star-plugin-new

---

# 27. 项目核心设计原则

插件最终不是一个普通“Bilibili 通知器”。

核心应定义为：

```text
VTuber Schedule State Tracker
```

信息来源：

```text
周表图片
+
临时动态
+
实际直播状态
```

持续维护：

```text
VTuber
↓
WeeklySchedule
↓
StreamPlan[]
↓
Revision History
```

LLM 的职责是：

```text
理解
```

Tool 的职责是：

```text
表达受控修改动作
```

ScheduleService 的职责是：

```text
验证并真正修改状态
```

DataManager 的职责是：

```text
持久化
```

Dispatcher 的职责是：

```text
通知用户
```

必须保持这些边界。
