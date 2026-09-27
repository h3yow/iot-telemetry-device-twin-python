# IoT 遥测与设备孪生一致性服务（本地版）

一套纯本地、无外部中间件/真实设备/联网依赖的物联网设备接入、遥测汇聚与设备状态一致性服务。
在断连重连、乱序补传与并发操作下，所有结论均可预期、可追溯。

## 结构

```
iot_service/
  models.py     # 数据模型与可区分的失败原因枚举（RejectReason）
  registry.py   # 设备注册与鉴权
  telemetry.py  # 遥测去重、乱序/离线补传合并
  twin.py       # 期望/上报状态分离、CAS 并发、重连冲突判定
  rules.py      # 阈值/连接/自定义规则、告警去重与恢复、异常归一化
  service.py    # 门面：编排上述模块
main.py         # 本地 HTTP 接入层（FastAPI，仅 127.0.0.1）
tests/          # 场景化单测（日志打印输入/判定依据/最终状态）
```

## 接入与鉴权约定

- 设备先 `register_device(device_id, token, scopes)` 注册，token 为预共享密钥，
  比较使用 `hmac.compare_digest` 防时序侧信道。
- 每次遥测上报都携带 `(device_id, token)`，鉴权失败按可区分原因拒绝：
  - `DEVICE_NOT_REGISTERED`：设备未注册；
  - `INVALID_CREDENTIALS`：token 不匹配；
  - `PERMISSION_DENIED`：缺少 `telemetry:write` scope。
- 上报体内的 `device_id` 会被认证身份覆盖，防止伪造他人身份上报。

## 遥测判定规则

- **去重**：`message_id` 在去重窗口（默认 300s）内重复出现 → `DUPLICATE_MESSAGE`，不重复计入。
- **乱序/过期**：设备侧序号 `seq <= 已应用最大 seq` → `STALE_MESSAGE`，直接丢弃，状态不倒退。
- **离线补传**：离线期间缓存的消息在重连后逐条上报，按 `seq` 自然并入；迟到的旧条目被安全丢弃。
- 被接受的消息会同步更新孪生 `reported` 并触发规则评估（同一临界区内，保证并发一致）。

## 孪生（期望/上报）判定规则

- `desired` 与 `reported` 分离维护，各自带单调版本号。
- **期望状态更新采用 CAS**：`set_desired(patch, base_version)` 要求 `base_version`
  等于当前 `desired_version`，否则返回 `VERSION_CONFLICT`，杜绝并发丢失更新；
  调用方冲突后重读重试即可。
- **上报状态只接受 seq 递增**，过期上报被拒绝，状态无故回退不可能发生。
- **重连校验（确定且可解释）**：重连时逐键比对 `desired`/`reported`：
  - 全部一致 → 期望状态已生效；
  - 存在差异 → 判定为冲突，`detail` 逐键列出 desired/reported 值与处置
    （desired 保持挂起，等待设备上报匹配值）。结论只取决于当前 twin 内容，
    同样输入重放必然得到同样结论。

## 告警判定规则

- 阈值规则：`field op threshold`（`>, >=, <, <=, ==, !=`）；连接规则：OFFLINE 触发；
  自定义规则：注入判定函数。
- **去重**：同一 `(device_id, rule_id)` 同时仅一条 ACTIVE 告警，条件持续满足不重复触发。
- **恢复**：条件不再满足（或设备回 ONLINE）时自动置为 RESOLVED。
- **乱序防护**：按设备记录已评估的最大 seq，过期遥测不进入规则评估，
  不会产生倒退性告警结论。
- **异常归一化**：规则执行异常被捕获为 `RuleError(rule_id, device_id, error_type, detail)`，
  `detail` 保留原始异常 repr 与 payload 上下文，可通过 `/rule-errors` 或
  `service.rules.errors()` 定位。

## 本地验证

```bash
uv sync                 # 安装依赖
uv run pytest -q        # 运行全部场景测试（日志含输入/判定依据/最终状态）
uv run python main.py   # 启动本地 HTTP 服务（127.0.0.1:8000）
```

HTTP 接口示例：

```bash
curl -X POST localhost:8000/devices   -H 'content-type: application/json' \
  -d '{"device_id":"d1","token":"tok"}'
curl -X POST localhost:8000/connect   -H 'content-type: application/json' \
  -d '{"device_id":"d1","token":"tok"}'
curl -X POST localhost:8000/telemetry -H 'content-type: application/json' \
  -d '{"device_id":"d1","token":"tok","message_id":"m1","seq":1,"payload":{"temp":35}}'
curl localhost:8000/alerts
curl localhost:8000/devices/d1/twin
```

测试覆盖：未注册/凭证无效/权限不足、重复上报去重、乱序补传不倒退、
离线批量补传并入、重连告警恢复、离线期望状态重连生效/冲突、
CAS 并发无丢失更新、上报状态不回退、阈值告警去重与恢复、
过期遥测不产生告警、规则异常归一化、多线程并发上报一致性。
