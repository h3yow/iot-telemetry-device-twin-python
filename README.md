# 物联网设备接入、遥测汇聚与设备状态一致性服务（本地）

一套**纯本地、零外部依赖**的 Python 实现：设备注册与鉴权、遥测上报（去重 / 乱序补传 /
防倒退）、设备孪生（期望状态与上报状态分离、并发版本控制、重连对账、确定性冲突判定）、
基于阈值与连接状态的规则引擎与告警链路（去重、恢复、异常归一化）。

不依赖任何消息队列、数据库、真实设备或联网服务；所有状态保存在进程内存，
时间通过可注入的 `Clock` 获取，保证测试结论确定可复现。

## 目录结构

```
iot_twin/
  clock.py     可注入时钟（SystemClock / FakeClock）
  errors.py    错误码与领域异常 DomainError
  models.py    领域模型：Device / TelemetrySample / TwinState / Alert ...
  registry.py  设备注册、凭证鉴权、权限校验
  dedupe.py    按设备+seq 的有界幂等去重窗口
  telemetry.py 遥测汇聚：(ts_ms, seq) 水位、latest-wins、批量排序并入
  twin.py      设备孪生：期望/上报分离、乐观锁、重连对账、冲突判定
  rules.py     规则：数值阈值（GT/GTE/LT/LTE/EQ）与连接状态
  alerts.py    告警状态机：FIRING / RESOLVED / ERROR，去重恢复、事件时间防倒退
  service.py   门面编排（推荐入口）
  app.py       可选 FastAPI HTTP 适配层（仅绑定 127.0.0.1）
tests/         单元与端到端测试（日志打印输入、判定依据、最终状态）
examples/demo.py  可直接运行的本地演示
main.py        可选 HTTP 服务入口
```

## 快速开始

```bash
uv sync --group dev        # 安装依赖（fastapi + pytest）
uv run pytest              # 运行全部测试（带 -s，打印判定日志）
uv run python examples/demo.py   # 端到端演示，无需起服务
uv run python main.py      # 可选：启动本地 HTTP 服务（127.0.0.1:8080）
```

## 接入与鉴权约定

1. **注册**：管理员调用 `register_device(device_id, credential)`（HTTP 为
   `POST /admin/devices`，头 `Authorization: Bearer admin`）。重复注册返回
   `DEVICE_ALREADY_REGISTERED`。
2. **鉴权**：设备上报时携带 `Authorization: Bearer <credential>`。服务按以下顺序给出
   **可区分**的失败原因：

   | 场景 | 错误码 / `IngestResult.reject_reason` |
   | --- | --- |
   | 设备未注册 | `DEVICE_NOT_REGISTERED`（HTTP 404） |
   | 凭证错误 | `INVALID_CREDENTIAL`（HTTP 401） |
   | 设备 A 上报设备 B 的数据 / 设备尝试管理操作 | `PERMISSION_DENIED`（HTTP 403） |
   | 同一 seq 重复上报 | `DUPLICATE_SAMPLE`（HTTP 422） |
   | 乱序/过期样本（不高于水位） | `STALE_SAMPLE`（HTTP 422） |
   | 载荷格式错误 | `BAD_TELEMETRY`（HTTP 422） |

3. **遥测报文**：`seq`（设备内单调递增序号）、`ts_ms`（设备事件时间）、`metrics`
   （字符串键，值可为数值或 `led="on"` 这类状态值；仅数值指标参与阈值规则）、
   可选 `ack_desired_version`（设备确认执行的期望状态版本）。

## 遥测汇聚规则

* **幂等去重**：同一设备相同 `seq` 在有界窗口（默认 1024，FIFO 淘汰）内只计入一次，
  重复返回 `DUPLICATE_SAMPLE`。
* **防倒退**：以 `(ts_ms, seq)` 为水位，先比较事件时间，时间相同再比较序号；
  不高于水位的样本返回 `STALE_SAMPLE`，绝不覆盖已有汇聚结果。
* **批量补传**：重连后 `backfill()` 先按 `(ts_ms, seq)` 排序再逐条并入，
  因此离线期间乱序/批量到达的数据按事件时间确定地合并，旧重复被丢弃，
  指标按 latest-wins 聚合（新样本携带的键覆盖，未携带的键保留）。

## 期望状态 / 上报状态与冲突判定

* 期望状态（desired，管理员下发）与上报状态（reported，设备上报）**分离维护**，
  各自带单调版本。
* **并发更新**：每个孪生一把锁串行化更新，杜绝丢失更新；更新时可传
  `expected_version` 做乐观并发控制，不匹配返回 `VERSION_CONFLICT`，调用方可重试。
  `merge=True` 为补丁合并，`merge=False` 为整体替换。
* **离线期间下发**：设备离线时期望状态为 `PENDING`；重连后保持挂起并等待设备带
  `ack_desired_version` 的上报对账。
* **确定性对账结论**（`SyncStatus`）：
  * `IN_SYNC`：`ack == 当前期望版本` 且期望值逐键都出现在上报中且值相等；
  * `CONFLICTED`：`ack < 当前期望版本`（设备按旧期望执行），或任一键值不一致；
    `conflict_detail()` 给出逐键的 `desired vs reported` 明细；
  * `PENDING`：尚无期望/上报、或设备尚未确认新期望。
  * 设备不带 `ack` 的普通遥测**不会**把已有的 `IN_SYNC` 结论打回（状态不无故回退）；
    每次下发新期望才会重置为 `PENDING` 等待重新对账。
* 上报状态同样按 `(ts_ms, seq)` 水位并入，过期上报不会倒退孪生。

## 规则与告警规则

* 两类规则：`METRIC`（指标与阈值比较：`GT/GTE/LT/LTE/EQ`）与 `CONNECTION`
  （在线=1 / 离线=0）。
* 每个 `(规则, 设备)` 一条当前告警，状态机：
  `无 -> FIRING -> RESOLVED -> FIRING ...`，持续命中**去重**（不新增告警，只刷新值），
  规则执行异常归一化为 `ERROR`（`error_code=RULE_EVALUATION_ERROR`，告警明细保留规则号、
  异常类型与原始值，可定位），后续正常数据可把 `ERROR` 恢复为 `RESOLVED`。
* **指标缺失 = 信息不足**（`conclusive=False`）：该样本不含目标指标时维持原结论，
  不触发告警也不误恢复；**指标存在但无法转成数值才算执行异常**。
* **乱序防倒退**：告警链路为每个 `(规则, 设备)` 记录事件时间水位，`ts_ms` 更旧的评估
  一律忽略，过期遥测不可能把 `RESOLVED` 拉回 `FIRING`。
* 设备断连时活动告警自动置为 `RESOLVED`（"设备离线，告警自动恢复"）。

## 本地验证方法

```bash
uv run pytest -q          # 27 个用例，含乱序补传/重复上报/重连/并发
uv run python examples/demo.py
```

测试覆盖：

* 未注册、凭证无效、权限不足三类拒绝可区分（`tests/test_registry.py`、`test_service.py`）；
* 短时间重复上报去重（`test_telemetry.py`、`test_service.py`）；
* 断连重连后乱序批量补传、按序并入且不倒退（`test_telemetry.py`、`test_service.py`）；
* 离线期望下发、重连后生效、键值冲突/版本冲突的确定性结论（`test_twin.py`）；
* 16 线程并发期望更新 + 乐观版本重试，无丢失更新（`test_twin.py`、`test_service.py`）；
* 告警触发去重、恢复、再触发、乱序防倒退、异常归一化、离线自动恢复（`test_alerts.py`）；
* HTTP 层拒绝码可区分（`tests/test_app.py`，使用 TestClient，不绑定端口）。

单测通过 `tests/conftest.py` 配置的 logger 向 stdout 打印**输入、判定依据与最终状态**，
例如：

```
INFO iot_twin.test.service: 输入: 补传 seq=5,3,2,4,1(乱序); 判定依据: (ts,seq)排序+去重+水位;
 结果=[(1, False, 'DUPLICATE_SAMPLE'), (2, True, None), ...]
```

## 说明与边界

* 全内存实现，进程重启状态清空——定位为本地开发/教学/面试演示，不保证持久化；
  领域边界清晰，后续可在 `service.py` 门面后替换为持久化实现。
* HTTP 层为可选便利接口，认证使用演示级 Bearer Token，请勿用于生产。
