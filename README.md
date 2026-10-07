# 物联网设备接入、遥测汇聚与设备状态一致性服务（本地）

一套**纯本地、零外部依赖**的 Python 实现：设备注册与鉴权、遥测上报（去重 / 乱序补传 /
防倒退）、设备孪生（期望状态与上报状态分离、并发版本控制、重连对账、确定性冲突判定）、
基于阈值与连接状态的规则引擎与告警链路（去重、恢复、异常归一化），以及完整的期望状态
**「下发—确认—收尾」运营链路**（确认时限与可区分的超时结论、迟到确认自动纠正、
未确认变更撤销、批量下发与逐台结论查询）。

不依赖任何消息队列、数据库、真实设备或联网服务；所有状态保存在进程内存，
时间通过可注入的 `Clock` 获取，保证测试结论确定可复现。

## 目录结构

```
iot_twin/
  clock.py     可注入时钟（SystemClock / FakeClock）
  errors.py    错误码与领域异常 DomainError
  models.py    领域模型：Device / TelemetrySample / TwinState / Alert / BatchRecord ...
  registry.py  设备注册、凭证鉴权、权限校验
  dedupe.py    按设备+seq 的有界幂等去重窗口
  telemetry.py 遥测汇聚：(ts_ms, seq) 水位、latest-wins、批量排序并入
  twin.py      设备孪生：期望/上报分离、乐观锁、重连对账、冲突判定、确认时限、撤销
  rules.py     规则：数值阈值（GT/GTE/LT/LTE/EQ）与连接状态
  alerts.py    告警状态机：FIRING / RESOLVED / ERROR，去重恢复、事件时间防倒退
  dispatch.py  批量下发编排：逐台独立结果、部分失败隔离、幂等重试、批次查询
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
  * `ACK_TIMEOUT`：已下发且设了确认时限，超过时限设备仍未确认；与
    `CONFLICTED`（设备已表态但按旧版本/键值不对）和 `PENDING`（尚在等待）
    明确区分，`timeout_detail()` 给出可解释明细；
  * `PENDING`：尚无期望/上报、或设备尚未确认新期望、或离线期间时限未激活。
  * 设备不带 `ack` 的普通遥测**不会**把已有的 `IN_SYNC` 结论打回（状态不无故回退）；
    每次下发新期望才会重置为 `PENDING` 等待重新对账。
* 上报状态同样按 `(ts_ms, seq)` 水位并入，过期上报不会倒退孪生。

## 确认时限（下发—确认—收尾）

下发期望时可带 `ack_timeout_ms`（毫秒）。确认截止时刻（`deadline_ms`）怎么算：

* **设备在线时下发**：`deadline = 下发时刻 + ack_timeout_ms`，从下发那一刻计时；
* **设备离线期间下发**：时限**暂不激活**（`deadline_ms=None`），设备**重新上线
  那一刻**才设置为 `上线时刻 + ack_timeout_ms`。因此无论离线多久，都不会出现
  设备刚上线就直接被判超时；
* 超过 `deadline` 且设备仍未确认（状态还是 `PENDING`）-> 结论转为 `ACK_TIMEOUT`。

超时结论的边界：

* 超时是**可自动纠正的中间结论**，不是终态。时限之后设备补来**正确**确认
  （版本一致、键值一致），对账把结论自动改回 `IN_SYNC`，不会一直卡在超时；
  补来的是旧版本确认或键值对不上，则转为 `CONFLICTED`；
* 已经是 `IN_SYNC` / `CONFLICTED` 的结论（设备已表态）不会被超时覆盖；
* 不传 `ack_timeout_ms` 的下发**不设时限**，行为与旧版本完全一致（长期不确认
  也保持 `PENDING`，不会出现 `ACK_TIMEOUT`）。

### 对现有单台下发行为的兼容范围

* `update_desired(values, expected_version=None, merge=True)` 的既有签名、
  `merge` 补丁/替换语义、`expected_version` 乐观锁（不符仍返回 `VERSION_CONFLICT`）
  全部保持不变；新增的 `ack_timeout_ms` 是可选关键字参数，缺省即"不设时限"；
* 既有 `SyncStatus.PENDING / IN_SYNC / CONFLICTED` 取值与 `conflict_detail()`
  语义不变，`ACK_TIMEOUT` 为**新增**枚举值（只在显式设置时限时出现），不会改变
  任何旧调用的对账结论；
* 撤销与批量下发均为**新增**方法 / HTTP 端点，不改变原有注册鉴权、遥测汇聚、
  对账与告警链路的行为；告警与对账既有用例零改动通过。

## 撤销未确认的期望变更

`revoke_desired(device_id, expected_version=None)` 撤销一次**尚未被设备确认**的
期望变更：

* 撤销后取值回到这次变更之前**最后一次已被确认**的期望取值（首次下发即被撤销、
  此前没有确认基线时，期望被清空，`desired=None`）；
* **版本单调不重用**：撤销本身也是一次新版本（`revoke_of_version` 记录被撤销的
  版本），绝不回退到旧版本号，避免设备迟到确认与撤销版撞版本；
* **幂等**：对当前这个尚未被确认的撤销版重复撤销，直接返回现状，不多出一版、
  不把状态弄乱；
* **已确认生效的期望不允许直接撤销**（`ALREADY_CONFIRMED`，HTTP 409）：必须走
  一次新的下发去覆盖；
* **并发安全**：撤销、新下发、设备确认在同一把孪生锁内串行，撤销/下发还可带
  `expected_version` 做乐观并发控制，失败方收到 `VERSION_CONFLICT` 可重试，
  不发生丢失更新、取值无故回退或版本错乱。

## 批量下发

`dispatch_batch(items, batch_id=None, idempotency_key=None)` 一次给一批设备下发，
每台可带各自不同的键值 / 合并方式 / 乐观版本 / 确认时限：

* **逐台独立出结果**：`BatchItemResult` 记录每台 `DISPATCHED`（含新期望版本）或
  `REJECTED`（含可区分的 `reject_reason` 与明细，如设备未注册、`VERSION_CONFLICT`）。
  一台失败不影响其它设备；
* **幂等重试**：相同 `idempotency_key` 的重复提交直接返回首次批次记录，**不会重复
  下发、版本不会乱跳**（即使重试内容不同也以首次为准）；
* **逐台结论查询**：`get_batch(batch_id)` 返回该批每台的成败原因，并回填设备**当前**
  的对账结论（`current_sync_status`，含超时/迟到确认纠正后的最新值）；
* 空批次、同一设备在一批里重复出现 -> 整批 `INVALID_REQUEST`（HTTP 400），
  不产生任何下发；批次不存在 -> `BATCH_NOT_FOUND`（HTTP 404）。

HTTP 接口（均需 `Authorization: Bearer admin`）：

| 接口 | 说明 |
| --- | --- |
| `POST /admin/desired/{device_id}` | 单台下发，body 可带 `ack_timeout_ms` |
| `POST /admin/desired/{device_id}/revoke` | 撤销未确认期望，body 可带 `expected_version` |
| `POST /admin/desired-batch` | 批量下发，body 带 `items` / 可选 `batch_id`、`idempotency_key` |
| `GET /admin/desired-batch/{batch_id}` | 查询一批的逐台结论与当前对账状态 |

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
uv run pytest -q          # 45 个用例，含乱序补传/重复上报/重连/并发/超时/撤销/批量
uv run python examples/demo.py
```

测试覆盖：

* 未注册、凭证无效、权限不足三类拒绝可区分（`tests/test_registry.py`、`test_service.py`）；
* 短时间重复上报去重（`test_telemetry.py`、`test_service.py`）；
* 断连重连后乱序批量补传、按序并入且不倒退（`test_telemetry.py`、`test_service.py`）；
* 离线期望下发、重连后生效、键值冲突/版本冲突的确定性结论（`test_twin.py`）；
* 16 线程并发期望更新 + 乐观版本重试，无丢失更新（`test_twin.py`、`test_service.py`）；
* 告警触发去重、恢复、再触发、乱序防倒退、异常归一化、离线自动恢复（`test_alerts.py`）；
* **确认时限**：在线超时前后结论、超时与旧版本/键值冲突的区分、迟到正确确认自动纠正、
  离线期间下发时限从重连时刻起算（`test_ack_timeout.py`、`test_operations_chain.py`）；
* **撤销**：回到最后确认取值、版本单调、重复撤销幂等、已确认不可撤销、撤销与新下发/
  设备确认并发不乱序（`test_revoke.py`、`test_operations_chain.py`）；
* **批量下发**：部分失败逐台隔离、同幂等键重试不重复下发/版本不乱跳、逐台结论查询随
  对账演变回填、空批次/批次内重复设备整批拒绝、设备身份无权批量（`test_batch.py`）；
* HTTP 层拒绝码与撤销/批量新端点（`tests/test_app.py`，使用 TestClient，不绑定端口）。

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
