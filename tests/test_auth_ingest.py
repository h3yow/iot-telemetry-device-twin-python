"""鉴权、权限与重复上报的接入测试。"""
from iot_service.models import RejectReason, TelemetryMessage

from conftest import log_case


def test_unregistered_device_rejected(service):
    msg = TelemetryMessage(device_id="ghost", payload={"t": 1}, seq=1)
    r = service.ingest("ghost", "tok", msg)
    log_case("未注册设备上报", {"device": "ghost"}, "注册表无此设备", {"accepted": r.accepted, "reason": r.reason})
    assert not r.accepted and r.reason == RejectReason.DEVICE_NOT_REGISTERED


def test_invalid_token_rejected(service):
    service.register_device("d1", "right")
    msg = TelemetryMessage(device_id="d1", payload={"t": 1}, seq=1)
    r = service.ingest("d1", "wrong", msg)
    log_case("凭证无效", {"device": "d1", "token": "wrong"}, "token 与注册值不一致", {"reason": r.reason})
    assert not r.accepted and r.reason == RejectReason.INVALID_CREDENTIALS


def test_permission_denied_rejected(service):
    service.register_device("d2", "tok", scopes={"twin:read"})  # 无 telemetry:write
    msg = TelemetryMessage(device_id="d2", payload={"t": 1}, seq=1)
    r = service.ingest("d2", "tok", msg)
    log_case("权限不足", {"scopes": {"twin:read"}}, "缺少 telemetry:write", {"reason": r.reason})
    assert not r.accepted and r.reason == RejectReason.PERMISSION_DENIED


def test_duplicate_message_not_counted_twice(service):
    service.register_device("d1", "tok")
    service.connect("d1", "tok")
    msg = TelemetryMessage(device_id="d1", payload={"t": 20}, message_id="m-1", seq=1)
    r1 = service.ingest("d1", "tok", msg)
    r2 = service.ingest("d1", "tok", msg)
    twin = service.get_twin("d1")
    log_case("重复上报去重", {"message_id": "m-1", "次数": 2},
             "message_id 在去重窗口内已存在",
             {"first": r1.accepted, "second_reason": r2.reason, "reported_version": twin.reported_version})
    assert r1.accepted
    assert not r2.accepted and r2.reason == RejectReason.DUPLICATE_MESSAGE
    assert twin.reported_version == 1  # 只计入一次


def test_distinct_failure_reasons_are_distinguishable(service):
    service.register_device("ok", "tok")
    reasons = {
        service.ingest("ghost", "x", TelemetryMessage(device_id="ghost", payload={}, seq=1)).reason,
        service.ingest("ok", "bad", TelemetryMessage(device_id="ok", payload={}, seq=1)).reason,
    }
    log_case("失败原因可区分", {"场景": ["未注册", "凭证错误"]}, "RejectReason 枚举", {"reasons": sorted(map(str, reasons))})
    assert reasons == {RejectReason.DEVICE_NOT_REGISTERED, RejectReason.INVALID_CREDENTIALS}
