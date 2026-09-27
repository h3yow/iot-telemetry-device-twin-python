"""孪生状态一致性：CAS 并发、无丢失更新、无状态回退、冲突可解释。"""
import threading

from iot_service.models import RejectReason, TelemetryMessage

from conftest import log_case


def test_cas_version_conflict_rejected(service):
    service.register_device("d1", "tok")
    twin = service.get_twin("d1")
    r1 = service.set_desired("d1", {"mode": "auto"}, twin.desired_version)
    r2 = service.set_desired("d1", {"mode": "manual"}, twin.desired_version)  # 过期基线
    log_case("CAS 版本冲突", {"base_version": twin.desired_version, "并发写": 2},
             "base_version 与当前 desired_version 不一致则拒绝",
             {"r1": r1.applied, "r2_reason": r2.reason, "desired": service.get_twin("d1").desired})
    assert r1.applied
    assert not r2.applied and r2.reason == RejectReason.VERSION_CONFLICT
    assert service.get_twin("d1").desired == {"mode": "auto"}  # 未被覆盖


def test_concurrent_desired_updates_no_lost_update(service):
    service.register_device("d1", "tok")
    n_threads, n_updates = 8, 25
    barrier = threading.Barrier(n_threads)

    def worker(tid: int) -> None:
        barrier.wait()
        for i in range(n_updates):
            while True:  # 乐观并发：冲突后重读重试
                v = service.get_twin("d1").desired_version
                if service.set_desired("d1", {f"k{tid}_{i}": i}, v).applied:
                    break

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    twin = service.get_twin("d1")
    log_case("并发期望更新无丢失", {"线程": n_threads, "每线程更新": n_updates},
             "CAS 冲突重试，版本号=成功更新次数",
             {"desired_version": twin.desired_version, "keys": len(twin.desired)})
    assert twin.desired_version == n_threads * n_updates
    assert len(twin.desired) == n_threads * n_updates  # 无丢失更新


def test_reported_never_regresses(service):
    service.register_device("d1", "tok")
    service.connect("d1", "tok")
    service.ingest("d1", "tok", TelemetryMessage(device_id="d1", payload={"fw": "2.0"},
                                                 message_id="a", seq=10))
    r = service.ingest("d1", "tok", TelemetryMessage(device_id="d1", payload={"fw": "1.0"},
                                                     message_id="b", seq=9))
    twin = service.get_twin("d1")
    log_case("上报状态不回退", {"到达": [("seq10", "2.0"), ("seq9", "1.0")]},
             "reported 只接受 seq 递增",
             {"旧包reason": r.reason, "reported": twin.reported})
    assert r.reason == RejectReason.STALE_MESSAGE
    assert twin.reported["fw"] == "2.0"


def test_reconnect_conflict_is_deterministic_and_explainable(service):
    service.register_device("d1", "tok")
    service.connect("d1", "tok")
    service.ingest("d1", "tok", TelemetryMessage(device_id="d1", payload={"mode": "manual"},
                                                 message_id="m1", seq=1))
    service.disconnect("d1")
    service.set_desired("d1", {"mode": "eco", "level": 3}, service.get_twin("d1").desired_version)
    c1 = service.connect("d1", "tok").sync
    c2 = service.twin.sync_on_reconnect("d1")  # 重放校验，结论应一致
    log_case("重连冲突确定且可解释", {"desired": {"mode": "eco", "level": 3}, "reported": {"mode": "manual"}},
             "逐键比对 desired/reported，差异键保持挂起",
             {"conflict": c1.conflict, "detail": c1.detail, "重放一致": (c1.conflict, c1.detail) == (c2.conflict, c2.detail)})
    assert c1.conflict and not c1.applied
    assert "mode" in c1.detail and "level" in c1.detail
    assert (c1.conflict, c1.detail) == (c2.conflict, c2.detail)  # 确定性
