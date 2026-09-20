"""MetricRecorded 必须写入投影指标列表，且详情与血缘读取一致。"""

import hashlib

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from sqlalchemy.dialects.postgresql import JSONB

from app.cqrs import (
    list_events,
    rebuild_projection_from_events,
    record_metric,
    start_run,
)
from app.database import Base, get_db
from app.main import app
from app.models import EventStore, RunProjection


@compiles(JSONB, "sqlite")
def _compile_jsonb_sqlite(_type, compiler, **kw):
    return "JSON"


def sha(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
    finally:
        session.close()


def test_record_metric_appends_one_with_correct_fields(db):
    run = start_run(
        db,
        actor="researcher",
        project="p1",
        name="n1",
        dataset_content_sha256=sha("ds"),
        code_commit_sha="abc1234",
        description=None,
    )
    assert run.metrics_json == []

    run = record_metric(
        db,
        run_id=run.id,
        actor="researcher",
        name="loss",
        value=0.5,
        step=1,
        expected_version=1,
    )

    # 条数恰好加一
    assert len(run.metrics_json) == 1
    metric = run.metrics_json[0]
    # 名称、数值、step 必须正确写入投影
    assert metric["name"] == "loss"
    assert metric["value"] == 0.5
    assert metric["step"] == 1
    assert metric["actor"] == "researcher"

    # 事件库中同时存在对应事件
    events = list_events(db, run.id)
    metric_events = [e for e in events if e.event_type == "MetricRecorded"]
    assert len(metric_events) == 1
    assert metric_events[0].payload_json == {"name": "loss", "value": 0.5, "step": 1}


def test_record_second_loss_refresh_shows_real_metrics(db):
    """再记一条 loss 后，重新从库里读出的投影应看到两条真实指标，而非空列表或占位。"""
    run = start_run(
        db,
        actor="researcher",
        project="p1",
        name="n1",
        dataset_content_sha256=sha("ds2"),
        code_commit_sha="abc1234",
        description=None,
    )
    record_metric(
        db,
        run_id=run.id,
        actor="researcher",
        name="loss",
        value=0.5,
        step=1,
        expected_version=1,
    )
    record_metric(
        db,
        run_id=run.id,
        actor="researcher",
        name="loss",
        value=0.4,
        step=2,
        expected_version=2,
    )

    # 模拟详情页刷新：丢弃会话身份映射后重新读取投影
    db.expire_all()
    stored = db.get(RunProjection, run.id)
    assert stored is not None
    assert len(stored.metrics_json) == 2
    assert [(m["name"], m["value"], m["step"]) for m in stored.metrics_json] == [
        ("loss", 0.5, 1),
        ("loss", 0.4, 2),
    ]
    # 不允许出现任何占位行
    assert all(m["name"] != "(pending sync)" for m in stored.metrics_json)


def test_rebuilt_projection_metrics_fields_match_stored(db):
    run = start_run(
        db,
        actor="researcher",
        project="p1",
        name="n1",
        dataset_content_sha256=sha("ds3"),
        code_commit_sha="deadbeef",
        description=None,
    )
    record_metric(
        db,
        run_id=run.id,
        actor="researcher",
        name="loss",
        value=0.42,
        step=7,
        expected_version=run.version,
    )

    rebuilt = rebuild_projection_from_events(db, run.id)
    stored = db.get(RunProjection, run.id)
    assert rebuilt is not None and stored is not None

    def _summary(metrics):
        return [(m["name"], m["value"], m["step"], m["actor"]) for m in metrics]

    # 血缘汇总（重放投影）与详情（存储投影）指标完全一致（忽略时间戳序列化格式）
    assert _summary(rebuilt.metrics_json) == _summary(stored.metrics_json)
    assert len(stored.metrics_json) == 1
    assert stored.metrics_json[0]["name"] == "loss"
    assert stored.metrics_json[0]["value"] == 0.42
    assert stored.metrics_json[0]["step"] == 7

    # 事件库有事件但投影缺失时，重放也必须补齐指标
    event_count = db.query(EventStore).count()
    assert event_count == 2


@pytest.fixture()
def client():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)

    def _get_db():
        session = Session()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = _get_db
    # 不使用 with：避免触发 lifespan 中针对真实 Postgres 的 create_all
    yield TestClient(app)
    app.dependency_overrides.clear()


def _auth_headers(client) -> dict:
    resp = client.post(
        "/api/auth/login",
        json={"username": "researcher", "password": "lab123456"},
    )
    assert resp.status_code == 200, resp.text
    token = resp.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def test_api_detail_and_lineage_show_recorded_metric(client):
    headers = _auth_headers(client)

    created = client.post(
        "/api/runs",
        headers=headers,
        json={
            "project": "p1",
            "name": "n1",
            "dataset_content_sha256": sha("ds-api"),
            "code_commit_sha": "abc1234",
            "description": None,
            "expected_version": 0,
        },
    )
    assert created.status_code == 201, created.text
    run_id = created.json()["id"]
    assert created.json()["metrics_json"] == []

    recorded = client.post(
        f"/api/runs/{run_id}/metrics",
        headers=headers,
        json={"name": "loss", "value": 0.31, "step": 3, "expected_version": 1},
    )
    assert recorded.status_code == 200, recorded.text
    body = recorded.json()
    assert len(body["metrics_json"]) == 1
    assert body["metrics_json"][0]["name"] == "loss"
    assert body["metrics_json"][0]["value"] == 0.31
    assert body["metrics_json"][0]["step"] == 3

    # 刷新详情
    detail = client.get(f"/api/runs/{run_id}", headers=headers)
    assert detail.status_code == 200
    detail_metrics = detail.json()["metrics_json"]
    assert len(detail_metrics) == 1
    assert detail_metrics[0]["name"] == "loss"
    assert detail_metrics[0]["value"] == 0.31
    assert detail_metrics[0]["step"] == 3

    # 血缘汇总与详情一致
    lineage = client.get(f"/api/runs/{run_id}/lineage", headers=headers)
    assert lineage.status_code == 200
    lineage_metrics = lineage.json()["metrics"]
    assert lineage_metrics == detail_metrics

    # 事件时间线也能查到该事件
    events = client.get(f"/api/runs/{run_id}/events", headers=headers)
    assert events.status_code == 200
    metric_events = [e for e in events.json() if e["event_type"] == "MetricRecorded"]
    assert len(metric_events) == 1
    assert metric_events[0]["payload_json"] == {
        "name": "loss",
        "value": 0.31,
        "step": 3,
    }
