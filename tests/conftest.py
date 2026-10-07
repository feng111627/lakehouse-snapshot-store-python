"""测试公共夹具与场景日志。

每个用例执行前后都会输出一条日志，标明所属业务场景、用例名和结果，
运行 `uv run pytest` 时可直接从日志看到覆盖了哪些业务。
"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

import main
from lakehouse.table import TableRegistry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("lakehouse.tests")

# 用例 docstring 第一行约定为 "场景：xxx"，日志里直接引用。
SCENARIO_LABEL = "场景"


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """每个用例一套独立的磁盘目录和表注册表，互不影响。"""
    registry = TableRegistry(tmp_path / "data")
    monkeypatch.setattr(main, "registry", registry)
    return TestClient(main.app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def log_scenario(request):
    doc = (request.node.obj.__doc__ or "").strip().splitlines()
    scenario = doc[0] if doc else request.node.name
    logger.info("【开始】%s | 用例: %s", scenario, request.node.name)
    yield
    outcome = getattr(request.node, "rep_call", None)
    result = "通过" if outcome is not None and outcome.passed else "失败/错误"
    logger.info("【结束】%s | 用例: %s | 结果: %s", scenario, request.node.name, result)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    setattr(item, f"rep_{report.when}", report)
