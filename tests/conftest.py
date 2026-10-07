import pytest
from fastapi.testclient import TestClient

from lakehouse import Column, ColumnType, SnapshotStore, TableSchema
from lakehouse.api import create_app


@pytest.fixture()
def store(tmp_path):
    return SnapshotStore(tmp_path / "data")


@pytest.fixture()
def client(store):
    return TestClient(create_app(store))


@pytest.fixture()
def user_schema():
    return TableSchema(
        "id",
        (
            Column("id", ColumnType.INT),
            Column("name", ColumnType.STR),
        ),
    )


@pytest.fixture()
def store_with_table(store, user_schema):
    store.create_table("users", user_schema)
    return store
