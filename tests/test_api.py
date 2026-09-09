import requests
from fastapi.testclient import TestClient

from funread.api.app import create_app
from funread.legado.manage.source.storage import upsert_source_list_record


def test_sources_api_filters_and_paginates(monkeypatch, tmp_path):
    database_url = f"sqlite:///{tmp_path / 'api.db'}"
    monkeypatch.setenv("FUNREAD_DATABASE_URL", database_url)
    for url, source_type in (
        ("https://example.com/book.json", "book"),
        ("https://example.com/rss-1.json", "rss"),
        ("https://example.com/rss-2.json", "rss"),
    ):
        upsert_source_list_record(
            url=url, source_type=source_type, source_count=3, database_url=database_url
        )

    with TestClient(create_app()) as client:
        response = client.get(
            "/api/v1/sources", params={"source_type": "rss", "limit": 1, "offset": 1}
        )

    assert response.status_code == 200
    page = response.json()
    assert (page["total"], page["limit"], page["offset"]) == (2, 1, 1)
    assert [item["url"] for item in page["items"]] == ["https://example.com/rss-1.json"]


def test_healthz(monkeypatch, tmp_path):
    monkeypatch.setenv("FUNREAD_DATABASE_URL", f"sqlite:///{tmp_path / 'health.db'}")

    with TestClient(create_app()) as client:
        response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


class _SourceResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def test_source_management_flow(monkeypatch, tmp_path):
    monkeypatch.setenv("FUNREAD_DATABASE_URL", f"sqlite:///{tmp_path / 'manage.db'}")
    payload = [{"bookSourceName": "测试", "bookSourceUrl": "https://books.example.com"}]
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: _SourceResponse(payload))

    with TestClient(create_app()) as client:
        created = client.post("/api/v1/sources", json={"url": "https://example.com/books.json"})
        assert created.status_code == 201
        source = created.json()
        assert (source["source_type"], source["source_count"], source["enabled"]) == (
            "book",
            1,
            True,
        )

        duplicate = client.post("/api/v1/sources", json={"url": "https://example.com/books.json"})
        assert duplicate.status_code == 409

        source_id = source["id"]
        disabled = client.patch(f"/api/v1/sources/{source_id}", json={"enabled": False})
        assert disabled.json()["enabled"] is False
        filtered = client.get(
            "/api/v1/sources", params={"enabled": False, "keyword": "books.json"}
        ).json()
        assert [item["id"] for item in filtered["items"]] == [source_id]

        payload.append({"bookSourceName": "测试 2", "bookSourceUrl": "https://b.example.com"})
        collected = client.post(f"/api/v1/sources/{source_id}/collect").json()
        assert collected == {"source_id": source_id, "ok": True, "source_count": 2, "error": None}

        assert client.post(f"/api/v1/sources/{source_id}/reset-cursor").status_code == 204
        assert client.delete(f"/api/v1/sources/{source_id}").status_code == 204
        assert (
            client.patch(f"/api/v1/sources/{source_id}", json={"enabled": True}).status_code == 404
        )


def test_collect_source_records_failure(monkeypatch, tmp_path):
    database_url = f"sqlite:///{tmp_path / 'failure.db'}"
    monkeypatch.setenv("FUNREAD_DATABASE_URL", database_url)
    source = upsert_source_list_record(
        url="https://example.com/rss.json",
        source_type="rss",
        source_count=4,
        database_url=database_url,
    )
    monkeypatch.setattr(
        requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(requests.ConnectionError("offline")),
    )

    with TestClient(create_app()) as client:
        report = client.post(f"/api/v1/sources/{source.id}/collect").json()
        row = client.get("/api/v1/sources").json()["items"][0]

    assert report["ok"] is False
    assert report["source_count"] == 4
    assert row["consecutive_failures"] == 1
    assert row["last_error"] == "offline"
