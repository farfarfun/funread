"""Source-list registration, management, and manual collection."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

import requests
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import AnyHttpUrl, BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from funread.legado.manage.source.storage import (
    SourceListRecord,
    count_source_items,
    count_source_payloads,
    fetch_source_list_data,
    get_session_factory,
    utcnow,
)

router = APIRouter(prefix="/sources", tags=["sources"])
SourceType = Literal["book", "rss"]


class SourceCreate(BaseModel):
    url: AnyHttpUrl
    source_type: SourceType | None = None


class SourceUpdate(BaseModel):
    enabled: bool


class SourceListRecordOut(BaseModel):
    id: int
    url: str
    source_type: SourceType
    source_count: int
    enabled: bool
    consecutive_failures: int
    last_error: str | None
    last_success_at: datetime | None
    increment_start: int | None
    increment_stop: int | None
    last_queried_at: datetime
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class SourceListPage(BaseModel):
    items: list[SourceListRecordOut]
    total: int
    limit: int
    offset: int


class CollectReport(BaseModel):
    source_id: int
    ok: bool
    source_count: int
    error: str | None = None


def _get_or_404(session: Session, source_id: int) -> SourceListRecord:
    record = session.get(SourceListRecord, source_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="采集源不存在")
    return record


def _download(url: str) -> object:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def _detect_source_type(payload: object) -> SourceType | None:
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = next(
            (payload[key] for key in ("list", "data") if isinstance(payload.get(key), list)),
            [payload],
        )
    else:
        return None

    for item in items:
        if not isinstance(item, dict):
            continue
        if "bookSourceUrl" in item or "bookSourceName" in item:
            return "book"
        if "sourceUrl" in item or "sourceName" in item:
            return "rss"
    return None


@router.get("/supported", response_model=list[SourceType])
def list_supported() -> list[SourceType]:
    return ["book", "rss"]


@router.post("", response_model=SourceListRecordOut, status_code=status.HTTP_201_CREATED)
def create_source(payload: SourceCreate) -> SourceListRecordOut:
    url = str(payload.url)
    session_factory = get_session_factory()
    with session_factory() as session:
        existing = session.scalar(select(SourceListRecord).where(SourceListRecord.url == url))
        if existing is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"采集源已存在（id={existing.id}）",
            )

        try:
            source_data = _download(url)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"无法读取采集源：{exc}",
            ) from exc

        detected = _detect_source_type(source_data)
        source_type = payload.source_type or detected
        if source_type is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="无法识别采集源类型，请选择书源或 RSS 源",
            )
        if (
            detected is not None
            and payload.source_type is not None
            and detected != payload.source_type
        ):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"采集源内容识别为 {detected}，与所选类型 {payload.source_type} 不一致",
            )

        now = utcnow()
        record = SourceListRecord(
            url=url,
            source_type=source_type,
            source_count=count_source_items(source_data),
            last_queried_at=now,
            last_success_at=now,
        )
        session.add(record)
        session.commit()
        session.refresh(record)
        return SourceListRecordOut.model_validate(record)


@router.get("", response_model=SourceListPage)
def list_sources(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    source_type: SourceType | None = None,
    enabled: bool | None = None,
    keyword: str | None = Query(default=None, min_length=1, max_length=255),
) -> SourceListPage:
    session_factory = get_session_factory()
    with session_factory() as session:
        filters = []
        if source_type is not None:
            filters.append(SourceListRecord.source_type == source_type)
        if enabled is not None:
            filters.append(SourceListRecord.enabled.is_(enabled))
        if keyword is not None:
            filters.append(SourceListRecord.url.contains(keyword))
        total = session.execute(
            select(func.count()).select_from(SourceListRecord).where(*filters)
        ).scalar_one()
        rows = (
            session.execute(
                select(SourceListRecord)
                .where(*filters)
                .order_by(SourceListRecord.id.desc())
                .offset(offset)
                .limit(limit)
            )
            .scalars()
            .all()
        )
        items = [SourceListRecordOut.model_validate(row) for row in rows]
    return SourceListPage(items=items, total=total, limit=limit, offset=offset)


@router.patch("/{source_id}", response_model=SourceListRecordOut)
def update_source(source_id: int, payload: SourceUpdate) -> SourceListRecordOut:
    session_factory = get_session_factory()
    with session_factory() as session:
        record = _get_or_404(session, source_id)
        record.enabled = payload.enabled
        session.commit()
        session.refresh(record)
        return SourceListRecordOut.model_validate(record)


@router.delete("/{source_id}", status_code=status.HTTP_204_NO_CONTENT, response_model=None)
def delete_source(source_id: int) -> None:
    session_factory = get_session_factory()
    with session_factory() as session:
        session.delete(_get_or_404(session, source_id))
        session.commit()


@router.post(
    "/{source_id}/reset-cursor",
    status_code=status.HTTP_204_NO_CONTENT,
    response_model=None,
)
def reset_source_cursor(source_id: int) -> None:
    session_factory = get_session_factory()
    with session_factory() as session:
        record = _get_or_404(session, source_id)
        record.last_queried_at = datetime(2000, 1, 1)
        record.consecutive_failures = 0
        record.last_error = None
        session.commit()


@router.post("/{source_id}/collect", response_model=CollectReport)
def collect_source(source_id: int) -> CollectReport:
    session_factory = get_session_factory()
    with session_factory() as session:
        record = _get_or_404(session, source_id)
        record.last_queried_at = utcnow()
        try:
            record.source_count = count_source_payloads(fetch_source_list_data(record))
            record.last_success_at = record.last_queried_at
            record.consecutive_failures = 0
            record.last_error = None
            report = CollectReport(source_id=record.id, ok=True, source_count=record.source_count)
        except Exception as exc:
            record.consecutive_failures += 1
            record.last_error = str(exc)[:2048]
            report = CollectReport(
                source_id=record.id,
                ok=False,
                source_count=record.source_count,
                error=record.last_error,
            )
        session.commit()
        return report
