"""Read-only listing of collected source-list records."""

from __future__ import annotations

from datetime import datetime
from typing import List

from fastapi import APIRouter, Query
from pydantic import BaseModel
from sqlalchemy import func, select

from funread.legado.manage.source.storage import SourceListRecord, get_session_factory

router = APIRouter(prefix="/sources", tags=["sources"])


class SourceListRecordOut(BaseModel):
    id: int
    url: str
    source_type: str
    source_count: int
    updated_at: datetime

    model_config = {"from_attributes": True}


class SourceListPage(BaseModel):
    items: List[SourceListRecordOut]
    total: int
    limit: int
    offset: int


@router.get("", response_model=SourceListPage)
def list_sources(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> SourceListPage:
    session_factory = get_session_factory()
    with session_factory() as session:
        total = session.execute(select(func.count()).select_from(SourceListRecord)).scalar_one()
        rows = (
            session.execute(
                select(SourceListRecord)
                .order_by(SourceListRecord.id.desc())
                .offset(offset)
                .limit(limit)
            )
            .scalars()
            .all()
        )
        items = [SourceListRecordOut.model_validate(row) for row in rows]
    return SourceListPage(items=items, total=total, limit=limit, offset=offset)
