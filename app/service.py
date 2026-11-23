"""Domain service: source ingest, delivery, splitting and correction sheets.

Numbering rules
---------------
* A source row is ``(table_name, row_no)`` and carries the training team's
  internally *merged* student number.
* A project has its own namespace.  The first time a row becomes known to the
  project it receives an anonymous project row number that never changes.
  Each merged student receives one project student number, shared by every
  table of that project.
* A split moves selected rows of one merged student onto a freshly allocated
  project student number.  All other rows keep their number.
* Every delivery registers unique ``(project, row, student)`` items.  A split
  scans those items for the rows it moves: if a row was successfully delivered
  under the old number, a correction line is written in the *same transaction*.
  Later deliveries read the row's current number and therefore need no
  correction.
* A correction line stores the original anonymous project row number and the
  new student number, so a correction sheet is an immutable record and can be
  fetched verbatim later.

Every mutation runs inside ``BEGIN IMMEDIATE``: a failure rolls back the
reassignment, the split record and any correction lines together.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from .db import write_tx
from .errors import ApiError


def utcnow() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Lookup helpers (call inside a transaction)
# ---------------------------------------------------------------------------

def require_project(conn: sqlite3.Connection, project_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM projects WHERE project_id = ?", (project_id,)
    ).fetchone()
    if row is None:
        raise ApiError(404, "project_not_found", f"课题 {project_id!r} 不存在")
    return row


def _next_row_no(conn: sqlite3.Connection, project_id: str) -> int:
    row = conn.execute(
        "SELECT next_row_no FROM project_counters WHERE project_id = ?",
        (project_id,),
    ).fetchone()
    number = int(row["next_row_no"])
    conn.execute(
        "UPDATE project_counters SET next_row_no = ? WHERE project_id = ?",
        (number + 1, project_id),
    )
    return number


def _next_student_no(conn: sqlite3.Connection, project_id: str) -> int:
    row = conn.execute(
        "SELECT next_student_no FROM project_counters WHERE project_id = ?",
        (project_id,),
    ).fetchone()
    number = int(row["next_student_no"])
    conn.execute(
        "UPDATE project_counters SET next_student_no = ? WHERE project_id = ?",
        (number + 1, project_id),
    )
    return number


def _student_no_for_merged(
    conn: sqlite3.Connection, project_id: str, merged_student_no: str
) -> int:
    row = conn.execute(
        """
        SELECT student_no FROM project_student_numbers
        WHERE project_id = ? AND merged_student_no = ?
        """,
        (project_id, merged_student_no),
    ).fetchone()
    if row is not None:
        return int(row["student_no"])
    number = _next_student_no(conn, project_id)
    conn.execute(
        """
        INSERT INTO project_student_numbers
            (project_id, student_no, kind, merged_student_no,
             origin_merged_student_no, split_id, created_at)
        VALUES (?, ?, 'merged', ?, ?, NULL, ?)
        """,
        (project_id, number, merged_student_no, merged_student_no, utcnow()),
    )
    return number


def _origin_merged_for_student(
    conn: sqlite3.Connection, project_id: str, student_no: int
) -> str:
    """The training team's merged number a project student number came from."""
    row = conn.execute(
        """
        SELECT origin_merged_student_no FROM project_student_numbers
        WHERE project_id = ? AND student_no = ?
        """,
        (project_id, student_no),
    ).fetchone()
    if row is None:
        raise ApiError(500, "internal_inconsistency", "学员号缺少归并来源")
    return row["origin_merged_student_no"]


def _delivered_keys(
    conn: sqlite3.Connection, project_id: str
) -> set[tuple[str, int]]:
    rows = conn.execute(
        "SELECT DISTINCT table_name, row_no FROM delivery_items WHERE project_id = ?",
        (project_id,),
    ).fetchall()
    return {(r["table_name"], int(r["row_no"])) for r in rows}


def _split_keys(conn: sqlite3.Connection, project_id: str) -> set[tuple[str, int]]:
    rows = conn.execute(
        "SELECT table_name, row_no FROM split_rows WHERE project_id = ?",
        (project_id,),
    ).fetchall()
    return {(r["table_name"], int(r["row_no"])) for r in rows}


def _validate_unique_rows(rows: list[dict[str, Any]]) -> None:
    seen: set[tuple[str, int]] = set()
    duplicates: list[dict[str, Any]] = []
    for row in rows:
        key = (row["table_name"], int(row["row_no"]))
        if key in seen:
            duplicates.append({"table_name": key[0], "row_no": key[1]})
        seen.add(key)
    if duplicates:
        raise ApiError(
            422,
            "duplicate_rows",
            "同一批次中出现了重复的表名与行号",
            {"duplicates": duplicates},
        )


# ---------------------------------------------------------------------------
# Source rows
# ---------------------------------------------------------------------------

def upsert_source_rows(
    conn: sqlite3.Connection, rows: list[dict[str, Any]]
) -> dict[str, Any]:
    with write_tx(conn):
        return upsert_source_rows_tx(conn, rows)


def upsert_source_rows_tx(
    conn: sqlite3.Connection, rows: list[dict[str, Any]]
) -> dict[str, Any]:
    _validate_unique_rows(rows)

    created = updated = unchanged = 0
    now = utcnow()
    out: list[dict[str, Any]] = []
    for row in rows:
        table_name = row["table_name"]
        row_no = int(row["row_no"])
        merged = row["merged_student_no"]
        existing = conn.execute(
            "SELECT * FROM source_rows WHERE table_name = ? AND row_no = ?",
            (table_name, row_no),
        ).fetchone()
        if existing is None:
            conn.execute(
                """
                INSERT INTO source_rows
                    (table_name, row_no, merged_student_no, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (table_name, row_no, merged, now, now),
            )
            created += 1
            record = {
                "table_name": table_name,
                "row_no": row_no,
                "merged_student_no": merged,
                "created_at": now,
                "updated_at": now,
            }
        elif existing["merged_student_no"] == merged:
            unchanged += 1
            record = dict(existing)
        else:
            owners = conn.execute(
                """
                SELECT DISTINCT project_id FROM project_rows
                WHERE table_name = ? AND row_no = ?
                ORDER BY project_id
                """,
                (table_name, row_no),
            ).fetchall()
            if owners:
                raise ApiError(
                    409,
                    "source_row_locked",
                    "该行已进入课题，不能修改归并学员号",
                    {
                        "table_name": table_name,
                        "row_no": row_no,
                        "project_ids": [r["project_id"] for r in owners],
                    },
                )
            conn.execute(
                """
                UPDATE source_rows SET merged_student_no = ?, updated_at = ?
                WHERE table_name = ? AND row_no = ?
                """,
                (merged, now, table_name, row_no),
            )
            updated += 1
            record = {
                "table_name": table_name,
                "row_no": row_no,
                "merged_student_no": merged,
                "created_at": existing["created_at"],
                "updated_at": now,
            }
        out.append(record)
    return {
        "received": len(rows),
        "created": created,
        "updated": updated,
        "unchanged": unchanged,
        "rows": out,
    }


def list_source_rows(
    conn: sqlite3.Connection, limit: int, offset: int, table_name: str | None
) -> dict[str, Any]:
    where = "WHERE table_name = ?" if table_name is not None else ""
    params: list[Any] = [table_name] if table_name is not None else []
    total = conn.execute(
        f"SELECT COUNT(*) AS n FROM source_rows {where}", params
    ).fetchone()["n"]
    rows = conn.execute(
        f"""
        SELECT table_name, row_no, merged_student_no, created_at, updated_at
        FROM source_rows {where}
        ORDER BY table_name, row_no LIMIT ? OFFSET ?
        """,
        [*params, limit, offset],
    ).fetchall()
    return {
        "total": int(total),
        "limit": limit,
        "offset": offset,
        "items": [dict(r) for r in rows],
    }


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------

def create_project(conn: sqlite3.Connection, name: str) -> dict[str, Any]:
    with write_tx(conn):
        return create_project_tx(conn, name)


def create_project_tx(conn: sqlite3.Connection, name: str) -> dict[str, Any]:
    now = utcnow()
    project_id = "prj_" + fingerprint({"name": name, "at": now})[:20]
    conn.execute(
        "INSERT INTO projects (project_id, name, created_at) VALUES (?, ?, ?)",
        (project_id, name, now),
    )
    conn.execute(
        """
        INSERT INTO project_counters (project_id, next_row_no, next_student_no)
        VALUES (?, 1, 1)
        """,
        (project_id,),
    )
    return {"project_id": project_id, "name": name, "created_at": now}


def list_projects(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT project_id, name, created_at FROM projects
        ORDER BY created_at, project_id
        """
    ).fetchall()
    items = [dict(r) for r in rows]
    return {"total": len(items), "items": items}


def get_project(conn: sqlite3.Connection, project_id: str) -> dict[str, Any]:
    row = require_project(conn, project_id)
    return {
        "project_id": row["project_id"],
        "name": row["name"],
        "created_at": row["created_at"],
    }


def find_project_by_name(conn: sqlite3.Connection, name: str) -> str | None:
    row = conn.execute(
        "SELECT project_id FROM projects WHERE name = ? ORDER BY created_at LIMIT 1",
        (name,),
    ).fetchone()
    return None if row is None else row["project_id"]


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------

def _delivery_response(conn: sqlite3.Connection, delivery_id: int) -> dict[str, Any]:
    delivery = conn.execute(
        "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
    ).fetchone()
    if delivery is None:
        raise ApiError(404, "delivery_not_found", "交付记录不存在")
    lines = conn.execute(
        """
        SELECT ordinal, table_name, project_row_no, student_no, first_registration
        FROM delivery_items WHERE delivery_id = ? ORDER BY ordinal
        """,
        (delivery_id,),
    ).fetchall()
    return {
        "delivery_id": int(delivery["delivery_id"]),
        "project_id": delivery["project_id"],
        "request_id": delivery["request_id"],
        "created_at": delivery["created_at"],
        "item_count": int(delivery["requested_count"]),
        "new_project_row_count": int(delivery["new_project_row_count"]),
        "items": [
            {
                "ordinal": int(line["ordinal"]),
                "table_name": line["table_name"],
                "project_row_no": int(line["project_row_no"]),
                "student_no": int(line["student_no"]),
                "first_registration": bool(line["first_registration"]),
            }
            for line in lines
        ],
    }


def deliver(
    conn: sqlite3.Connection,
    project_id: str,
    items: list[dict[str, Any]],
    request_id: str | None,
) -> dict[str, Any]:
    with write_tx(conn):
        return deliver_tx(conn, project_id, items, request_id)


def deliver_tx(
    conn: sqlite3.Connection,
    project_id: str,
    items: list[dict[str, Any]],
    request_id: str | None,
) -> dict[str, Any]:
    require_project(conn, project_id)

    signature = fingerprint(
        {
            "items": [
                {
                    "table_name": item["table_name"],
                    "row_no": int(item["row_no"]),
                    "merged_student_no": item.get("merged_student_no"),
                }
                for item in items
            ]
        }
    )

    if request_id is not None:
        existing = conn.execute(
            "SELECT * FROM deliveries WHERE project_id = ? AND request_id = ?",
            (project_id, request_id),
        ).fetchone()
        if existing is not None:
            if existing["request_fingerprint"] != signature:
                raise ApiError(
                    409,
                    "request_id_conflict",
                    "request_id 已用于另一批交付内容",
                    {"delivery_id": int(existing["delivery_id"])},
                )
            return _delivery_response(conn, int(existing["delivery_id"]))

    # Validate the whole batch before writing anything.
    _validate_unique_rows(items)

    sources: dict[tuple[str, int], sqlite3.Row] = {}
    missing: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    for item in items:
        key = (item["table_name"], int(item["row_no"]))
        source = conn.execute(
            "SELECT * FROM source_rows WHERE table_name = ? AND row_no = ?", key
        ).fetchone()
        if source is None:
            missing.append({"table_name": key[0], "row_no": key[1]})
            continue
        sources[key] = source
        requested_merged = item.get("merged_student_no")
        if requested_merged is not None and requested_merged != source["merged_student_no"]:
            mismatches.append(
                {
                    "table_name": key[0],
                    "row_no": key[1],
                    "expected": source["merged_student_no"],
                    "provided": requested_merged,
                }
            )
    if missing:
        raise ApiError(
            404,
            "source_row_not_found",
            "交付明细中有未登记的表名与行号",
            {"missing": missing},
        )
    if mismatches:
        raise ApiError(
            409,
            "merged_student_mismatch",
            "交付明细中的归并学员号与登记不一致",
            {"mismatches": mismatches},
        )

    now = utcnow()
    cursor = conn.execute(
        """
        INSERT INTO deliveries
            (project_id, request_id, request_fingerprint, requested_count,
             new_project_row_count, created_at)
        VALUES (?, ?, ?, ?, 0, ?)
        """,
        (project_id, request_id, signature, len(items), now),
    )
    delivery_id = int(cursor.lastrowid)

    new_project_row_count = 0
    for ordinal, item in enumerate(items, start=1):
        table_name = item["table_name"]
        row_no = int(item["row_no"])
        source = sources[(table_name, row_no)]

        project_row = conn.execute(
            """
            SELECT * FROM project_rows
            WHERE project_id = ? AND table_name = ? AND row_no = ?
            """,
            (project_id, table_name, row_no),
        ).fetchone()

        if project_row is None:
            project_row_no = _next_row_no(conn, project_id)
            student_no = _student_no_for_merged(
                conn, project_id, source["merged_student_no"]
            )
            conn.execute(
                """
                INSERT INTO project_rows
                    (project_id, table_name, row_no, project_row_no,
                     current_student_no, original_student_no,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project_id,
                    table_name,
                    row_no,
                    project_row_no,
                    student_no,
                    student_no,
                    now,
                    now,
                ),
            )
            new_project_row_count += 1
        else:
            project_row_no = int(project_row["project_row_no"])
            student_no = int(project_row["current_student_no"])

        # Every successful delivery line is stored once and never rewritten.
        # NULL delivery_item_id is impossible here because deliveries are the
        # only writer of this table and each request is validated up front.
        conn.execute(
            """
            INSERT INTO delivery_items
                (delivery_id, project_id, ordinal, table_name, row_no,
                 project_row_no, student_no, source_merged_student_no,
                 first_registration, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                delivery_id,
                project_id,
                ordinal,
                table_name,
                row_no,
                project_row_no,
                student_no,
                source["merged_student_no"],
                1 if project_row is None else 0,
                now,
            ),
        )

    conn.execute(
        "UPDATE deliveries SET new_project_row_count = ? WHERE delivery_id = ?",
        (new_project_row_count, delivery_id),
    )
    return _delivery_response(conn, delivery_id)


def list_deliveries(
    conn: sqlite3.Connection, project_id: str, limit: int, offset: int
) -> dict[str, Any]:
    require_project(conn, project_id)
    total = conn.execute(
        "SELECT COUNT(*) AS n FROM deliveries WHERE project_id = ?", (project_id,)
    ).fetchone()["n"]
    ids = conn.execute(
        """
        SELECT delivery_id FROM deliveries WHERE project_id = ?
        ORDER BY delivery_id LIMIT ? OFFSET ?
        """,
        (project_id, limit, offset),
    ).fetchall()
    return {
        "total": int(total),
        "items": [_delivery_response(conn, int(r["delivery_id"])) for r in ids],
    }


def get_delivery(
    conn: sqlite3.Connection, project_id: str, delivery_id: int
) -> dict[str, Any]:
    require_project(conn, project_id)
    row = conn.execute(
        "SELECT delivery_id FROM deliveries WHERE delivery_id = ? AND project_id = ?",
        (delivery_id, project_id),
    ).fetchone()
    if row is None:
        raise ApiError(404, "delivery_not_found", "交付记录不存在")
    return _delivery_response(conn, delivery_id)


# ---------------------------------------------------------------------------
# Project rows
# ---------------------------------------------------------------------------

def list_project_rows(
    conn: sqlite3.Connection,
    project_id: str,
    limit: int,
    offset: int,
    table_name: str | None,
    student_no: int | None,
) -> dict[str, Any]:
    require_project(conn, project_id)
    where = ["pr.project_id = ?"]
    params: list[Any] = [project_id]
    if table_name is not None:
        where.append("pr.table_name = ?")
        params.append(table_name)
    if student_no is not None:
        where.append("pr.current_student_no = ?")
        params.append(student_no)
    clause = " AND ".join(where)
    total = conn.execute(
        f"SELECT COUNT(*) AS n FROM project_rows pr WHERE {clause}", params
    ).fetchone()["n"]
    rows = conn.execute(
        f"""
        SELECT pr.*, sr.merged_student_no AS source_merged_student_no
        FROM project_rows pr
        JOIN source_rows sr
          ON sr.table_name = pr.table_name AND sr.row_no = pr.row_no
        WHERE {clause}
        ORDER BY pr.project_row_no LIMIT ? OFFSET ?
        """,
        [*params, limit, offset],
    ).fetchall()
    split_keys = _split_keys(conn, project_id)
    delivered = _delivered_keys(conn, project_id)
    items = []
    for row in rows:
        key = (row["table_name"], int(row["row_no"]))
        items.append(
            {
                "project_row_no": int(row["project_row_no"]),
                "table_name": row["table_name"],
                "source_row_no": int(row["row_no"]),
                "merged_student_no": row["source_merged_student_no"],
                "current_student_no": int(row["current_student_no"]),
                "original_student_no": int(row["original_student_no"]),
                "is_split_out": key in split_keys,
                "is_delivered": key in delivered,
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )
    return {"total": int(total), "limit": limit, "offset": offset, "items": items}


# ---------------------------------------------------------------------------
# Splits
# ---------------------------------------------------------------------------

def _resolve_split_rows(
    conn: sqlite3.Connection, project_id: str, rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Normalise split references to ``table_name`` + ``row_no``.

    A row may be addressed by its source coordinates or by the original
    anonymous project row number printed on a correction sheet.
    """
    resolved: list[dict[str, Any]] = []
    unknown_anonymous: list[int] = []
    for row in rows:
        if row.get("original_row_no") is not None:
            project_row = conn.execute(
                """
                SELECT table_name, row_no FROM project_rows
                WHERE project_id = ? AND project_row_no = ?
                """,
                (project_id, int(row["original_row_no"])),
            ).fetchone()
            if project_row is None:
                unknown_anonymous.append(int(row["original_row_no"]))
                continue
            resolved.append(
                {
                    "table_name": project_row["table_name"],
                    "row_no": int(project_row["row_no"]),
                }
            )
        else:
            resolved.append(
                {"table_name": row["table_name"], "row_no": int(row["row_no"])}
            )
    if unknown_anonymous:
        raise ApiError(
            404,
            "anonymous_row_not_found",
            "拆分请求中的原匿名行号在课题中不存在",
            {"original_row_nos": unknown_anonymous},
        )
    _validate_unique_rows(resolved)
    return resolved


def _split_response(conn: sqlite3.Connection, split_id: int) -> dict[str, Any]:
    split = conn.execute(
        "SELECT * FROM splits WHERE split_id = ?", (split_id,)
    ).fetchone()
    if split is None:
        raise ApiError(404, "split_not_found", "拆分记录不存在")
    project_id = split["project_id"]
    old_student_no = int(split["old_student_no"])
    merged = _origin_merged_for_student(conn, project_id, old_student_no)

    rows = conn.execute(
        "SELECT * FROM split_rows WHERE split_id = ? ORDER BY table_name, row_no",
        (split_id,),
    ).fetchall()
    correction_keys = {
        (r["table_name"], int(r["row_no"]))
        for r in conn.execute(
            "SELECT table_name, row_no FROM correction_items WHERE split_id = ?",
            (split_id,),
        ).fetchall()
    }
    delivered_keys = {
        (r["table_name"], int(r["row_no"]))
        for r in conn.execute(
            """
            SELECT table_name, row_no FROM delivery_items
            WHERE project_id = ? AND student_no = ?
            """,
            (project_id, old_student_no),
        ).fetchall()
    }
    row_items = []
    for row in rows:
        key = (row["table_name"], int(row["row_no"]))
        row_items.append(
            {
                "table_name": row["table_name"],
                "source_row_no": int(row["row_no"]),
                "project_row_no": int(row["project_row_no"]),
                "old_student_no": int(row["old_student_no"]),
                "new_student_no": int(row["new_student_no"]),
                "was_delivered_under_old_no": key in delivered_keys,
                "correction_registered": key in correction_keys,
            }
        )
    return {
        "split_id": int(split["split_id"]),
        "project_id": project_id,
        "request_id": split["request_id"],
        "merged_student_no": merged,
        "old_student_no": old_student_no,
        "new_student_no": int(split["new_student_no"]),
        "row_count": int(split["row_count"]),
        "correction_count": int(split["correction_count"]),
        "created_at": split["created_at"],
        "rows": row_items,
    }


def split_student(
    conn: sqlite3.Connection,
    project_id: str,
    merged_student_no: str | None,
    rows: list[dict[str, Any]],
    request_id: str | None,
) -> dict[str, Any]:
    with write_tx(conn):
        return split_student_tx(
            conn, project_id, merged_student_no, rows, request_id
        )


def split_student_tx(
    conn: sqlite3.Connection,
    project_id: str,
    merged_student_no: str | None,
    rows: list[dict[str, Any]],
    request_id: str | None,
) -> dict[str, Any]:
    require_project(conn, project_id)

    # Resolution only reads; it also makes request retries stable when the
    # client mixes addressing styles.
    resolved = _resolve_split_rows(conn, project_id, rows)
    signature = fingerprint(
        {"merged_student_no": merged_student_no, "rows": resolved}
    )

    if request_id is not None:
        existing = conn.execute(
            "SELECT * FROM splits WHERE project_id = ? AND request_id = ?",
            (project_id, request_id),
        ).fetchone()
        if existing is not None:
            if existing["request_fingerprint"] != signature:
                raise ApiError(
                    409,
                    "request_id_conflict",
                    "request_id 已用于另一批拆分内容",
                    {"split_id": int(existing["split_id"])},
                )
            return _split_response(conn, int(existing["split_id"]))

    # Validate row existence and merged-student membership.
    sources: dict[tuple[str, int], sqlite3.Row] = {}
    missing: list[dict[str, Any]] = []
    for key in resolved:
        source = conn.execute(
            "SELECT * FROM source_rows WHERE table_name = ? AND row_no = ?",
            (key["table_name"], key["row_no"]),
        ).fetchone()
        if source is None:
            missing.append(dict(key))
        else:
            sources[(key["table_name"], key["row_no"])] = source
    if missing:
        raise ApiError(
            404,
            "source_row_not_found",
            "拆分明细中有未登记的表名与行号",
            {"missing": missing},
        )

    source_merged = {
        key: source["merged_student_no"] for key, source in sources.items()
    }
    distinct_merged = sorted(set(source_merged.values()))
    if merged_student_no is None:
        if len(distinct_merged) != 1:
            raise ApiError(
                409,
                "rows_span_multiple_students",
                "拆分行分属多个归并学员号，请明确指定 merged_student_no",
                {"merged_student_nos": distinct_merged},
            )
        merged_student_no = distinct_merged[0]
    else:
        mismatched = [
            {
                "table_name": table_name,
                "row_no": row_no,
                "source_merged_student_no": source_merged[(table_name, row_no)],
            }
            for (table_name, row_no) in source_merged
            if source_merged[(table_name, row_no)] != merged_student_no
        ]
        if mismatched:
            raise ApiError(
                409,
                "merged_student_mismatch",
                "拆分行不属于指定的归并学员号",
                {"merged_student_no": merged_student_no, "rows": mismatched},
            )

    old_student_no = _student_no_for_merged(conn, project_id, merged_student_no)
    origin_merged = merged_student_no

    # Rows already known to the project must still belong to that group.  A
    # row that was split out earlier is in a different group and cannot be
    # split again under the same merged number: the caller's view is stale.
    existing_rows: dict[tuple[str, int], sqlite3.Row] = {}
    conflicts: list[dict[str, Any]] = []
    for key in resolved:
        project_row = conn.execute(
            """
            SELECT * FROM project_rows
            WHERE project_id = ? AND table_name = ? AND row_no = ?
            """,
            (project_id, key["table_name"], key["row_no"]),
        ).fetchone()
        if project_row is not None:
            existing_rows[(key["table_name"], key["row_no"])] = project_row
            if int(project_row["current_student_no"]) != old_student_no:
                conflicts.append(
                    {
                        "table_name": key["table_name"],
                        "row_no": key["row_no"],
                        "current_student_no": int(
                            project_row["current_student_no"]
                        ),
                    }
                )
    if conflicts:
        raise ApiError(
            409,
            "row_not_in_student",
            "部分行已不属于该归并学员号（可能已被拆分）",
            {
                "merged_student_no": merged_student_no,
                "old_student_no": old_student_no,
                "conflicts": conflicts,
            },
        )

    # ---- atomic split -----------------------------------------------------
    now = utcnow()
    new_student_no = _next_student_no(conn, project_id)
    cursor = conn.execute(
        """
        INSERT INTO splits
            (project_id, request_id, request_fingerprint, old_student_no,
             new_student_no, row_count, correction_count, created_at)
        VALUES (?, ?, ?, ?, ?, ?, 0, ?)
        """,
        (
            project_id,
            request_id,
            signature,
            old_student_no,
            new_student_no,
            len(resolved),
            now,
        ),
    )
    split_id = int(cursor.lastrowid)
    conn.execute(
        """
        INSERT INTO project_student_numbers
            (project_id, student_no, kind, merged_student_no,
             origin_merged_student_no, split_id, created_at)
        VALUES (?, ?, 'split', NULL, ?, ?, ?)
        """,
        (project_id, new_student_no, origin_merged, split_id, now),
    )

    correction_count = 0
    for key in resolved:
        table_name = key["table_name"]
        row_no = key["row_no"]
        project_row = existing_rows.get((table_name, row_no))
        if project_row is not None:
            project_row_no = int(project_row["project_row_no"])
            conn.execute(
                """
                UPDATE project_rows
                SET current_student_no = ?, updated_at = ?
                WHERE project_id = ? AND table_name = ? AND row_no = ?
                """,
                (new_student_no, now, project_id, table_name, row_no),
            )
            was_in_project = 1
        else:
            project_row_no = _next_row_no(conn, project_id)
            conn.execute(
                """
                INSERT INTO project_rows
                    (project_id, table_name, row_no, project_row_no,
                     current_student_no, original_student_no,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project_id,
                    table_name,
                    row_no,
                    project_row_no,
                    new_student_no,
                    old_student_no,
                    now,
                    now,
                ),
            )
            was_in_project = 0

        conn.execute(
            """
            INSERT INTO split_rows
                (split_id, project_id, table_name, row_no, project_row_no,
                 old_student_no, new_student_no, was_in_project)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                split_id,
                project_id,
                table_name,
                row_no,
                project_row_no,
                old_student_no,
                new_student_no,
                was_in_project,
            ),
        )

        # Every successfully registered delivery of this row under the old
        # number becomes a correction line, whatever the delivery/split order:
        # rows delivered before the split already used the old number, and a
        # row delivered several times gets one line per delivery record.
        delivered_items = conn.execute(
            """
            SELECT delivery_item_id, delivery_id, project_row_no
            FROM delivery_items
            WHERE project_id = ? AND table_name = ? AND row_no = ?
              AND student_no = ?
            ORDER BY delivery_item_id
            """,
            (project_id, table_name, row_no, old_student_no),
        ).fetchall()
        for delivered in delivered_items:
            conn.execute(
                """
                INSERT INTO correction_items
                    (project_id, split_id, delivery_item_id, delivery_id,
                     table_name, row_no, original_row_no, old_student_no,
                     new_student_no, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project_id,
                    split_id,
                    int(delivered["delivery_item_id"]),
                    int(delivered["delivery_id"]),
                    table_name,
                    row_no,
                    int(delivered["project_row_no"]),
                    old_student_no,
                    new_student_no,
                    now,
                ),
            )
            correction_count += 1

    conn.execute(
        "UPDATE splits SET correction_count = ? WHERE split_id = ?",
        (correction_count, split_id),
    )
    return _split_response(conn, split_id)


def list_splits(
    conn: sqlite3.Connection, project_id: str, limit: int, offset: int
) -> dict[str, Any]:
    require_project(conn, project_id)
    total = conn.execute(
        "SELECT COUNT(*) AS n FROM splits WHERE project_id = ?", (project_id,)
    ).fetchone()["n"]
    ids = conn.execute(
        """
        SELECT split_id FROM splits WHERE project_id = ?
        ORDER BY split_id LIMIT ? OFFSET ?
        """,
        (project_id, limit, offset),
    ).fetchall()
    return {
        "total": int(total),
        "items": [_split_response(conn, int(r["split_id"])) for r in ids],
    }


def get_split(
    conn: sqlite3.Connection, project_id: str, split_id: int
) -> dict[str, Any]:
    require_project(conn, project_id)
    row = conn.execute(
        "SELECT split_id FROM splits WHERE split_id = ? AND project_id = ?",
        (split_id, project_id),
    ).fetchone()
    if row is None:
        raise ApiError(404, "split_not_found", "拆分记录不存在")
    return _split_response(conn, split_id)


# ---------------------------------------------------------------------------
# Correction sheets
# ---------------------------------------------------------------------------

def correction_sheet(
    conn: sqlite3.Connection,
    project_id: str,
    split_id: int | None,
    delivery_id: int | None,
) -> dict[str, Any]:
    require_project(conn, project_id)
    where = ["project_id = ?"]
    params: list[Any] = [project_id]

    if split_id is not None:
        split = conn.execute(
            "SELECT split_id FROM splits WHERE split_id = ? AND project_id = ?",
            (split_id, project_id),
        ).fetchone()
        if split is None:
            raise ApiError(404, "split_not_found", "拆分记录不存在")
        where.append("split_id = ?")
        params.append(split_id)

    if delivery_id is not None:
        delivery = conn.execute(
            "SELECT delivery_id FROM deliveries WHERE delivery_id = ? AND project_id = ?",
            (delivery_id, project_id),
        ).fetchone()
        if delivery is None:
            raise ApiError(404, "delivery_not_found", "交付记录不存在")
        where.append("delivery_id = ?")
        params.append(delivery_id)

    if split_id is not None:
        scope = "split"
    elif delivery_id is not None:
        scope = "delivery"
    else:
        scope = "project"

    rows = conn.execute(
        f"""
        SELECT correction_id, split_id, table_name, original_row_no,
               old_student_no, new_student_no, delivery_id, recorded_at
        FROM correction_items
        WHERE {' AND '.join(where)}
        ORDER BY correction_id
        """,
        params,
    ).fetchall()
    lines = [
        {
            "correction_id": int(r["correction_id"]),
            "split_id": int(r["split_id"]),
            "table_name": r["table_name"],
            "original_row_no": int(r["original_row_no"]),
            "old_student_no": int(r["old_student_no"]),
            "new_student_no": int(r["new_student_no"]),
            "delivery_id": int(r["delivery_id"]),
            "recorded_at": r["recorded_at"],
        }
        for r in rows
    ]
    return {
        "project_id": project_id,
        "scope": scope,
        "split_id": split_id,
        "delivery_id": delivery_id,
        "line_count": len(lines),
        "digest_sha256": fingerprint(lines),
        "lines": lines,
    }


# ---------------------------------------------------------------------------
# Merge trial (read-only)
# ---------------------------------------------------------------------------

#: A pair qualifies only when its common delivered rows cover both tables.
MERGE_TRIAL_REQUIRED_TABLES = ("registration", "assessment")

#: Cap on student numbers per side; keeps the exact matching cheap.
MERGE_TRIAL_MAX_STUDENT_NUMBERS = 12


def _project_row_map(
    conn: sqlite3.Connection, project_id: str
) -> dict[tuple[str, int], sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT table_name, row_no, project_row_no, current_student_no
        FROM project_rows WHERE project_id = ?
        """,
        (project_id,),
    ).fetchall()
    return {(r["table_name"], int(r["row_no"])): r for r in rows}


def _require_student_numbers(
    conn: sqlite3.Connection, project_id: str, numbers: list[int]
) -> None:
    existing = {
        int(r["student_no"])
        for r in conn.execute(
            "SELECT student_no FROM project_student_numbers WHERE project_id = ?",
            (project_id,),
        ).fetchall()
    }
    missing = [number for number in numbers if number not in existing]
    if missing:
        raise ApiError(
            404,
            "student_no_not_found",
            "指定的匿名学员号在课题中不存在",
            {"project_id": project_id, "student_numbers": missing},
        )


def _edge_infos(
    common_rows: list[dict[str, Any]],
    numbers_a: set[int],
    numbers_b: set[int],
) -> dict[tuple[int, int], dict[str, Any]]:
    """Group common rows by the (A, B) current student-number pair.

    Only pairs whose numbers were both selected appear in the result.  An edge
    is *qualified* when its rows cover every required table.
    """
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for rec in common_rows:
        edge = (rec["student_no_a"], rec["student_no_b"])
        if edge[0] in numbers_a and edge[1] in numbers_b:
            grouped.setdefault(edge, []).append(rec)
    infos: dict[tuple[int, int], dict[str, Any]] = {}
    for edge, rows in grouped.items():
        tables = {rec["table_name"] for rec in rows}
        infos[edge] = {
            "rows": rows,
            "weight": len(rows),
            "qualified": all(
                table in tables for table in MERGE_TRIAL_REQUIRED_TABLES
            ),
            "missing_tables": [
                table for table in MERGE_TRIAL_REQUIRED_TABLES if table not in tables
            ],
        }
    return infos


def _best_matching(
    free_a: list[int],
    free_b: list[int],
    edge_infos: dict[tuple[int, int], dict[str, Any]],
) -> tuple[int, tuple[tuple[int, int], ...]]:
    """Maximum-weight one-to-one pairing with a deterministic tie-break.

    Only qualified edges may be taken.  Among equally heavy matchings the
    lexicographically smallest pair tuple wins, so the same committed state
    always yields the same pairing.  With at most 12 numbers per side the
    bitmask DP stays trivially cheap, and unlike a greedy pass it never
    sacrifices the total for the locally heaviest pair.
    """
    index_b = {b: j for j, b in enumerate(free_b)}
    options = {
        a: [
            (b, edge_infos[(a, b)]["weight"])
            for b in free_b
            if (a, b) in edge_infos and edge_infos[(a, b)]["qualified"]
        ]
        for a in free_a
    }

    @lru_cache(maxsize=None)
    def dp(i: int, used_mask: int) -> tuple[int, tuple[tuple[int, int], ...]]:
        if i == len(free_a):
            return (0, ())
        # Option 1: free_a[i] stays unpaired.
        best_weight, best_pairs = dp(i + 1, used_mask)
        # Option 2: pair free_a[i] with a still-free, qualified b.
        for b, weight in options[free_a[i]]:
            bit = 1 << index_b[b]
            if used_mask & bit:
                continue
            sub_weight, sub_pairs = dp(i + 1, used_mask | bit)
            candidate = (sub_weight + weight, ((free_a[i], b),) + sub_pairs)
            if candidate[0] > best_weight or (
                candidate[0] == best_weight and candidate[1] < best_pairs
            ):
                best_weight, best_pairs = candidate
        return best_weight, best_pairs

    return dp(0, 0)


def merge_trial(
    conn: sqlite3.Connection,
    project_a: str,
    project_b: str,
    numbers_a: list[int],
    numbers_b: list[int],
    must_keep_row_nos: list[int],
) -> dict[str, Any]:
    """Pair the current student numbers of two projects, read-only.

    Only source rows already delivered in *both* projects are used, each
    counted once no matter how often it was delivered.  Every selected pair
    must own at least one common ``registration`` and one common ``assessment``
    row; each student number pairs with at most one counterpart.  The result
    is the pairing that preserves the most common source rows.

    ``must_keep_row_nos`` are project A anonymous row numbers that must all be
    preserved; when that is impossible a 409 details every conflicting row and
    its reason.  The caller runs this inside one read transaction, so the
    whole result reflects a single committed state even if deliveries or
    splits commit concurrently.  Nothing is written.
    """
    if project_a == project_b:
        raise ApiError(422, "same_project", "甲乙必须是两个不同的课题")
    require_project(conn, project_a)
    require_project(conn, project_b)

    numbers_a = sorted(set(numbers_a))
    numbers_b = sorted(set(numbers_b))
    if (
        not numbers_a
        or not numbers_b
        or len(numbers_a) > MERGE_TRIAL_MAX_STUDENT_NUMBERS
        or len(numbers_b) > MERGE_TRIAL_MAX_STUDENT_NUMBERS
    ):
        raise ApiError(
            422,
            "student_numbers_out_of_range",
            f"每侧须指定 1 到 {MERGE_TRIAL_MAX_STUDENT_NUMBERS} 个匿名学员号",
        )
    _require_student_numbers(conn, project_a, numbers_a)
    _require_student_numbers(conn, project_b, numbers_b)

    rows_a = _project_row_map(conn, project_a)
    rows_b = _project_row_map(conn, project_b)
    delivered_a = _delivered_keys(conn, project_a)
    delivered_b = _delivered_keys(conn, project_b)
    common_keys = delivered_a & delivered_b

    common_rows: list[dict[str, Any]] = []
    for key in sorted(common_keys):
        row_a, row_b = rows_a[key], rows_b[key]
        common_rows.append(
            {
                "table_name": key[0],
                "source_row_no": key[1],
                "student_no_a": int(row_a["current_student_no"]),
                "student_no_b": int(row_b["current_student_no"]),
                "project_row_no_a": int(row_a["project_row_no"]),
                "project_row_no_b": int(row_b["project_row_no"]),
            }
        )

    set_a, set_b = set(numbers_a), set(numbers_b)
    edge_infos = _edge_infos(common_rows, set_a, set_b)

    # ---- must-keep rows ----------------------------------------------------
    must_keep_row_nos = sorted(set(must_keep_row_nos))
    unknown: list[int] = []
    conflicts: list[dict[str, Any]] = []
    forced: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for row_no_a in must_keep_row_nos:
        project_row = conn.execute(
            """
            SELECT table_name, row_no FROM project_rows
            WHERE project_id = ? AND project_row_no = ?
            """,
            (project_a, row_no_a),
        ).fetchone()
        if project_row is None:
            unknown.append(row_no_a)
            continue
        key = (project_row["table_name"], int(project_row["row_no"]))
        entry = {
            "row_no_a": row_no_a,
            "table_name": key[0],
            "source_row_no": key[1],
        }
        if key not in delivered_a:
            conflicts.append(
                {
                    **entry,
                    "reason": "not_delivered_in_a",
                    "message": "该行在甲课题尚未成功交付",
                }
            )
            continue
        if key not in delivered_b:
            conflicts.append(
                {
                    **entry,
                    "reason": "not_delivered_in_b",
                    "message": "该行未在乙课题交付，不属于共同交付行",
                }
            )
            continue
        a = int(rows_a[key]["current_student_no"])
        b = int(rows_b[key]["current_student_no"])
        if a not in set_a or b not in set_b:
            conflicts.append(
                {
                    **entry,
                    "reason": "student_no_not_selected",
                    "message": "该行当前的学员号不在本次指定的学员号范围内",
                    "student_no_a": a,
                    "student_no_b": b,
                }
            )
            continue
        forced.setdefault((a, b), []).append(entry)
    if unknown:
        raise ApiError(
            404,
            "anonymous_row_not_found",
            "必留行中的甲课题匿名行号不存在",
            {"row_nos_a": unknown},
        )

    # A forced pair must itself satisfy the two-table qualification.
    for edge, entries in forced.items():
        info = edge_infos.get(edge)
        missing = (
            info["missing_tables"]
            if info is not None
            else list(MERGE_TRIAL_REQUIRED_TABLES)
        )
        if missing:
            for entry in entries:
                conflicts.append(
                    {
                        **entry,
                        "reason": "pair_not_qualified",
                        "message": "该必留行要求的配对在报名表或测评表缺少共同交付行",
                        "student_no_a": edge[0],
                        "student_no_b": edge[1],
                        "missing_tables": missing,
                    }
                )

    # Forced pairs may not share a student number on either side.
    edge_conflicts: dict[tuple[int, int], set[tuple[int, int]]] = {
        edge: set() for edge in forced
    }
    forced_edges = sorted(forced)
    for i, first in enumerate(forced_edges):
        for second in forced_edges[i + 1 :]:
            if first[0] == second[0] or first[1] == second[1]:
                edge_conflicts[first].add(second)
                edge_conflicts[second].add(first)
    for edge, others in edge_conflicts.items():
        if not others:
            continue
        other_rows = sorted(
            entry["row_no_a"] for other in others for entry in forced[other]
        )
        for entry in forced[edge]:
            conflicts.append(
                {
                    **entry,
                    "reason": "pair_conflict",
                    "message": "必留行要求的配对与其他必留行冲突（每个学员号最多配一个对方号码）",
                    "required_pair": {
                        "student_no_a": edge[0],
                        "student_no_b": edge[1],
                    },
                    "conflicts_with_row_nos_a": other_rows,
                }
            )

    if conflicts:
        conflicts.sort(key=lambda item: (item["row_no_a"], item["reason"]))
        raise ApiError(
            409,
            "must_keep_conflict",
            "必留行无法全部保留",
            {"conflicts": conflicts},
        )

    # ---- optimal pairing ----------------------------------------------------
    used_a = {edge[0] for edge in forced}
    used_b = {edge[1] for edge in forced}
    free_a = [a for a in numbers_a if a not in used_a]
    free_b = [b for b in numbers_b if b not in used_b]
    _, free_pairs = _best_matching(free_a, free_b, edge_infos)
    all_pairs = sorted(list(forced) + list(free_pairs))

    pair_items: list[dict[str, Any]] = []
    for a, b in all_pairs:
        rows = sorted(
            edge_infos[(a, b)]["rows"],
            key=lambda rec: (rec["table_name"], rec["source_row_no"]),
        )
        pair_items.append(
            {
                "student_no_a": a,
                "student_no_b": b,
                "row_count": len(rows),
                "registration_row_count": sum(
                    1 for rec in rows if rec["table_name"] == "registration"
                ),
                "assessment_row_count": sum(
                    1 for rec in rows if rec["table_name"] == "assessment"
                ),
                "rows": [
                    {
                        "table_name": rec["table_name"],
                        "source_row_no": rec["source_row_no"],
                        "project_row_no_a": rec["project_row_no_a"],
                        "project_row_no_b": rec["project_row_no_b"],
                    }
                    for rec in rows
                ],
            }
        )

    paired_a = {pair[0] for pair in all_pairs}
    paired_b = {pair[1] for pair in all_pairs}
    digest = fingerprint(
        {
            "project_a": project_a,
            "project_b": project_b,
            "student_numbers_a": numbers_a,
            "student_numbers_b": numbers_b,
            "must_keep_row_nos": must_keep_row_nos,
            "pairs": pair_items,
        }
    )
    return {
        "project_a": project_a,
        "project_b": project_b,
        "student_numbers_a": numbers_a,
        "student_numbers_b": numbers_b,
        "common_delivered_row_count": len(common_rows),
        "pair_count": len(pair_items),
        "preserved_row_count": sum(item["row_count"] for item in pair_items),
        "must_keep_row_nos": must_keep_row_nos,
        "pairs": pair_items,
        "unpaired_student_numbers_a": [a for a in numbers_a if a not in paired_a],
        "unpaired_student_numbers_b": [b for b in numbers_b if b not in paired_b],
        "digest_sha256": digest,
    }
