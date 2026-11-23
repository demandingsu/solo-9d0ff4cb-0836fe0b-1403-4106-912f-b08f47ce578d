"""Deterministic demo data for ``POST /demo/seed``.

The scenario is designed so that it exercises every rule in one run:

* two source tables (``registration`` and ``assessment``) share merged student
  numbers, so the project student number is shared across tables;
* merged student ``S002`` is an accidental merge of two real people.  It is
  delivered before the split, split, and delivered again afterwards, so its
  correction sheet contains the interspersed deliveries while the later
  delivery uses the new student number;
* a second split (``S003``) only touches a row that was never delivered, so it
  must produce an empty correction sheet.

Seeding is idempotent: it returns the project it created on the first call.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from . import service

DEMO_PROJECT_NAME = "演示课题组：培训报名与测评"

SOURCE_ROWS: list[dict[str, Any]] = [
    # S001: one learner, one row in each table.
    {"table_name": "registration", "row_no": 1, "merged_student_no": "S001"},
    {"table_name": "assessment", "row_no": 10, "merged_student_no": "S001"},
    # S002: two people accidentally merged, three rows in two tables.
    {"table_name": "registration", "row_no": 2, "merged_student_no": "S002"},
    {"table_name": "assessment", "row_no": 11, "merged_student_no": "S002"},
    {"table_name": "assessment", "row_no": 12, "merged_student_no": "S002"},
    # S003: never delivered before its split.
    {"table_name": "registration", "row_no": 3, "merged_student_no": "S003"},
    {"table_name": "assessment", "row_no": 13, "merged_student_no": "S003"},
    # S004: delivered but never split.
    {"table_name": "registration", "row_no": 4, "merged_student_no": "S004"},
    {"table_name": "assessment", "row_no": 14, "merged_student_no": "S004"},
]


def seed_demo(conn: sqlite3.Connection) -> dict[str, Any]:
    existing = service.find_project_by_name(conn, DEMO_PROJECT_NAME)
    if existing is not None:
        return {
            "project_id": existing,
            "seeded": False,
            "message": "演示数据已存在，未重复写入",
            "summary": _summary(conn, existing),
        }

    from .db import write_tx

    with write_tx(conn):
        service.upsert_source_rows_tx(conn, SOURCE_ROWS)
        project = service.create_project_tx(conn, DEMO_PROJECT_NAME)
        project_id = project["project_id"]

        # 1. First delivery: every row of S002 and S004, plus S001's two rows.
        first = service.deliver_tx(
            conn,
            project_id,
            [
                {"table_name": "registration", "row_no": 1},
                {"table_name": "assessment", "row_no": 10},
                {"table_name": "registration", "row_no": 2},
                {"table_name": "assessment", "row_no": 11},
                {"table_name": "registration", "row_no": 4},
                {"table_name": "assessment", "row_no": 14},
            ],
            request_id="demo-delivery-1",
        )
        # 2. A second delivery interleaved after the first: S002's third row.
        service.deliver_tx(
            conn,
            project_id,
            [{"table_name": "assessment", "row_no": 12}],
            request_id="demo-delivery-2",
        )
        # 3. Split the accidental merge of S002: two of its three rows become
        #    a new student, one row stays with S002.  Both moved rows were
        #    already delivered under the old number.
        split = service.split_student_tx(
            conn,
            project_id,
            "S002",
            [
                {"table_name": "assessment", "row_no": 11},
                {"table_name": "assessment", "row_no": 12},
            ],
            request_id="demo-split-1",
        )
        # 4. Delivery after the split: the moved row comes out with the new
        #    number and must not generate an extra correction line.
        service.deliver_tx(
            conn,
            project_id,
            [
                {"table_name": "assessment", "row_no": 11},
                {"table_name": "registration", "row_no": 2},
            ],
            request_id="demo-delivery-3",
        )
        # 5. Split rows that were never delivered: empty correction sheet.
        service.split_student_tx(
            conn,
            project_id,
            "S003",
            [
                {"table_name": "registration", "row_no": 3},
                {"table_name": "assessment", "row_no": 13},
            ],
            request_id="demo-split-2",
        )

        summary = _summary(conn, project_id)
        summary["first_delivery_id"] = first["delivery_id"]
        summary["first_split_id"] = split["split_id"]
    return {
        "project_id": project_id,
        "seeded": True,
        "message": "演示数据已生成",
        "summary": summary,
    }


def _summary(conn: sqlite3.Connection, project_id: str) -> dict[str, Any]:
    counts = {}
    for key, sql in {
        "source_rows": "SELECT COUNT(*) AS n FROM source_rows",
        "project_rows": "SELECT COUNT(*) AS n FROM project_rows WHERE project_id = ?",
        "deliveries": "SELECT COUNT(*) AS n FROM deliveries WHERE project_id = ?",
        "delivery_items": "SELECT COUNT(*) AS n FROM delivery_items WHERE project_id = ?",
        "splits": "SELECT COUNT(*) AS n FROM splits WHERE project_id = ?",
        "correction_lines": "SELECT COUNT(*) AS n FROM correction_items WHERE project_id = ?",
    }.items():
        params = () if key == "source_rows" else (project_id,)
        counts[key] = int(conn.execute(sql, params).fetchone()["n"])
    return counts


# ---------------------------------------------------------------------------
# Merge-trial demo: two projects over the same rows, split differently
# ---------------------------------------------------------------------------

MERGE_TRIAL_PROJECT_A_NAME = "演示课题组甲：合并试算"
MERGE_TRIAL_PROJECT_B_NAME = "演示课题组乙：合并试算"

# S101's 13 rows fall into three groups: rows both projects keep under the
# merged number, rows only project B splits out, and rows only project A
# splits out.  The resulting edge weights are 5 / 4 / 4, so the heaviest
# single pair (5) is *not* part of the optimal pairing (4 + 4 + 2 = 10).
_MERGE_TRIAL_STAY = [
    ("registration", 101),
    ("registration", 102),
    ("registration", 103),
    ("assessment", 201),
    ("assessment", 202),
]
_MERGE_TRIAL_SPLIT_IN_B = [
    ("registration", 104),
    ("registration", 105),
    ("assessment", 203),
    ("assessment", 204),
]
_MERGE_TRIAL_SPLIT_IN_A = [
    ("registration", 106),
    ("registration", 107),
    ("assessment", 205),
    ("assessment", 206),
]
_MERGE_TRIAL_CLEAN = [("registration", 108), ("assessment", 207)]

MERGE_TRIAL_SOURCE_ROWS: list[dict[str, Any]] = [
    *[
        {"table_name": table, "row_no": row_no, "merged_student_no": "S101"}
        for table, row_no in (
            _MERGE_TRIAL_STAY + _MERGE_TRIAL_SPLIT_IN_B + _MERGE_TRIAL_SPLIT_IN_A
        )
    ],
    *[
        {"table_name": table, "row_no": row_no, "merged_student_no": "S102"}
        for table, row_no in _MERGE_TRIAL_CLEAN
    ],
]


def _items(pairs: list[tuple[str, int]]) -> list[dict[str, Any]]:
    return [{"table_name": table, "row_no": row_no} for table, row_no in pairs]


def seed_merge_trial(conn: sqlite3.Connection) -> dict[str, Any]:
    existing_a = service.find_project_by_name(conn, MERGE_TRIAL_PROJECT_A_NAME)
    existing_b = service.find_project_by_name(conn, MERGE_TRIAL_PROJECT_B_NAME)
    if existing_a is not None and existing_b is not None:
        return {
            "project_a": existing_a,
            "project_b": existing_b,
            "seeded": False,
            "message": "合并试算演示数据已存在，未重复写入",
            "summary": _merge_trial_summary(),
        }

    from .db import write_tx

    with write_tx(conn):
        service.upsert_source_rows_tx(conn, MERGE_TRIAL_SOURCE_ROWS)
        project_a = service.create_project_tx(conn, MERGE_TRIAL_PROJECT_A_NAME)[
            "project_id"
        ]
        project_b = service.create_project_tx(conn, MERGE_TRIAL_PROJECT_B_NAME)[
            "project_id"
        ]
        all_rows = _items(
            _MERGE_TRIAL_STAY
            + _MERGE_TRIAL_SPLIT_IN_B
            + _MERGE_TRIAL_SPLIT_IN_A
            + _MERGE_TRIAL_CLEAN
        )

        # Project A: deliver everything, re-deliver the stay group (a duplicate
        # delivery still counts once), then split out its own group.
        service.deliver_tx(conn, project_a, all_rows, request_id="merge-demo-a-d1")
        service.deliver_tx(
            conn, project_a, _items(_MERGE_TRIAL_STAY), request_id="merge-demo-a-d2"
        )
        service.split_student_tx(
            conn,
            project_a,
            "S101",
            _items(_MERGE_TRIAL_SPLIT_IN_A),
            request_id="merge-demo-a-split",
        )

        # Project B: same rows, but it splits out a different group.
        service.deliver_tx(conn, project_b, all_rows, request_id="merge-demo-b-d1")
        service.split_student_tx(
            conn,
            project_b,
            "S101",
            _items(_MERGE_TRIAL_SPLIT_IN_B),
            request_id="merge-demo-b-split",
        )

    return {
        "project_a": project_a,
        "project_b": project_b,
        "seeded": True,
        "message": "合并试算演示数据已生成",
        "summary": _merge_trial_summary(),
    }


def _merge_trial_summary() -> dict[str, Any]:
    return {
        "student_numbers_a": [1, 2, 3],
        "student_numbers_b": [1, 2, 3],
        "expected_preserved_row_count": 10,
        "hint": (
            "POST /merge-trials：project_a/project_b 用返回的两个课题，"
            "student_numbers_a/student_numbers_b 各填 [1, 2, 3]；"
            "must_keep_row_nos 可填甲课题匿名行号（如 [1] 强制保留第一对，"
            "[1, 6] 触发必留行冲突）"
        ),
    }
