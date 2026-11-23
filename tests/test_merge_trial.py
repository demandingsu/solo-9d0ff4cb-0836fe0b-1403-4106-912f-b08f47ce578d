"""Tests for the read-only merge-trial pairing API.

The main fixture builds two projects over the same source rows with different
split histories, so the heaviest single pair (5 rows) is *not* part of the
optimal pairing (4 + 4 + 2 = 10 rows):

===================  ================  ================
common rows          current no. in A  current no. in B
===================  ================  ================
STAY (5 rows)        1                 2
SPLIT_IN_B (4 rows)  1                 3 (split out)
SPLIT_IN_A (4 rows)  3 (split out)     2
CLEAN (2 rows)       2                 1
ONLY_REG (1 row)     4                 4
===================  ================  ================
"""

from __future__ import annotations

import random

import pytest

from app import service
from app.db import connect, init_db, read_tx

STAY = [
    ("registration", 101),
    ("registration", 102),
    ("registration", 103),
    ("assessment", 201),
    ("assessment", 202),
]
SPLIT_IN_B = [
    ("registration", 104),
    ("registration", 105),
    ("assessment", 203),
    ("assessment", 204),
]
SPLIT_IN_A = [
    ("registration", 106),
    ("registration", 107),
    ("assessment", 205),
    ("assessment", 206),
]
CLEAN = [("registration", 108), ("assessment", 207)]
ONLY_REG = [("registration", 109)]


def _items(pairs):
    return [{"table_name": table, "row_no": row_no} for table, row_no in pairs]


def deliver(client, project_id, items, request_id=None):
    body = {"items": items}
    if request_id is not None:
        body["request_id"] = request_id
    response = client.post(f"/projects/{project_id}/deliveries", json=body)
    assert response.status_code == 201, response.text
    return response.json()


def split(client, project_id, merged_student_no, rows):
    response = client.post(
        f"/projects/{project_id}/splits",
        json={"merged_student_no": merged_student_no, "rows": rows},
    )
    assert response.status_code == 201, response.text
    return response.json()


def rows_by_source(client, project_id):
    response = client.get(f"/projects/{project_id}/rows", params={"limit": 1000})
    assert response.status_code == 200, response.text
    return {
        (row["table_name"], row["source_row_no"]): row
        for row in response.json()["items"]
    }


def trial(client, project_a, project_b, numbers_a, numbers_b, must_keep=None):
    body = {
        "project_a": project_a,
        "project_b": project_b,
        "student_numbers_a": numbers_a,
        "student_numbers_b": numbers_b,
    }
    if must_keep is not None:
        body["must_keep_row_nos"] = must_keep
    return client.post("/merge-trials", json=body)


@pytest.fixture()
def merge_projects(client):
    source_rows = [
        *[
            {"table_name": t, "row_no": n, "merged_student_no": "S101"}
            for t, n in STAY + SPLIT_IN_B + SPLIT_IN_A
        ],
        *[
            {"table_name": t, "row_no": n, "merged_student_no": "S102"}
            for t, n in CLEAN
        ],
        {"table_name": "registration", "row_no": 109, "merged_student_no": "S103"},
    ]
    response = client.post("/source-rows", json={"rows": source_rows})
    assert response.status_code == 200, response.text

    project_a = client.post("/projects", json={"name": "甲课题组"}).json()["project_id"]
    project_b = client.post("/projects", json={"name": "乙课题组"}).json()["project_id"]

    # A: S101 -> 1, S102 -> 2, split group -> 3, S103 -> 4.
    deliver(client, project_a, _items(STAY + SPLIT_IN_B + SPLIT_IN_A + CLEAN))
    deliver(client, project_a, _items(STAY))  # duplicate delivery, counts once
    split(client, project_a, "S101", _items(SPLIT_IN_A))
    deliver(client, project_a, _items(ONLY_REG))

    # B: different delivery order, so its anonymous numbers differ from A's.
    # S102 -> 1, S101 -> 2, split group -> 3, S103 -> 4.
    deliver(client, project_b, _items(CLEAN + SPLIT_IN_A + SPLIT_IN_B + STAY))
    split(client, project_b, "S101", _items(SPLIT_IN_B))
    deliver(client, project_b, _items(ONLY_REG))
    return project_a, project_b


def test_pairing_prefers_global_optimum_over_best_single_pair(client, merge_projects):
    project_a, project_b = merge_projects
    response = trial(client, project_a, project_b, [1, 2, 3, 4], [1, 2, 3, 4])
    assert response.status_code == 200, response.text
    result = response.json()

    # 13 S101 rows + 2 S102 rows + 1 S103 row, duplicate deliveries count once.
    assert result["common_delivered_row_count"] == 16

    pairs = {(p["student_no_a"], p["student_no_b"]): p for p in result["pairs"]}
    # The heaviest single pair (1, 2) keeps 5 rows but blocks (1, 3) and
    # (3, 2); the optimum keeps 4 + 4 + 2 = 10 rows instead of 5 + 2 = 7.
    assert (1, 2) not in pairs
    assert set(pairs) == {(1, 3), (3, 2), (2, 1)}
    assert result["preserved_row_count"] == 10
    assert result["pair_count"] == 3

    # Each pair lists the usable rows with both projects' anonymous numbers.
    rows_a = rows_by_source(client, project_a)
    rows_b = rows_by_source(client, project_b)
    pair_rows = {
        (r["table_name"], r["source_row_no"]): r for r in pairs[(1, 3)]["rows"]
    }
    assert set(pair_rows) == set(SPLIT_IN_B)
    for key, row in pair_rows.items():
        assert row["project_row_no_a"] == rows_a[key]["project_row_no"]
        assert row["project_row_no_b"] == rows_b[key]["project_row_no"]
    # A and B numbered their rows independently.
    assert {r["project_row_no_a"] for r in pair_rows.values()} == {6, 7, 8, 9}
    assert {r["project_row_no_b"] for r in pair_rows.values()} == {7, 8, 9, 10}
    assert pairs[(1, 3)]["registration_row_count"] == 2
    assert pairs[(1, 3)]["assessment_row_count"] == 2


def test_pair_requires_common_rows_in_both_tables(client, merge_projects):
    project_a, project_b = merge_projects
    result = trial(client, project_a, project_b, [1, 2, 3, 4], [1, 2, 3, 4]).json()
    # S103 shares only a registration row: the (4, 4) pair is not qualified.
    assert result["unpaired_student_numbers_a"] == [4]
    assert result["unpaired_student_numbers_b"] == [4]
    preserved_keys = {
        (row["table_name"], row["source_row_no"])
        for pair in result["pairs"]
        for row in pair["rows"]
    }
    assert ("registration", 109) not in preserved_keys


def test_trial_result_is_deterministic(client, merge_projects):
    project_a, project_b = merge_projects
    first = trial(client, project_a, project_b, [1, 2, 3, 4], [1, 2, 3, 4]).json()
    second = trial(client, project_a, project_b, [1, 2, 3, 4], [1, 2, 3, 4]).json()
    assert first == second
    assert first["digest_sha256"] == second["digest_sha256"]


def test_must_keep_rows_are_all_preserved(client, merge_projects):
    project_a, project_b = merge_projects
    rows_a = rows_by_source(client, project_a)
    keep = [rows_a[("registration", 101)]["project_row_no"]]  # forces pair (1, 2)
    response = trial(client, project_a, project_b, [1, 2, 3], [1, 2, 3], must_keep=keep)
    assert response.status_code == 200, response.text
    result = response.json()

    pairs = {(p["student_no_a"], p["student_no_b"]) for p in result["pairs"]}
    assert (1, 2) in pairs
    # Forcing (1, 2) blocks (1, 3) and (3, 2): only (2, 1) can still be added,
    # so the total drops from 10 to 5 + 2 = 7.  The must-keep row is kept.
    assert result["preserved_row_count"] == 7
    assert result["must_keep_row_nos"] == keep
    kept_rows = {
        row["project_row_no_a"] for pair in result["pairs"] for row in pair["rows"]
    }
    assert keep[0] in kept_rows


def test_conflicting_must_keep_rows_are_reported(client, merge_projects):
    project_a, project_b = merge_projects
    rows_a = rows_by_source(client, project_a)
    keep = [
        rows_a[("registration", 101)]["project_row_no"],  # needs pair (1, 2)
        rows_a[("registration", 104)]["project_row_no"],  # needs pair (1, 3)
    ]
    response = trial(client, project_a, project_b, [1, 2, 3], [1, 2, 3], must_keep=keep)
    assert response.status_code == 409, response.text
    error = response.json()["error"]
    assert error["code"] == "must_keep_conflict"

    conflicts = error["details"]["conflicts"]
    # Both rows demand student number 1 of project A: they cannot both be kept.
    assert {c["row_no_a"] for c in conflicts} == set(keep)
    assert all(c["reason"] == "pair_conflict" for c in conflicts)
    by_row = {c["row_no_a"]: c for c in conflicts}
    assert by_row[keep[0]]["required_pair"] == {"student_no_a": 1, "student_no_b": 2}
    assert by_row[keep[0]]["conflicts_with_row_nos_a"] == [keep[1]]
    assert by_row[keep[1]]["required_pair"] == {"student_no_a": 1, "student_no_b": 3}
    assert by_row[keep[1]]["conflicts_with_row_nos_a"] == [keep[0]]


def test_must_keep_row_not_common_delivered(client, merge_projects):
    project_a, project_b = merge_projects
    client.post(
        "/source-rows",
        json={
            "rows": [
                {"table_name": "registration", "row_no": 110, "merged_student_no": "S104"}
            ]
        },
    )
    deliver(client, project_a, [{"table_name": "registration", "row_no": 110}])
    keep = [rows_by_source(client, project_a)[("registration", 110)]["project_row_no"]]

    response = trial(client, project_a, project_b, [1], [1], must_keep=keep)
    assert response.status_code == 409, response.text
    conflicts = response.json()["error"]["details"]["conflicts"]
    assert [c["reason"] for c in conflicts] == ["not_delivered_in_b"]
    assert conflicts[0]["row_no_a"] == keep[0]


def test_must_keep_row_outside_selected_student_numbers(client, merge_projects):
    project_a, project_b = merge_projects
    rows_a = rows_by_source(client, project_a)
    # assessment:205 currently belongs to student number 3 in A and 2 in B.
    keep = [rows_a[("assessment", 205)]["project_row_no"]]
    response = trial(client, project_a, project_b, [1, 2], [1, 2], must_keep=keep)
    assert response.status_code == 409, response.text
    conflicts = response.json()["error"]["details"]["conflicts"]
    assert [c["reason"] for c in conflicts] == ["student_no_not_selected"]
    assert conflicts[0]["student_no_a"] == 3
    assert conflicts[0]["student_no_b"] == 2


def test_must_keep_row_whose_pair_is_not_qualified(client, merge_projects):
    project_a, project_b = merge_projects
    rows_a = rows_by_source(client, project_a)
    # S103 has only a registration row: pair (4, 4) lacks assessment rows.
    keep = [rows_a[("registration", 109)]["project_row_no"]]
    response = trial(client, project_a, project_b, [1, 2, 3, 4], [1, 2, 3, 4], must_keep=keep)
    assert response.status_code == 409, response.text
    conflicts = response.json()["error"]["details"]["conflicts"]
    assert [c["reason"] for c in conflicts] == ["pair_not_qualified"]
    assert conflicts[0]["student_no_a"] == 4
    assert conflicts[0]["student_no_b"] == 4
    assert conflicts[0]["missing_tables"] == ["assessment"]


def test_must_keep_unknown_row_no(client, merge_projects):
    project_a, project_b = merge_projects
    response = trial(client, project_a, project_b, [1], [1], must_keep=[99999])
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "anonymous_row_not_found"
    assert response.json()["error"]["details"]["row_nos_a"] == [99999]


def test_unknown_student_number(client, merge_projects):
    project_a, project_b = merge_projects
    response = trial(client, project_a, project_b, [1, 99], [1])
    assert response.status_code == 404
    error = response.json()["error"]
    assert error["code"] == "student_no_not_found"
    assert error["details"]["project_id"] == project_a
    assert error["details"]["student_numbers"] == [99]


def test_unknown_project(client, merge_projects):
    project_a, _ = merge_projects
    response = trial(client, "prj_missing", project_a, [1], [1])
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "project_not_found"


def test_request_validation(client, merge_projects):
    project_a, project_b = merge_projects

    same = trial(client, project_a, project_a, [1], [1])
    assert same.status_code == 422

    too_many = trial(client, project_a, project_b, list(range(1, 14)), [1])
    assert too_many.status_code == 422

    duplicated = trial(client, project_a, project_b, [1, 1], [1])
    assert duplicated.status_code == 422

    duplicated_keep = trial(client, project_a, project_b, [1], [1], must_keep=[2, 2])
    assert duplicated_keep.status_code == 422

    extra_field = client.post(
        "/merge-trials",
        json={
            "project_a": project_a,
            "project_b": project_b,
            "student_numbers_a": [1],
            "student_numbers_b": [1],
            "surprise": True,
        },
    )
    assert extra_field.status_code == 422
    assert extra_field.json()["error"]["code"] == "validation_error"


def test_trial_is_read_only(client, merge_projects):
    project_a, project_b = merge_projects
    before = {}
    for project_id in (project_a, project_b):
        before[project_id] = {
            "rows": client.get(f"/projects/{project_id}/rows", params={"limit": 1000}).json(),
            "deliveries": client.get(f"/projects/{project_id}/deliveries").json(),
            "corrections": client.get(f"/projects/{project_id}/corrections").json(),
            "splits": client.get(f"/projects/{project_id}/splits").json(),
        }
    response = trial(
        client, project_a, project_b, [1, 2, 3, 4], [1, 2, 3, 4], must_keep=[1]
    )
    assert response.status_code == 200, response.text
    for project_id in (project_a, project_b):
        assert client.get(f"/projects/{project_id}/rows", params={"limit": 1000}).json() == before[project_id]["rows"]
        assert client.get(f"/projects/{project_id}/deliveries").json() == before[project_id]["deliveries"]
        assert client.get(f"/projects/{project_id}/corrections").json() == before[project_id]["corrections"]
        assert client.get(f"/projects/{project_id}/splits").json() == before[project_id]["splits"]


def test_trial_reads_a_single_committed_snapshot(tmp_path):
    """A delivery committed mid-trial must not leak into the trial's reads."""
    db_path = str(tmp_path / "snapshot.db")
    init_db(db_path)
    setup = connect(db_path)
    try:
        service.upsert_source_rows(
            setup,
            [
                {"table_name": "registration", "row_no": 301, "merged_student_no": "S201"},
                {"table_name": "assessment", "row_no": 301, "merged_student_no": "S201"},
                {"table_name": "assessment", "row_no": 302, "merged_student_no": "S201"},
            ],
        )
        project_a = service.create_project(setup, "甲")["project_id"]
        project_b = service.create_project(setup, "乙")["project_id"]
        for project_id in (project_a, project_b):
            service.deliver(
                setup,
                project_id,
                [
                    {"table_name": "registration", "row_no": 301},
                    {"table_name": "assessment", "row_no": 301},
                ],
                request_id=f"{project_id}-d1",
            )
    finally:
        setup.close()

    conn1 = connect(db_path)
    conn2 = connect(db_path)
    try:
        with read_tx(conn1):
            # Pin conn1's snapshot, then commit a new delivery from conn2.
            conn1.execute("SELECT COUNT(*) FROM projects").fetchone()
            for project_id in (project_a, project_b):
                service.deliver(
                    conn2,
                    project_id,
                    [{"table_name": "assessment", "row_no": 302}],
                    request_id=f"{project_id}-d2",
                )
            result = service.merge_trial(conn1, project_a, project_b, [1], [1], [])
        # The trial saw only the state committed before its snapshot.
        assert result["common_delivered_row_count"] == 2
        assert result["preserved_row_count"] == 2
    finally:
        conn1.close()
        conn2.close()

    conn3 = connect(db_path)
    try:
        with read_tx(conn3):
            fresh = service.merge_trial(conn3, project_a, project_b, [1], [1], [])
        assert fresh["common_delivered_row_count"] == 3
        assert fresh["preserved_row_count"] == 3
    finally:
        conn3.close()


def test_best_matching_matches_brute_force():
    rng = random.Random(20261123)
    for _ in range(300):
        size_a = rng.randint(1, 5)
        size_b = rng.randint(1, 5)
        edge_infos = {}
        for a in range(1, size_a + 1):
            for b in range(1, size_b + 1):
                if rng.random() < 0.6:
                    edge_infos[(a, b)] = {
                        "weight": rng.randint(1, 9),
                        "qualified": rng.random() < 0.8,
                    }
        free_a = list(range(1, size_a + 1))
        free_b = list(range(1, size_b + 1))
        weight, pairs = service._best_matching(free_a, free_b, edge_infos)

        # Brute-force reference over all matchings.
        best = 0

        def brute(i, used, total):
            nonlocal best
            if i == len(free_a):
                best = max(best, total)
                return
            brute(i + 1, used, total)
            for b in free_b:
                info = edge_infos.get((free_a[i], b))
                if b not in used and info is not None and info["qualified"]:
                    brute(i + 1, used | {b}, total + info["weight"])

        brute(0, set(), 0)
        assert weight == best
        # Structural invariants of the returned pairing.
        assert len({a for a, _ in pairs}) == len(pairs)
        assert len({b for _, b in pairs}) == len(pairs)
        assert weight == sum(edge_infos[pair]["weight"] for pair in pairs)


def test_demo_seed_merge_trial(client):
    first = client.post("/demo/seed-merge-trial")
    assert first.status_code == 200, first.text
    assert first.json()["seeded"] is True
    project_a = first.json()["project_a"]
    project_b = first.json()["project_b"]
    assert project_a != project_b

    second = client.post("/demo/seed-merge-trial")
    assert second.json()["seeded"] is False
    assert second.json()["project_a"] == project_a
    assert second.json()["project_b"] == project_b

    result = trial(client, project_a, project_b, [1, 2, 3], [1, 2, 3])
    assert result.status_code == 200, result.text
    assert result.json()["preserved_row_count"] == 10
    assert result.json()["common_delivered_row_count"] == 15
