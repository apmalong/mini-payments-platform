from check_contracts import check, normalize

CONTRACT = {"table": "public.t", "columns": [
    {"name": "id", "type": "bigint", "nullable": False},
    {"name": "amount", "type": "numeric(18,2)", "nullable": False},
    {"name": "note", "type": "text", "nullable": True},
]}
LIVE = {"id": {"type": "bigint", "nullable": False}, "amount": {"type": "numeric(18,2)", "nullable": False},
        "note": {"type": "text", "nullable": True}}


def test_matching_schema_passes():
    assert check(CONTRACT, LIVE) == ([], [])


def test_breaking_changes_are_reported():
    live = {"id": {"type": "text", "nullable": False}, "amount": {"type": "numeric(18,2)", "nullable": True}}
    breaking, _ = check(CONTRACT, live)
    assert any("id changed type" in b for b in breaking)
    assert any("amount must be not null" in b for b in breaking)
    assert any("note is missing" in b for b in breaking)


def test_new_column_is_only_a_warning():
    breaking, warnings = check(CONTRACT, {**LIVE, "extra": {"type": "text", "nullable": True}})
    assert breaking == [] and warnings == ["new column extra (text) is not in the contract yet"]


def test_normalize_types():
    assert normalize("timestamp with time zone", None, None) == "timestamptz"
    assert normalize("numeric", 18, 2) == "numeric(18,2)"
