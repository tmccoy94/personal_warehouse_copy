from pipelines.defs.assets.warehouse_loaders.fct_members import assign_branch_with_fallback


def test_zip_match_takes_priority_over_home_branch():
    # 64151 is in _NORTH_BRANCH_ZIPS — ZIP wins even if CORE says otherwise
    assert assign_branch_with_fallback("64151", "Main Branch") == "North Branch"


def test_op_zip_returns_op_branch_not_overland_park():
    # 66212 is in _OVERLAND_PARK_BRANCH_ZIPS — must return warehouse name 'OP Branch'
    assert assign_branch_with_fallback("66212", None) == "OP Branch"


def test_no_zip_falls_back_to_home_branch_raw():
    assert assign_branch_with_fallback(None, "Main Branch") == "Main Branch"


def test_overland_park_home_branch_normalizes_to_op_branch():
    # CORE returns 'Overland Park Branch' but warehouse renamed the dim_branch entry
    # to 'OP Branch'. The fallback must normalize, otherwise these rows get
    # silently dropped at dim_branch lookup time.
    assert assign_branch_with_fallback(None, "Overland Park Branch") == "OP Branch"
    assert assign_branch_with_fallback("99999", "Overland Park Branch") == "OP Branch"


def test_unmatched_zip_falls_back_to_home_branch():
    # 99999 is not in any territory map
    assert assign_branch_with_fallback("99999", "South Branch") == "South Branch"


def test_no_zip_and_no_home_branch_returns_unassigned():
    # Critical: this row must end up in fct_home_branch_members_summary under
    # the 'Unassigned' branch_key — not silently dropped.
    assert assign_branch_with_fallback(None, None) == "Unassigned"


def test_unmatched_zip_and_no_home_branch_returns_unassigned():
    assert assign_branch_with_fallback("99999", None) == "Unassigned"


def test_non_integer_zip_falls_back_safely():
    # Bad ZIP strings shouldn't raise — they should fall through to home_branch logic
    assert assign_branch_with_fallback("ABCDE", "Main Branch") == "Main Branch"
    assert assign_branch_with_fallback("ABCDE", None) == "Unassigned"


def test_zip_with_dash_extension_uses_first_five_digits():
    # 64151-1234 should still match North territory (first 5 digits = 64151)
    assert assign_branch_with_fallback("64151-1234", None) == "North Branch"
