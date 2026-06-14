from src.silver.validate_all_core_sources import build_validation_summary


def test_build_validation_summary_passes_when_all_results_pass():
    summary = build_validation_summary(
        generated_at="2026-05-25T00:00:00+00:00",
        results=[
            {
                "source_name": "a",
                "passed": True,
                "check_count": 2,
                "passed_check_count": 2,
                "failed_check_count": 0,
                "failed_checks": [],
            },
            {
                "source_name": "b",
                "passed": True,
                "check_count": 3,
                "passed_check_count": 3,
                "failed_check_count": 0,
                "failed_checks": [],
            },
        ],
    )

    assert summary["passed"] is True
    assert summary["source_count"] == 2
    assert summary["passed_source_count"] == 2
    assert summary["failed_source_count"] == 0
    assert summary["total_check_count"] == 5
    assert summary["total_failed_check_count"] == 0


def test_build_validation_summary_fails_when_any_result_fails():
    summary = build_validation_summary(
        generated_at="2026-05-25T00:00:00+00:00",
        results=[
            {
                "source_name": "a",
                "passed": True,
                "check_count": 2,
                "passed_check_count": 2,
                "failed_check_count": 0,
                "failed_checks": [],
            },
            {
                "source_name": "b",
                "passed": False,
                "check_count": 3,
                "passed_check_count": 2,
                "failed_check_count": 1,
                "failed_checks": ["bad_check"],
            },
        ],
    )

    assert summary["passed"] is False
    assert summary["source_count"] == 2
    assert summary["passed_source_count"] == 1
    assert summary["failed_source_count"] == 1
    assert summary["total_check_count"] == 5
    assert summary["total_failed_check_count"] == 1
