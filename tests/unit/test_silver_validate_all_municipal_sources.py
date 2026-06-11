from src.silver.validate_all_municipal_sources import summarize_validation_report_dicts


def test_summarize_validation_report_dicts_all_passed():
    report_dicts = [
        {
            "validator_name": "one",
            "validation_name": "validation_one",
            "passed": True,
            "checks": [
                {"name": "check_1", "passed": True, "details": {}},
                {"name": "check_2", "passed": True, "details": {}},
            ],
        },
        {
            "validator_name": "two",
            "validation_name": "validation_two",
            "passed": True,
            "checks": [
                {"name": "check_3", "passed": True, "details": {}},
            ],
        },
    ]

    summary = summarize_validation_report_dicts(report_dicts)

    assert summary["report_count"] == 2
    assert summary["passed_report_count"] == 2
    assert summary["failed_report_count"] == 0
    assert summary["check_count"] == 3
    assert summary["passed_check_count"] == 3
    assert summary["failed_check_count"] == 0
    assert summary["failed_validations"] == []
    assert summary["failed_checks"] == []


def test_summarize_validation_report_dicts_captures_failed_checks():
    report_dicts = [
        {
            "validator_name": "one",
            "validation_name": "validation_one",
            "passed": False,
            "checks": [
                {"name": "check_1", "passed": True, "details": {}},
                {
                    "name": "check_2",
                    "passed": False,
                    "details": {"reason": "bad value"},
                },
            ],
        }
    ]

    summary = summarize_validation_report_dicts(report_dicts)

    assert summary["report_count"] == 1
    assert summary["passed_report_count"] == 0
    assert summary["failed_report_count"] == 1
    assert summary["check_count"] == 2
    assert summary["passed_check_count"] == 1
    assert summary["failed_check_count"] == 1
    assert summary["failed_validations"] == ["validation_one"]
    assert summary["failed_checks"] == [
        {
            "validator_name": "one",
            "validation_name": "validation_one",
            "check_name": "check_2",
            "details": {"reason": "bad value"},
        }
    ]
