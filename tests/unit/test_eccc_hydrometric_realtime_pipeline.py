from unittest.mock import Mock, patch

from src.pipelines.run_eccc_hydrometric_realtime import (
    run_eccc_hydrometric_realtime_pipeline,
)


@patch(
    "src.pipelines.run_eccc_hydrometric_realtime."
    "validate_eccc_hydro_realtime_observation_silver_outputs"
)
@patch("src.pipelines.run_eccc_hydrometric_realtime." "run_eccc_hydro_realtime_observation_silver")
@patch("src.pipelines.run_eccc_hydrometric_realtime." "download_eccc_hydrometric_realtime")
def test_realtime_pipeline_success(
    mock_download,
    mock_silver,
    mock_validation,
    tmp_path,
):
    mock_download.return_value = {
        "run_id": "bronze-run",
        "raw_file_path": "bronze.jsonl.gz",
        "row_count": 100,
    }

    mock_silver.return_value = {
        "run_id": "silver-run",
        "output_path": "silver.parquet",
        "row_count": 100,
        "station_count": 2,
    }

    validation_report = Mock()
    validation_report.passed = True
    validation_report.checks = [Mock()]
    validation_report.to_dict.return_value = {
        "passed": True,
        "checks": [
            {
                "name": "example_check",
                "passed": True,
            }
        ],
    }
    mock_validation.return_value = validation_report

    report = run_eccc_hydrometric_realtime_pipeline(
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        pipeline_report_root=tmp_path / "pipeline_runs",
    )

    assert report["status"] == "success"
    assert len(report["steps"]) == 3
    assert report["steps"][0]["name"] == "bronze_ingestion"
    assert report["steps"][1]["name"] == "silver_build"
    assert report["steps"][2]["name"] == "silver_validation"

    silver_call_kwargs = mock_silver.call_args.kwargs
    assert silver_call_kwargs["raw_path"] == "bronze.jsonl.gz"


@patch("src.pipelines.run_eccc_hydrometric_realtime." "download_eccc_hydrometric_realtime")
def test_realtime_pipeline_records_failure(
    mock_download,
    tmp_path,
):
    mock_download.side_effect = RuntimeError("API unavailable")

    report = run_eccc_hydrometric_realtime_pipeline(
        bronze_root=tmp_path / "bronze",
        silver_root=tmp_path / "silver",
        pipeline_report_root=tmp_path / "pipeline_runs",
    )

    assert report["status"] == "failed"
    assert report["error"]["type"] == "RuntimeError"
    assert "API unavailable" in report["error"]["message"]
