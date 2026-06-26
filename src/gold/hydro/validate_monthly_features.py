from __future__ import annotations

import json

from src.gold.hydro.validation import validate_hydro_monthly_feature_outputs


def main() -> None:
    report = validate_hydro_monthly_feature_outputs()

    print(json.dumps(report.to_dict(), indent=2))

    if not report.passed:
        raise SystemExit("[FAIL] Gold hydro monthly feature validation failed.")

    print("[OK] Gold hydro monthly feature validation passed | " f"checks={len(report.checks)}")


if __name__ == "__main__":
    main()
