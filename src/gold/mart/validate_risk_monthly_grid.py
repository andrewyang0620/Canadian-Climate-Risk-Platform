from __future__ import annotations

import json

from src.gold.mart.validation import validate_risk_monthly_grid_outputs


def main() -> None:
    report = validate_risk_monthly_grid_outputs()

    print(json.dumps(report.to_dict(), indent=2))

    if not report.passed:
        raise SystemExit("[FAIL] Gold grid-month risk mart validation failed.")

    print("[OK] Gold grid-month risk mart validation passed | " f"checks={len(report.checks)}")


if __name__ == "__main__":
    main()
