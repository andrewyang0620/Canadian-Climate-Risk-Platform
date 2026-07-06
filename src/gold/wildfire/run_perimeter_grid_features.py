from __future__ import annotations

import argparse
from pathlib import Path

from src.gold.wildfire.perimeter_grid_features import (
    run_gold_grid_month_wildfire_perimeter_feature,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Gold grid-month wildfire perimeter features."
    )

    parser.add_argument(
        "--lakehouse-root",
        default="lakehouse",
        help="Lakehouse root path.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    run_gold_grid_month_wildfire_perimeter_feature(
        lakehouse_root=Path(args.lakehouse_root),
    )


if __name__ == "__main__":
    main()
