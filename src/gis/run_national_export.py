from __future__ import annotations

import argparse
import json

from src.gis.national_export import (
    export_national_gis_data,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export national climate-risk GIS "
            "serving datasets."
        )
    )

    parser.add_argument(
        "--gold-root",
        default="lakehouse/gold",
    )

    parser.add_argument(
        "--output-root",
        default="dashboard/gis/data",
    )

    parser.add_argument(
        "--months",
        nargs="*",
        default=None,
        help=(
            "Optional YYYY-MM months. "
            "Exports all months when omitted."
        ),
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    result = export_national_gis_data(
        gold_root=args.gold_root,
        output_root=args.output_root,
        months=args.months,
    )

    print(
        "[OK] national GIS export complete | "
        f"grid_cells={result['grid_cell_count']} "
        f"months={result['month_count']} "
        f"range={result['minimum_month']}"
        f"..{result['maximum_month']}"
    )

    print(
        json.dumps(
            {
                "geometry": (
                    result[
                        "geometry_output_path"
                    ]
                ),
                "manifest": (
                    result[
                        "manifest_output_path"
                    ]
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()