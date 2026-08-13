from __future__ import annotations

import argparse
import json

from src.gis.static_maps import (
    render_national_static_maps,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Render national static GIS validation maps."
        )
    )

    parser.add_argument(
        "--month",
        required=True,
        help="Reference month in YYYY-MM format.",
    )

    parser.add_argument(
        "--data-root",
        default="dashboard/gis/data",
    )

    parser.add_argument(
        "--output-root",
        default="dashboard/gis/maps",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    results = render_national_static_maps(
        reference_month=args.month,
        data_root=args.data_root,
        output_root=args.output_root,
    )

    print(
        "[OK] static GIS maps complete | "
        f"month={args.month} "
        f"maps={len(results)}"
    )

    print(
        json.dumps(
            results,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()