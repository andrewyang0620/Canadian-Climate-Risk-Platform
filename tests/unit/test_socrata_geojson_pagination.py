import json

from src.ingestion.downloaders.http_downloader import HttpDownloadResult
from src.ingestion.downloaders.socrata import SocrataDownloader


class FakeHttpDownloader:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None):
        self.calls.append((url, params or {}))

        if params and params.get("$select") == "count(*)":
            return HttpDownloadResult(
                url=url,
                final_url=url,
                status_code=200,
                content_type="application/json",
                content=json.dumps([{"count": "3"}]).encode("utf-8"),
                size_bytes=16,
                checksum="count-checksum",
                downloaded_at="2026-05-09T00:00:00+00:00",
            )

        offset = int((params or {}).get("$offset", 0))
        limit = int((params or {}).get("$limit", 50000))

        all_features = [
            {
                "type": "Feature",
                "properties": {"id": "1"},
                "geometry": {"type": "Point", "coordinates": [-114.0, 51.0]},
            },
            {
                "type": "Feature",
                "properties": {"id": "2"},
                "geometry": {"type": "Point", "coordinates": [-114.1, 51.1]},
            },
            {
                "type": "Feature",
                "properties": {"id": "3"},
                "geometry": {"type": "Point", "coordinates": [-114.2, 51.2]},
            },
        ]

        page_features = all_features[offset : offset + limit]

        payload = {
            "type": "FeatureCollection",
            "features": page_features,
        }

        content = json.dumps(payload).encode("utf-8")

        return HttpDownloadResult(
            url=url,
            final_url=url,
            status_code=200,
            content_type="application/geo+json",
            content=content,
            size_bytes=len(content),
            checksum="page-checksum",
            downloaded_at="2026-05-09T00:00:00+00:00",
        )


def test_socrata_geojson_download_is_paginated_and_row_count_validated():
    downloader = SocrataDownloader(
        http_downloader=FakeHttpDownloader(),
        default_page_limit=2,
    )

    result = downloader.download_dataset(
        domain="data.example.com",
        dataset_id="abcd-1234",
        export_format="geojson",
    )

    payload = json.loads(result.download.content.decode("utf-8"))

    assert result.expected_row_count == 3
    assert result.actual_row_count == 3
    assert result.pages_downloaded == 2
    assert result.row_count_validation_supported is True
    assert result.row_count_validation_passed is True
    assert payload["type"] == "FeatureCollection"
    assert len(payload["features"]) == 3
