#!/usr/bin/env python3
"""Download StatsBomb Open Data into a local folder.

The script fetches the repository structure from the public GitHub raw files,
then downloads:
- competitions.json
- all match files for every competition/season found in competitions.json
- all event files for every match
- all lineup files for every match
- any available three-sixty files for matches that have them

This keeps the data organized locally so it can be loaded later with Spark or
any other processing pipeline.
"""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import json
import ssl
import sys
import time
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import certifi


BASE_RAW_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


def fetch_json(url: str, retries: int = 3, timeout: int = 60):
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(request, timeout=timeout, context=SSL_CONTEXT) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.5 * attempt)
            else:
                raise RuntimeError(f"Failed to fetch JSON from {url}") from exc
    if last_error:
        raise RuntimeError(f"Failed to fetch JSON from {url}") from last_error


def download_file(
    url: str,
    destination: Path,
    retries: int = 3,
    timeout: int = 60,
    allow_missing: bool = False,
) -> str:
    if destination.exists() and destination.stat().st_size > 0:
        return "skipped"

    destination.parent.mkdir(parents=True, exist_ok=True)
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(request, timeout=timeout, context=SSL_CONTEXT) as response:
                data = response.read()
            destination.write_bytes(data)
            return "downloaded"
        except HTTPError as exc:
            if allow_missing and exc.code == 404:
                return "missing"
            last_error = exc
            if attempt < retries:
                time.sleep(1.5 * attempt)
            else:
                raise RuntimeError(f"Failed to download {url}") from exc
        except (URLError, TimeoutError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(1.5 * attempt)
            else:
                raise RuntimeError(f"Failed to download {url}") from exc
    if last_error:
        raise RuntimeError(f"Failed to download {url}") from last_error
    return False


def build_match_targets(base_dir: Path, competitions: Iterable[dict]) -> list[tuple[str, Path]]:
    targets: list[tuple[str, Path]] = []
    seen_match_ids: set[int] = set()

    for competition in competitions:
        competition_id = competition.get("competition_id")
        season_id = competition.get("season_id")
        if competition_id is None or season_id is None:
            continue

        matches_url = f"{BASE_RAW_URL}/matches/{competition_id}/{season_id}.json"
        matches = fetch_json(matches_url)
        matches_dir = base_dir / "matches" / str(competition_id)
        matches_dir.mkdir(parents=True, exist_ok=True)
        (matches_dir / f"{season_id}.json").write_text(
            json.dumps(matches, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        for match in matches:
            match_id = match.get("match_id")
            if match_id is None or match_id in seen_match_ids:
                continue
            seen_match_ids.add(match_id)

            targets.append(
                (
                    f"{BASE_RAW_URL}/events/{match_id}.json",
                    base_dir / "events" / f"{match_id}.json",
                )
            )
            targets.append(
                (
                    f"{BASE_RAW_URL}/lineups/{match_id}.json",
                    base_dir / "lineups" / f"{match_id}.json",
                )
            )
            targets.append(
                (
                    f"{BASE_RAW_URL}/three-sixty/{match_id}.json",
                    base_dir / "three-sixty" / f"{match_id}.json",
                )
            )

    return targets


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download all StatsBomb Open Data matches, events, lineups, and any available 360 data."
    )
    parser.add_argument(
        "--output-dir",
        default="statsbomb-open-data",
        help="Directory where the data will be stored.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of concurrent downloads to run.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="How many times each request should be retried.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Network timeout in seconds for each request.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    competitions_url = f"{BASE_RAW_URL}/competitions.json"
    competitions = fetch_json(competitions_url, retries=args.retries, timeout=args.timeout)
    (output_dir / "competitions.json").write_text(
        json.dumps(competitions, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    targets = build_match_targets(output_dir, competitions)
    print(f"Found {len(competitions)} competition-season rows.")
    print(f"Prepared {len(targets) // 2} unique matches.")

    downloaded = 0
    skipped = 0
    missing = 0
    with futures.ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        future_map = {
            executor.submit(
                download_file,
                url,
                destination,
                args.retries,
                args.timeout,
                destination.parent.name == "three-sixty",
            ): destination
            for url, destination in targets
        }
        for future in futures.as_completed(future_map):
            destination = future_map[future]
            try:
                result = future.result()
                if result == "downloaded":
                    downloaded += 1
                elif result == "skipped":
                    skipped += 1
                elif result == "missing":
                    missing += 1
            except Exception as exc:
                print(f"Failed: {destination} ({exc})", file=sys.stderr)

    print(
        f"Done. Downloaded {downloaded} files, skipped {skipped} existing files, "
        f"missing 360 files {missing}."
    )
    print(f"Data saved to: {output_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())