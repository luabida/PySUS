"""Resumable reupload of Parquet files affected by the DBF encoding fix.

Regenerates the S3 artifacts for the DATASUS groups that were decoded with
the old latin-1 reader: SIA ``AD``/``AN``/``AR``/``BI`` and SIH
``RD``/``RJ``.

The run appends one journal line per completed file. If it is interrupted,
re-running the same command resumes from the journal: files already
uploaded are skipped, failed/interrupted files are retried.

Usage:
    python -m pysus.management.scripts.reupload_encoding --status
    python -m pysus.management.scripts.reupload_encoding
    python -m pysus.management.scripts.reupload_encoding --reset
    python -m pysus.management.scripts.reupload_encoding --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
from collections import Counter
from pathlib import Path

from pysus import CACHEPATH
from pysus.management.records import load_journal_keys, load_journal_origins
from pysus.management.scripts.sync_clients import load_env
from pysus.management.sync import (
    ENCODING_REUPLOAD_DATASETS,
    ENCODING_REUPLOAD_GROUPS,
    SyncEngine,
)

JOURNAL_NAME = "reupload-encoding.jsonl"


def default_journal() -> Path:
    """Return the stable journal path used by this job."""
    return Path(CACHEPATH) / "management" / "journal" / JOURNAL_NAME


def _raise_keyboard_interrupt(signum, frame):  # noqa: ANN001
    """Turn stop signals into KeyboardInterrupt for graceful shutdown."""
    raise KeyboardInterrupt


def install_stop_handlers() -> None:
    """Enable Ctrl-C/SIGTERM even when inherited as ignored (background)."""
    for name in ("SIGINT", "SIGTERM", "SIGHUP"):
        signum = getattr(signal, name, None)
        if signum is None:
            continue
        try:
            signal.signal(signum, _raise_keyboard_interrupt)
        except (ValueError, OSError):
            continue


def journal_summary(path: Path) -> dict[str, int]:
    """Return the latest outcome per file, counted by status."""
    latest: dict[tuple, str] = {}
    if not path.exists():
        return {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        key = (
            data.get("dataset"),
            data.get("group"),
            data.get("year"),
            data.get("month"),
            data.get("state"),
            data.get("stem"),
        )
        latest[key] = str(data.get("status", "unknown"))
    counts = Counter(latest.values())
    return dict(counts)


async def run(
    journal: Path | None = None,
    datasets: list[str] | None = None,
    reset: bool = False,
    dry_run: bool = False,
    checkpoint_every: int = 500,
    workers: int = 16,
    ftp_connections: int = 6,
) -> dict[str, int]:
    """Run the resumable encoding-fix reupload and return the summary."""
    selected_datasets: list[str] = []
    for name in datasets or ENCODING_REUPLOAD_DATASETS:
        canonical = name.strip().upper()
        if canonical and canonical not in selected_datasets:
            selected_datasets.append(canonical)
    journal_path = Path(journal) if journal else default_journal()
    if reset and journal_path.exists():
        journal_path.unlink()

    resume_keys = load_journal_keys(journal_path)
    resume_origins = load_journal_origins(journal_path)
    print(f"journal: {journal_path}", flush=True)
    if resume_keys:
        print(
            f"resuming: {len(resume_keys)} file(s) already uploaded",
            flush=True,
        )
    if dry_run:
        print("dry run: no downloads, uploads or journal writes", flush=True)

    env = load_env()
    engine = SyncEngine(
        access_key=env.get("ACCESS_KEY"),
        secret_key=env.get("SECRET_KEY"),
        dadosgov_token=env.get("DADOSGOV_TOKEN"),
    )

    counts: dict[str, int] = {}

    def on_outcome(outcome) -> None:
        counts[outcome.status] = counts.get(outcome.status, 0) + 1
        if outcome.status in ("uploaded", "failed", "needs_token"):
            print(f"[{outcome.status}] {outcome.detail}", flush=True)
        total = sum(counts.values())
        if total % 500 == 0:
            print(f"progress: {counts}", flush=True)

    try:
        async with engine:
            report = await engine.run(
                datasets=selected_datasets,
                groups=ENCODING_REUPLOAD_GROUPS,
                force=True,
                dry_run=dry_run,
                checkpoint_every=checkpoint_every,
                on_outcome=on_outcome,
                workers=workers,
                ftp_connections=ftp_connections,
                resume=resume_keys or None,
                resume_origins=resume_origins or None,
                journal=None if dry_run else journal_path,
            )
    except BaseException:  # noqa: B902
        # Preserve catalog progress before a Ctrl-C/GOODBYE so the next run
        # can resume without leaving uploaded files out of the catalog.
        if not dry_run and getattr(engine, "_changed_catalog", False):
            try:
                await engine._checkpoint()
            except Exception as exc:  # noqa: BLE001
                print(
                    f"warning: final checkpoint failed: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
        raise

    return report.summary()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--journal",
        type=Path,
        default=None,
        help="Resume journal path (default: cache management journal)",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=None,
        help="Restrict to dataset(s), repeatable or comma-separated "
        "(e.g. --dataset SIH). Default: SIA and SIH",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete the journal and start from scratch",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print journal progress and exit",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be reuploaded without changing anything",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=500,
        help="Upload catalogs to S3 every N successful uploads",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Concurrent ingestion workers",
    )
    parser.add_argument(
        "--ftp-connections",
        type=int,
        default=6,
        help="FTP connection pool size",
    )
    args = parser.parse_args()

    journal_path = args.journal or default_journal()
    if args.status:
        print(
            json.dumps(
                {
                    "journal": str(journal_path),
                    "exists": journal_path.exists(),
                    "statuses": journal_summary(journal_path),
                }
            )
        )
        return 0

    datasets: list[str] | None = None
    if args.dataset:
        datasets = []
        for value in args.dataset:
            for part in value.split(","):
                name = part.strip().upper()
                if name and name not in datasets:
                    datasets.append(name)

    install_stop_handlers()
    try:
        summary = asyncio.run(
            run(
                journal=journal_path,
                datasets=datasets,
                reset=args.reset,
                dry_run=args.dry_run,
                checkpoint_every=args.checkpoint_every,
                workers=args.workers,
                ftp_connections=args.ftp_connections,
            )
        )
    except KeyboardInterrupt:
        print(
            f"\nstopped; rerun to resume from {journal_path}",
            file=sys.stderr,
            flush=True,
        )
        return 130

    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
