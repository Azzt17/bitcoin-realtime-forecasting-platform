#!/usr/bin/env python3

"""Render batch backfill plans and checkpoint progress for the 1h pipeline."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Iterable


def parse_timestamp(raw: str) -> datetime:
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def floor_to_month(value: datetime) -> datetime:
    value = value.astimezone(timezone.utc)
    return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def next_month(value: datetime) -> datetime:
    value = floor_to_month(value)
    year = value.year + (1 if value.month == 12 else 0)
    month = 1 if value.month == 12 else value.month + 1
    return value.replace(year=year, month=month)


def format_duration(minutes: float) -> str:
    total = max(0, int(round(minutes)))
    hours, remainder = divmod(total, 60)
    days, hours = divmod(hours, 24)
    if days:
        return f"{days}d {hours:02d}h"
    if hours:
        return f"{hours}h {remainder:02d}m"
    return f"{remainder}m"


def progress_bar(done: int, total: int, width: int = 24) -> str:
    if total <= 0:
        return f"[{'░' * width}] 0%"
    filled = max(0, min(width, round((done / total) * width)))
    percent = round((done / total) * 100)
    return f"[{'█' * filled}{'░' * (width - filled)}] {percent:3d}%"


def format_table(headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        widths = [len(header) for header in headers]
    else:
        widths = [
            max(len(header), max(len(str(row[index])) for row in rows))
            for index, header in enumerate(headers)
        ]

    def render_row(row: Iterable[str]) -> str:
        cells = [str(cell).ljust(widths[index]) for index, cell in enumerate(row)]
        return " | ".join(cells)

    separator = "-+-".join("-" * width for width in widths)
    lines = [render_row(headers), separator]
    lines.extend(render_row(row) for row in rows)
    return "\n".join(lines)


def load_clickhouse_rows() -> list[dict]:
    payload = json.load(sys.stdin)
    if isinstance(payload, dict) and "data" in payload and isinstance(payload["data"], list):
        return payload["data"]
    if isinstance(payload, list):
        return payload
    return []


def plan_batches(
    start_time: datetime,
    end_time: datetime,
    monthly_hours: dict[str, dict[str, int]],
    warmup_hours: int,
    throughput_hours_per_minute: float,
) -> list[dict[str, object]]:
    batches: list[dict[str, object]] = []
    total_source_hours = 0

    for month_key in sorted(monthly_hours):
        month_start = parse_timestamp(month_key)
        month_counts = monthly_hours.get(month_key, {})
        ohlcv_hours = int(month_counts.get("ohlcv_hours", 0))
        block_hours = int(month_counts.get("block_hours", 0))
        tx_hours = int(month_counts.get("tx_hours", 0))
        usable_hours = ohlcv_hours
        total_source_hours += usable_hours
        batch_end = next_month(month_start)
        batches.append(
            {
                "batch_id": month_start.strftime("%Y-%m"),
                "batch_start": month_start.strftime("%F %T"),
                "batch_end": batch_end.strftime("%F %T"),
                "warmup_start": (month_start - timedelta(hours=warmup_hours)).strftime("%F %T"),
                "ohlcv_hours": ohlcv_hours,
                "block_hours": block_hours,
                "tx_hours": tx_hours,
                "usable_hours": usable_hours,
                "estimated_minutes": (
                    (usable_hours / throughput_hours_per_minute)
                    if throughput_hours_per_minute > 0
                    else 0.0
                ),
            }
        )

    cumulative_hours = 0
    for batch in batches:
        cumulative_hours += int(batch["usable_hours"])
        batch["progress_pct"] = (
            (cumulative_hours / total_source_hours) * 100 if total_source_hours else 0
        )

    return batches


def render_plan(
    batches: list[dict[str, object]],
    throughput_hours_per_minute: float,
    start_time: datetime,
    end_time: datetime,
    compact: bool,
) -> str:
    total_source_hours = sum(int(batch["usable_hours"]) for batch in batches)
    total_estimated_minutes = (
        total_source_hours / throughput_hours_per_minute if throughput_hours_per_minute > 0 else 0.0
    )
    total_estimated_batches = len(batches)
    summary = [
        f"Backfill range      : {start_time.strftime('%F %T')} -> {end_time.strftime('%F %T')} (end exclusive)",
        f"Batch grain         : 1 month",
        f"Warmup window       : 96 hours",
        f"Batch count         : {total_estimated_batches}",
        f"Usable source hours  : {total_source_hours}",
        f"ETA assumption      : {throughput_hours_per_minute:.2f} source-hours/minute",
        f"Rough sequential ETA: {format_duration(total_estimated_minutes)}",
    ]

    rows = []
    visible_batches = batches
    omitted = 0
    if compact and len(batches) > 12:
        visible_batches = batches[:6] + batches[-4:]
        omitted = len(batches) - len(visible_batches)

    for batch in visible_batches:
        rows.append(
            [
                str(batch["batch_id"]),
                f'{batch["batch_start"]} -> {batch["batch_end"]}',
                str(batch["warmup_start"]),
                str(batch["ohlcv_hours"]),
                str(batch["block_hours"]),
                str(batch["tx_hours"]),
                str(batch["usable_hours"]),
                format_duration(float(batch["estimated_minutes"])),
                progress_bar(int(batch["progress_pct"]), 100, width=16),
            ]
        )

    table = format_table(
        [
            "batch",
            "window [UTC)",
            "warmup from",
            "ohlcv h",
            "block h",
            "tx h",
            "usable h",
            "est.",
            "cumulative",
        ],
        rows,
    )

    if omitted:
        table += f"\n... {omitted} batch rows omitted in compact view ..."

    plan_progress = progress_bar(0, total_source_hours)
    return "\n".join(summary + ["", plan_progress, "", table])


def render_report(rows: list[dict]) -> str:
    if not rows:
        return "No checkpoint rows found yet.\nPopulate btc.feature_backfill_batches to start tracking progress."

    status_counts = Counter(str(row.get("status", "unknown")) for row in rows)
    succeeded = status_counts.get("succeeded", 0) + status_counts.get("complete", 0)
    total = len(rows)
    updated_rows = sorted(rows, key=lambda row: str(row.get("updated_at", "")), reverse=True)

    lines = [
        f"Backfill checkpoint rows : {total}",
        f"Completed batches        : {succeeded}",
        f"Overall progress         : {progress_bar(succeeded, total)}",
        "",
        "Status counts:",
    ]
    for status in sorted(status_counts):
        lines.append(f"  - {status}: {status_counts[status]}")

    preview_rows = []
    for row in updated_rows[:12]:
        preview_rows.append(
            [
                str(row.get("batch_id", "")),
                str(row.get("status", "")),
                str(row.get("source_hours", "")),
                str(row.get("processed_hours", "")),
                str(row.get("attempt_count", "")),
                str(row.get("batch_owner", "")),
                str(row.get("updated_at", "")),
                str(row.get("last_error", ""))[:48],
            ]
        )

    lines.extend(
        [
            "",
            format_table(
                [
                    "batch",
                    "status",
                    "source h",
                    "done h",
                    "tries",
                    "owner",
                    "updated at",
                    "last error",
                ],
                preview_rows,
            ),
        ]
    )

    if len(rows) > len(preview_rows):
        lines.append(f"... {len(rows) - len(preview_rows)} older rows omitted ...")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser("plan")
    plan_parser.add_argument("--start-time", required=True)
    plan_parser.add_argument("--end-time", required=True)
    plan_parser.add_argument("--warmup-hours", type=int, default=96)
    plan_parser.add_argument("--throughput-hours-per-minute", type=float, default=1.0)
    plan_parser.add_argument("--compact", action="store_true", default=True)
    plan_parser.add_argument("--show-all", action="store_true", default=False)

    report_parser = subparsers.add_parser("report")
    report_parser.add_argument("--compact", action="store_true", default=True)

    args = parser.parse_args()
    rows = load_clickhouse_rows()

    if args.command == "plan":
        monthly_hours: dict[str, dict[str, int]] = {}
        for row in rows:
            key = str(row.get("month_start", ""))
            monthly_hours[key] = {
                "ohlcv_hours": int(row.get("ohlcv_hours", 0) or 0),
                "block_hours": int(row.get("block_hours", 0) or 0),
                "tx_hours": int(row.get("tx_hours", 0) or 0),
            }

        start_time = parse_timestamp(args.start_time)
        end_time = parse_timestamp(args.end_time)
        batches = plan_batches(
            start_time=start_time,
            end_time=end_time,
            monthly_hours=monthly_hours,
            warmup_hours=args.warmup_hours,
            throughput_hours_per_minute=args.throughput_hours_per_minute,
        )
        compact = not args.show_all
        print(
            render_plan(
                batches,
                throughput_hours_per_minute=args.throughput_hours_per_minute,
                start_time=start_time,
                end_time=end_time,
                compact=compact,
            )
        )
        return 0

    if args.command == "report":
        print(render_report(rows))
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
