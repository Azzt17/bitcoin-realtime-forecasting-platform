#!/usr/bin/env python3

"""Run the 1h feature job repeatedly over a rolling realtime window."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

DEFAULT_SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "/opt/spark/bin/spark-submit")
DEFAULT_JOB_PATH = os.environ.get(
    "REALTIME_FEATURE_JOB", "/opt/bitcoin-realtime-forecasting-platform/jobs/spark/build_features_1h.py"
)
DEFAULT_STATE_FILE = os.environ.get(
    "REALTIME_FEATURE_STATE_FILE", "/tmp/realtime_feature_loop_state.json"
)
DEFAULT_SPARK_JARS = os.environ.get(
    "SPARK_JARS",
    ",".join(
        [
            "/tmp/.ivy2/jars/com.clickhouse_jdbc-v2-0.9.8.jar",
            "/tmp/.ivy2/jars/com.clickhouse_client-v2-0.9.8.jar",
            "/tmp/.ivy2/jars/com.clickhouse_clickhouse-client-0.9.8.jar",
            "/tmp/.ivy2/jars/com.clickhouse_clickhouse-data-0.9.8.jar",
            "/tmp/.ivy2/jars/com.clickhouse_clickhouse-http-client-0.9.8.jar",
            "/tmp/.ivy2/jars/org.apache.httpcomponents.client5_httpclient5-5.4.4.jar",
            "/tmp/.ivy2/jars/org.apache.httpcomponents.core5_httpcore5-5.3.4.jar",
            "/tmp/.ivy2/jars/org.apache.httpcomponents.core5_httpcore5-h2-5.3.4.jar",
            "/tmp/.ivy2/jars/com.google.guava_guava-33.4.6-jre.jar",
            "/tmp/.ivy2/jars/com.google.guava_failureaccess-1.0.3.jar",
            "/tmp/.ivy2/jars/com.google.guava_listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar",
            "/tmp/.ivy2/jars/com.google.j2objc_j2objc-annotations-3.0.0.jar",
            "/tmp/.ivy2/jars/com.google.errorprone_error_prone_annotations-2.36.0.jar",
        ]
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the 1h feature job on a rolling realtime window."
    )
    parser.add_argument("--spark-submit", default=DEFAULT_SPARK_SUBMIT)
    parser.add_argument("--spark-master-url", required=True)
    parser.add_argument("--spark-driver-host", required=True)
    parser.add_argument("--job-path", default=DEFAULT_JOB_PATH)
    parser.add_argument("--clickhouse-host", required=True)
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-database", default="btc")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument(
        "--clickhouse-password",
        default=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    parser.add_argument("--target-table", default="features_1h_realtime")
    parser.add_argument("--window-hours", type=int, default=6)
    parser.add_argument("--warmup-hours", type=int, default=96)
    parser.add_argument("--interval-seconds", type=int, default=60)
    parser.add_argument("--spark-jars", default=DEFAULT_SPARK_JARS)
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def load_state(state_file: str) -> dict[str, Any]:
    path = Path(state_file)
    if not path.exists():
        return {"last_successful_end": None}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return {"last_successful_end": None}
        return {"last_successful_end": payload.get("last_successful_end")}
    except Exception as exc:
        print(f"[WARN] Could not load state file {state_file}: {exc}")
        return {"last_successful_end": None}


def save_state(state_file: str, last_successful_end: datetime) -> None:
    path = Path(state_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_successful_end": last_successful_end.astimezone(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    fd, tmp_path = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_command(args: argparse.Namespace, start_time: datetime, end_time: datetime) -> list[str]:
    command = [
        args.spark_submit,
        "--master",
        args.spark_master_url,
        "--deploy-mode",
        "client",
        "--jars",
        args.spark_jars,
        "--conf",
        "spark.driver.userClassPathFirst=true",
        "--conf",
        "spark.executor.userClassPathFirst=true",
        "--conf",
        "spark.ui.enabled=false",
        "--conf",
        f"spark.driver.host={args.spark_driver_host}",
        "--conf",
        "spark.driver.bindAddress=0.0.0.0",
        args.job_path,
        "--clickhouse-host",
        args.clickhouse_host,
        "--clickhouse-port",
        str(args.clickhouse_port),
        "--clickhouse-database",
        args.clickhouse_database,
        "--clickhouse-user",
        args.clickhouse_user,
        "--clickhouse-password",
        args.clickhouse_password,
        "--target-table",
        args.target_table,
        "--start-time",
        format_timestamp(start_time),
        "--end-time",
        format_timestamp(end_time),
        "--warmup-hours",
        str(args.warmup_hours),
    ]
    return command


def redact_command(command: list[str]) -> list[str]:
    redacted: list[str] = []
    skip_next = False
    for part in command:
        if skip_next:
            redacted.append("[redacted]")
            skip_next = False
            continue
        redacted.append(part)
        if part == "--clickhouse-password":
            skip_next = True
    return redacted


def run_once(args: argparse.Namespace, state: dict[str, Any]) -> datetime:
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=args.window_hours)
    command = build_command(args, start_time, end_time)

    last_successful_end = state.get("last_successful_end")
    if last_successful_end:
        print(f"[state] previous successful end: {last_successful_end}")

    print(
        f"[run] building {args.target_table} for window {format_timestamp(start_time)} -> {format_timestamp(end_time)}",
        flush=True,
    )
    print(f"[run] command: {' '.join(redact_command(command))}", flush=True)
    subprocess.run(command, check=True)
    save_state(args.state_file, end_time)
    print(
        f"[state] saved successful end {format_timestamp(end_time)} to {args.state_file}",
        flush=True,
    )
    return end_time


def main() -> int:
    args = parse_args()
    if args.interval_seconds < 5:
        raise SystemExit("interval-seconds must be at least 5")
    if args.window_hours < 1:
        raise SystemExit("window-hours must be positive")
    if args.warmup_hours < 1:
        raise SystemExit("warmup-hours must be positive")
    if not args.clickhouse_password:
        print("[WARN] ClickHouse password is empty; this is only safe if the cluster allows it.")

    state = load_state(args.state_file)
    print(
        f"[init] realtime feature loop started with window_hours={args.window_hours}, "
        f"warmup_hours={args.warmup_hours}, interval={args.interval_seconds}s",
        flush=True,
    )

    while True:
        started_at = datetime.now(timezone.utc)
        try:
            run_once(args, state)
        except subprocess.CalledProcessError as exc:
            print(f"[ERROR] feature build failed with exit code {exc.returncode}", flush=True)
        except Exception as exc:
            print(f"[ERROR] feature loop failed: {exc}", flush=True)

        state = load_state(args.state_file)

        if args.once:
            return 0

        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        sleep_for = max(0.0, float(args.interval_seconds) - elapsed)
        print(f"[sleep] waiting {sleep_for:.1f}s before next run", flush=True)
        time.sleep(sleep_for)


if __name__ == "__main__":
    raise SystemExit(main())
