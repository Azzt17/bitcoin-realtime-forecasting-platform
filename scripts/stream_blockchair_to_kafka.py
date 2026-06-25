#!/usr/bin/env python3

"""Poll Blockchair and stream on-chain events into Kafka safely."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_API_BASE = os.environ.get("BLOCKCHAIR_API_BASE", "https://api.blockchair.com/bitcoin")
DEFAULT_TOPIC = os.environ.get("KAFKA_TOPIC", "btc.onchain.raw")
DEFAULT_STATE_FILE = os.environ.get(
    "BLOCKCHAIR_STATE_FILE", "/tmp/blockchair_to_kafka_state.json"
)
DEFAULT_POLL_INTERVAL_SECONDS = int(os.environ.get("BLOCKCHAIR_POLL_INTERVAL_SECONDS", "300"))
DEFAULT_BLOCK_LIMIT = int(os.environ.get("BLOCKCHAIR_BLOCK_LIMIT", "10"))
DEFAULT_TX_LIMIT = int(os.environ.get("BLOCKCHAIR_TX_LIMIT", "10"))
DEFAULT_ENDPOINT_GAP_SECONDS = int(os.environ.get("BLOCKCHAIR_ENDPOINT_GAP_SECONDS", "15"))
DEFAULT_BACKOFF_SECONDS = int(os.environ.get("BLOCKCHAIR_BACKOFF_SECONDS", "900"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poll Blockchair and publish on-chain events to Kafka."
    )
    parser.add_argument(
        "--api-base",
        default=DEFAULT_API_BASE,
        help="Blockchair API base URL.",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("BLOCKCHAIR_API_KEY", ""),
        help="Blockchair API key. Prefer the BLOCKCHAIR_API_KEY environment variable.",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", ""),
        help="Kafka bootstrap servers, for example kafka-1:9092,kafka-2:9092.",
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--poll-interval-seconds", type=int, default=DEFAULT_POLL_INTERVAL_SECONDS)
    parser.add_argument("--block-limit", type=int, default=DEFAULT_BLOCK_LIMIT)
    parser.add_argument("--tx-limit", type=int, default=DEFAULT_TX_LIMIT)
    parser.add_argument("--endpoint-gap-seconds", type=int, default=DEFAULT_ENDPOINT_GAP_SECONDS)
    parser.add_argument("--backoff-seconds", type=int, default=DEFAULT_BACKOFF_SECONDS)
    parser.add_argument(
        "--request-timeout-seconds",
        type=int,
        default=int(os.environ.get("BLOCKCHAIR_REQUEST_TIMEOUT_SECONDS", "30")),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch Blockchair data and print progress without sending anything to Kafka.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single blocks + transactions cycle and exit.",
    )
    return parser.parse_args()


def load_state(state_file: str) -> dict[str, Any]:
    path = Path(state_file)
    if not path.exists():
        return {"last_block_id": 0, "last_tx_id": 0, "next_allowed_at": 0}

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return {"last_block_id": 0, "last_tx_id": 0, "next_allowed_at": 0}
        return {
            "last_block_id": int(payload.get("last_block_id", 0) or 0),
            "last_tx_id": int(payload.get("last_tx_id", 0) or 0),
            "next_allowed_at": int(payload.get("next_allowed_at", 0) or 0),
        }
    except Exception as exc:
        print(f"[WARN] Could not load state file {state_file}: {exc}")
        return {"last_block_id": 0, "last_tx_id": 0, "next_allowed_at": 0}


def save_state(state_file: str, last_block_id: int, last_tx_id: int, next_allowed_at: int = 0) -> None:
    path = Path(state_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_block_id": int(last_block_id),
        "last_tx_id": int(last_tx_id),
        "next_allowed_at": int(next_allowed_at),
        "updated_at": int(time.time()),
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


def normalize_records(raw_data: Any) -> list[dict[str, Any]]:
    if isinstance(raw_data, list):
        records = raw_data
    elif isinstance(raw_data, dict):
        records = list(raw_data.values())
    else:
        return []

    normalized = [record for record in records if isinstance(record, dict)]
    normalized.sort(key=lambda item: int(item.get("id", -1) or -1))
    return normalized


def get_blockchair_data(
    api_base: str,
    api_key: str,
    endpoint: str,
    params: dict[str, Any] | None = None,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    params = dict(params or {})
    if api_key:
        params["key"] = api_key
    query_string = urllib.parse.urlencode(params)
    url = f"{api_base.rstrip('/')}/{endpoint}"
    if query_string:
        url = f"{url}?{query_string}"

    headers = {"User-Agent": "bitcoin-realtime-forecasting-platform/1.0"}

    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status} from Blockchair endpoint {endpoint}")
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code == 430:
            raise RuntimeError("Blockchair returned HTTP 430 (temporary blacklist)") from exc
        raise RuntimeError(f"HTTP {exc.code} from Blockchair endpoint {endpoint}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to fetch Blockchair endpoint {endpoint}: {exc}") from exc


def connect_kafka(kafka_bootstrap: str):
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "kafka-python is not installed on this host. Run in --dry-run mode or install kafka-python."
        ) from exc

    if not kafka_bootstrap:
        raise SystemExit(
            "Kafka bootstrap servers are required. Set --kafka-bootstrap or KAFKA_BOOTSTRAP_SERVERS."
        )

    return KafkaProducer(
        bootstrap_servers=kafka_bootstrap.split(","),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        retries=5,
        linger_ms=100,
    )


def publish_event(producer, topic: str, event: dict[str, Any]) -> None:
    if producer is None:
        print(f"[DRY-RUN] {event['event_type']} id={event['data_id']} time={event['event_time']}")
        return
    producer.send(topic, event)


def ingest_endpoint(
    producer: Any | None,
    topic: str,
    api_base: str,
    api_key: str,
    endpoint: str,
    params: dict[str, Any],
    last_seen_id: int,
    timeout_seconds: int,
) -> int:
    payload = get_blockchair_data(api_base, api_key, endpoint, params=params, timeout_seconds=timeout_seconds)
    records = normalize_records(payload.get("data"))

    new_records = [record for record in records if int(record.get("id", 0) or 0) > last_seen_id]
    if not new_records:
        print(f"[{endpoint}] No new rows above id={last_seen_id}")
        return last_seen_id

    for record in new_records:
        event = {
            "event_type": "block" if endpoint == "blocks" else "transaction",
            "ingested_at": int(time.time()),
            "data": json.dumps(record, separators=(",", ":")),
            "data_id": int(record.get("id", 0) or 0),
        }
        publish_event(producer, topic, event)
        print(
            f"[{endpoint}] queued id={event['data_id']} hash={record.get('hash', '')}",
            flush=True,
        )
        last_seen_id = max(last_seen_id, event["data_id"])

    return last_seen_id


def main() -> int:
    args = parse_args()
    if args.poll_interval_seconds < 5:
        raise SystemExit("poll-interval-seconds must be at least 5")
    if args.block_limit < 1 or args.tx_limit < 1:
        raise SystemExit("block-limit and tx-limit must be positive")
    if not args.api_key and not args.dry_run:
        raise SystemExit(
            "Blockchair API key is required. Set BLOCKCHAIR_API_KEY or pass --api-key."
        )

    state = load_state(args.state_file)
    last_block_id = int(state["last_block_id"])
    last_tx_id = int(state["last_tx_id"])
    next_allowed_at = int(state.get("next_allowed_at", 0))

    producer: Any | None = None
    if not args.dry_run:
        producer = connect_kafka(args.kafka_bootstrap)
        print("[init] Kafka producer connected")
    else:
        print("[init] Dry-run mode enabled; Kafka publish is disabled")

    print(
        f"[init] Blockchair poller started with block_limit={args.block_limit}, "
        f"tx_limit={args.tx_limit}, poll_interval={args.poll_interval_seconds}s, "
        f"endpoint_gap={args.endpoint_gap_seconds}s",
        flush=True,
    )
    print(f"[init] Resuming from last_block_id={last_block_id}, last_tx_id={last_tx_id}")

    while True:
        now = int(time.time())
        if next_allowed_at and now < next_allowed_at:
            sleep_for = max(1, next_allowed_at - now)
            print(f"[cooldown] sleeping {sleep_for}s until Blockchair retry window", flush=True)
            time.sleep(sleep_for)
            continue

        try:
            last_block_id = ingest_endpoint(
                producer=producer,
                topic=args.topic,
                api_base=args.api_base,
                api_key=args.api_key,
                endpoint="blocks",
                params={"limit": args.block_limit},
                last_seen_id=last_block_id,
                timeout_seconds=args.request_timeout_seconds,
            )

            time.sleep(args.endpoint_gap_seconds)

            last_tx_id = ingest_endpoint(
                producer=producer,
                topic=args.topic,
                api_base=args.api_base,
                api_key=args.api_key,
                endpoint="transactions",
                params={"limit": args.tx_limit},
                last_seen_id=last_tx_id,
                timeout_seconds=args.request_timeout_seconds,
            )

            if producer is not None:
                producer.flush()

            next_allowed_at = 0
            save_state(args.state_file, last_block_id, last_tx_id, next_allowed_at=0)
            print(
                f"[state] saved last_block_id={last_block_id} last_tx_id={last_tx_id} "
                f"to {args.state_file}",
                flush=True,
            )
        except RuntimeError as exc:
            message = str(exc)
            if "HTTP 430" in message or "temporary blacklist" in message:
                next_allowed_at = int(time.time()) + args.backoff_seconds
                save_state(
                    args.state_file,
                    last_block_id,
                    last_tx_id,
                    next_allowed_at=next_allowed_at,
                )
                print(
                    f"[cooldown] Blockchair returned 430; next retry after {args.backoff_seconds}s",
                    flush=True,
                )
                time.sleep(args.backoff_seconds)
                continue
            raise
        except Exception as exc:
            print(f"[ERROR] Blockchair loop failed: {exc}", flush=True)
            print(f"[backoff] sleeping {args.backoff_seconds}s before retry", flush=True)
            time.sleep(args.backoff_seconds)
            continue

        if args.once:
            break

        time.sleep(args.poll_interval_seconds)

    if producer is not None:
        producer.flush()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
