# Realtime Pipeline Runbook

Dokumen ini menjelaskan kondisi runtime realtime yang benar-benar ada di repo saat ini. Isi dokumen ini sengaja dibuat operasional, bukan aspirational.

## Scope

Pipeline realtime MVP saat ini terdiri dari empat bagian:

1. Blockchair poller → Kafka
2. Kafka on-chain stream → ClickHouse
3. Realtime feature loop → ClickHouse feature table
4. Realtime prediction loop → ClickHouse predictions

## 1. Blockchair Poller

File:

- `scripts/stream_blockchair_to_kafka.py`

Fungsi:

- Poll Blockchair secara konservatif.
- Publish event ke Kafka topic `btc.onchain.raw`.
- Simpan state `last_block_id` dan `last_tx_id` ke file state agar resume tidak mulai dari nol.

Konfigurasi runtime:

- `BLOCKCHAIR_API_KEY` atau `--api-key`
- `KAFKA_BOOTSTRAP_SERVERS` atau `--kafka-bootstrap`
- `BLOCKCHAIR_STATE_FILE` atau `--state-file`

Catatan operasional:

- Jangan hard-code API key di source.
- Gunakan interval polling yang konservatif.
- Jika Kafka tidak tersedia, proses harus fail fast atau dijalankan dalam `--dry-run` untuk validasi API saja.
- Jika Blockchair membalas 430, poller akan menyimpan `next_allowed_at` di state file dan menahan retry sampai cooldown lewat.

## 2. Kafka on-chain stream → ClickHouse

File:

- `spark_streaming/onchain_to_clickhouse.py`
- `clickhouse/schema/realtime_onchain_events.sql`

Fungsi:

- Consume event JSON dari `btc.onchain.raw`.
- Parse payload string JSON yang dibawa oleh producer.
- Insert ke `btc.realtime_onchain_events` lewat HTTP insert `JSONEachRow`.

Perilaku penting:

- `event_time` dipilih dari payload `data.time` jika tersedia.
- Jika timestamp payload kosong atau tidak valid, job memakai fallback ke `ingested_at`.
- `has_witness` dinormalisasi menjadi `0` atau `1`.

## 3. Realtime feature loop

File:

- `scripts/realtime_feature_loop.py`
- `jobs/spark/build_features_1h.py`
- `clickhouse/schema/features_1h_realtime.sql`

Fungsi:

- Menjalankan `spark-submit` secara berulang.
- Mengambil window rolling terbaru dari ClickHouse.
- Menulis hasil ke `btc.features_1h_realtime`.

Konfigurasi runtime:

- `--spark-submit`
- `--spark-master-url`
- `--spark-driver-host`
- `--job-path`
- `--clickhouse-host`
- `--clickhouse-user`
- `--clickhouse-password`
- `--window-hours`
- `--warmup-hours`
- `--interval-seconds`

Catatan:

- Skrip ini sekarang benar-benar mengeksekusi Spark job.
- State file hanya untuk observability/resume, bukan sebagai source of truth feature output.

## 4. Realtime prediction loop

File:

- `spark_streaming/predict_realtime_1h.py`
- `clickhouse/schema/predictions.sql`

Fungsi:

- Poll row fitur terbaru dari `btc.features_1h_realtime`.
- Load model Spark GBT dari artifact path.
- Tulis hasil prediksi ke `btc.predictions`.

## Cara menjalankan

Contoh urutan aman:

1. Siapkan schema ClickHouse:

```bash
cat clickhouse/schema/realtime_onchain_events.sql | docker exec -i clickhouse clickhouse-client --multiquery
cat clickhouse/schema/features_1h_realtime.sql | docker exec -i clickhouse clickhouse-client --multiquery
```

2. Jalankan producer Blockchair:

```bash
export BLOCKCHAIR_API_KEY="..."
export KAFKA_BOOTSTRAP_SERVERS="kafka-1:9092,kafka-2:9092,kafka-3:9092"
python3 scripts/stream_blockchair_to_kafka.py
```

3. Jalankan on-chain ClickHouse streamer:

```bash
/opt/spark/bin/spark-submit \
  --master spark://SPARK_MASTER_IP:7077 \
  spark_streaming/onchain_to_clickhouse.py \
  --kafka-bootstrap kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --clickhouse-url http://ANALYTICS_NODE_IP:8123/ \
  --checkpoint /tmp/spark-checkpoint-onchain
```

4. Jalankan realtime feature loop:

```bash
python3 scripts/realtime_feature_loop.py \
  --spark-master-url spark://SPARK_MASTER_IP:7077 \
  --spark-driver-host SPARK_MASTER_IP \
  --clickhouse-host ANALYTICS_NODE_IP
```

5. Jalankan inference loop:

```bash
/opt/spark/bin/spark-submit \
  spark_streaming/predict_realtime_1h.py \
  --clickhouse-host ANALYTICS_NODE_IP \
  --model-path /tmp/bitcoin-models/spark_gbt/v1/pipeline_model
```

## Current limitations

- Blockchair polling tetap dibatasi oleh rate limit eksternal.
- Rute realtime ini masih MVP, bukan high-throughput production streaming.
- Jika state file hilang, poller bisa mengulang sebagian window dan menghasilkan duplikasi sementara di Kafka/ClickHouse.
- Rute on-chain saat ini masih berbasis raw block/tx ingestion, sehingga beban API harus dijaga konservatif.
- Jika Blockchair membalas 430, jangan restart berulang-ulang dari host yang sama; biarkan cooldown state selesai atau pindah ke IP lain.

## Security requirements

- Jangan commit API key, SSH key, `.env`, state file lokal, atau artefak runtime lain.
- Gunakan env var atau secret store untuk kredensial.
- Jangan staging `.ai/` atau cache lokal.
