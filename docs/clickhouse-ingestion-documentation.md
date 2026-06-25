# 🟡 ClickHouse Ingestion Documentation
### Bitcoin Realtime Forecasting Platform

> **Platform:** DigitalOcean Droplet (`analytics-node`) · **Database:** ClickHouse · **Deployment:** Docker Compose  
> 📅 Last Updated: 2026-06-20 · 👤 Data Ingestion Team · ✅ All Data Successfully Loaded

---

## 📋 Table of Contents

| # | Section |
|---|---------|
| 1 | [🚀 Proses Ingestion](#-proses-ingestion) |
| 2 | [🗄️ Struktur Database & Tabel](#️-struktur-database--tabel) |
| 3 | [🛠️ Command Reference](#️-command-reference) |
| 4 | [📊 Data Validation](#-data-validation) |
| 5 | [⚠️ Troubleshooting](#️-troubleshooting) |
| 6 | [📁 File Locations](#-file-locations) |

---

## 🚀 Proses Ingestion

### Alur Data (Flow Diagram)

```
┌────────────────────┐
│  Data Source       │  ← Blockchair / CSV
│  (External)        │
└────────┬───────────┘
         │  SCP Upload
         ▼
┌────────────────────┐
│  /data/imports/    │  ← Droplet: analytics-node
│  incoming/         │
└────────┬───────────┘
         │  gunzip
         ▼
┌────────────────────┐
│  Extract           │  .tsv.gz → .tsv
│  .tsv.gz → .tsv    │
└────────┬───────────┘
         │  chown ingestion:ingestion
         ▼
┌────────────────────┐
│  Fix Ownership     │
│  + Skip Header     │  tail -n +2
└────────┬───────────┘
         │  docker exec
         ▼
┌────────────────────┐
│  ClickHouse        │  INSERT INTO btc.table FORMAT TSV
│  (Docker)          │
└────────┬───────────┘
         │  cleanup
         ▼
┌────────────────────┐
│  Delete .tsv       │  (opsional: delete atau pindah .gz)
│  (+ opsional .gz)  │
└────────────────────┘
```

---

### 📋 Tahapan Ingestion

| Step | Aksi | Contoh Command |
|:----:|------|----------------|
| **1** | Upload file dari PC ke server | `scp file.tsv.gz ingestion@143.198.220.69:~/incoming/` |
| **2** | Ekstrak file `.gz` | `gunzip -f file.tsv.gz` |
| **3** | Fix kepemilikan file | `chown ingestion:ingestion file.tsv` |
| **4** | Skip header & import ke ClickHouse | `tail -n +2 file.tsv \| docker exec -i clickhouse clickhouse-client --query "INSERT INTO btc.table FORMAT TSV"` |
| **5** | Hapus file `.tsv` setelah import | `rm -f file.tsv` |
| **6** | *(Opsional)* Hapus file `.gz` | `rm -f file.tsv.gz` |

---

### 📜 Script Otomatis

#### `scripts/auto_ingest_transactions.sh`
```bash
#!/bin/bash
# ============================================================
# Auto Ingest: Bitcoin Transactions
# Target Table : btc.raw_transactions
# Input Files  : blockchair_bitcoin_transactions_*.tsv.gz
# ============================================================
# Proses:
#   1. Scan semua file .tsv.gz di folder incoming/
#   2. Extract .gz → .tsv
#   3. Skip header (baris pertama)
#   4. INSERT ke ClickHouse via docker exec
#   5. Hapus .tsv setelah berhasil import
```

#### `scripts/auto_ingest_blocks.sh`
```bash
#!/bin/bash
# ============================================================
# Auto Ingest: Bitcoin Blocks
# Target Table : btc.raw_blocks
# Input Files  : blockchair_bitcoin_blocks_*.tsv.gz
# ============================================================
# Proses:
#   1. Scan semua file .tsv.gz di folder incoming/
#   2. Extract .gz → .tsv
#   3. Skip header (baris pertama)
#   4. INSERT ke ClickHouse via docker exec
#   5. Hapus .tsv setelah berhasil import
```

#### Manual Import (1 File Spesifik)
```bash
gunzip -c file.tsv.gz \
  | tail -n +2 \
  | sudo docker exec -i clickhouse clickhouse-client \
      --query "INSERT INTO btc.table_name FORMAT TSV"
```

---

## 🗄️ Struktur Database & Tabel

```sql
CREATE DATABASE IF NOT EXISTS btc;
```

Database `btc` memiliki **3 tabel utama**:

| Tabel | Deskripsi | Total Rows | Size |
|-------|-----------|:----------:|:----:|
| `raw_ohlcv` | Harga BTC per menit (OHLCV) | 7,607,549 | 174.93 MiB |
| `raw_blocks` | Data block Bitcoin | 265,912 | ~102 MiB |
| `raw_transactions` | Data transaksi Bitcoin | 728,801,169 | 75.16 GiB |

---

### 📈 Tabel 1 — `btc.raw_ohlcv`

> **Deskripsi:** Data harga Bitcoin per menit (Open, High, Low, Close, Volume) dari tahun 2012 hingga 2026.

```
Total Rows  : 7,607,549
Data Size   : 174.93 MiB
Date Range  : 2012-01-01  →  2026-06-19
```

#### Schema
```sql
CREATE TABLE btc.raw_ohlcv (
    timestamp   DateTime,   -- Waktu pencatatan harga
    open        Float64,    -- Harga pembukaan
    high        Float64,    -- Harga tertinggi dalam interval
    low         Float64,    -- Harga terendah dalam interval
    close       Float64,    -- Harga penutupan
    volume      Float64     -- Volume transaksi (dalam BTC)
) ENGINE = MergeTree()
ORDER BY timestamp;
```

#### Sample Data
```sql
SELECT * FROM btc.raw_ohlcv LIMIT 3;
```

| timestamp | open | high | low | close | volume |
|-----------|-----:|-----:|----:|------:|-------:|
| 2026-06-19 00:29:00 | 62,862.84 | 62,883.15 | 62,851.38 | 62,883.15 | 1.998 |
| 2026-06-19 00:28:00 | 62,866.49 | 62,878.50 | 62,866.49 | 62,875.79 | 1.009 |
| 2026-06-19 00:27:00 | 62,877.85 | 62,877.86 | 62,850.51 | 62,859.25 | 0.249 |

#### Key Queries
```sql
-- Statistik harga keseluruhan
SELECT
    MIN(close)  AS min_price,
    MAX(close)  AS max_price,
    AVG(close)  AS avg_price
FROM btc.raw_ohlcv;

-- Ringkasan tahunan
SELECT
    toYear(timestamp) AS year,
    COUNT(*)          AS total_rows,
    MIN(close)        AS min_price,
    MAX(close)        AS max_price,
    AVG(close)        AS avg_price
FROM btc.raw_ohlcv
GROUP BY year
ORDER BY year;
```

---

### 🧱 Tabel 2 — `btc.raw_blocks`

> **Deskripsi:** Data setiap block Bitcoin yang berhasil ditambang, dari tahun 2021 hingga 2026.

```
Total Rows  : 265,912
Data Size   : ~102 MiB
Date Range  : 2021-06-11 00:01:28  →  2026-06-11 23:50:09
```

#### Schema
```sql
CREATE TABLE btc.raw_blocks (
    id                  UInt64,     -- Block height / nomor urut block
    hash                String,     -- Hash unik block
    time                DateTime,   -- Waktu block ditemukan
    median_time         DateTime,   -- Median time (BIP113)
    size                UInt64,     -- Ukuran block (bytes)
    stripped_size       UInt64,     -- Ukuran tanpa witness data
    weight              UInt64,     -- Block weight (BIP141)
    version             UInt32,     -- Versi block
    version_hex         String,
    version_bits        String,
    merkle_root         String,     -- Merkle root dari semua transaksi
    nonce               UInt32,     -- Nonce yang digunakan miner
    bits                String,     -- Target difficulty (compact)
    difficulty          Float64,    -- Difficulty saat block ditemukan
    chainwork           String,
    coinbase_data_hex   String,     -- Data coinbase transaksi
    transaction_count   UInt64,     -- Jumlah transaksi dalam block
    witness_count       UInt64,
    input_count         UInt64,
    output_count        UInt64,
    input_total         Float64,    -- Total BTC input (satoshi)
    input_total_usd     Float64,
    output_total        Float64,    -- Total BTC output (satoshi)
    output_total_usd    Float64,
    fee_total           Float64,    -- Total fee (satoshi)
    fee_total_usd       Float64,
    fee_per_kb          Float64,
    fee_per_kb_usd      Float64,
    fee_per_kwu         Float64,
    fee_per_kwu_usd     Float64,
    cdd_total           Float64,    -- Coin Days Destroyed
    generation          Float64,    -- Block reward (satoshi)
    generation_usd      Float64,
    reward              Float64,    -- Total reward (fee + generation)
    reward_usd          Float64,
    guessed_miner       String      -- Nama pool/miner yang ditebak
) ENGINE = MergeTree()
ORDER BY time;
```

#### Sample Data
```sql
SELECT id, time, transaction_count, guessed_miner
FROM btc.raw_blocks LIMIT 3;
```

| id | time | transaction_count | guessed_miner |
|---:|------|------------------:|---------------|
| 687,112 | 2021-06-11 00:01:28 | 2,058 | SlushPool |
| 687,113 | 2021-06-11 00:12:52 | 1,752 | F2Pool |
| 825,831 | 2024-01-14 23:48:06 | 4,360 | SBICrypto |

#### Key Queries
```sql
-- Top 10 miner berdasarkan jumlah block
SELECT
    guessed_miner,
    COUNT(*) AS block_count
FROM btc.raw_blocks
GROUP BY guessed_miner
ORDER BY block_count DESC
LIMIT 10;

-- Statistik harian block
SELECT
    toDate(time)            AS day,
    COUNT(*)                AS total_blocks,
    AVG(transaction_count)  AS avg_tx_per_block
FROM btc.raw_blocks
GROUP BY day
ORDER BY day DESC
LIMIT 10;
```

---

### 💸 Tabel 3 — `btc.raw_transactions`

> **Deskripsi:** Data setiap transaksi Bitcoin dari tahun 2021 hingga 2026. Ini adalah tabel terbesar dalam sistem.

```
Total Rows  : 728,801,169  (728 juta lebih!)
Data Size   : 75.16 GiB
Date Range  : 2021-05-31  →  2026-06-11
```

#### Schema
```sql
CREATE TABLE btc.raw_transactions (
    block_id            UInt64,     -- Block yang mengandung transaksi ini
    tx_hash             String,     -- Hash unik transaksi
    tx_time             DateTime,   -- Waktu transaksi dikonfirmasi
    size                UInt64,     -- Ukuran transaksi (bytes)
    weight              UInt64,     -- Weight transaksi (BIP141)
    version             UInt32,     -- Versi transaksi
    lock_time           UInt64,     -- Locktime transaksi
    is_coinbase         UInt8,      -- 1 jika transaksi coinbase (reward miner)
    has_witness         UInt8,      -- 1 jika memiliki SegWit witness data
    input_count         UInt64,     -- Jumlah input UTXO
    output_count        UInt64,     -- Jumlah output UTXO
    input_total         Float64,    -- Total nilai input (satoshi)
    input_total_usd     Float64,
    output_total        Float64,    -- Total nilai output (satoshi)
    output_total_usd    Float64,
    fee                 Float64,    -- Fee transaksi (satoshi)
    fee_usd             Float64,
    fee_per_kb          Float64,
    fee_per_kb_usd      Float64,
    fee_per_kwu         Float64,
    fee_per_kwu_usd     Float64,
    cdd_total           Float64     -- Coin Days Destroyed
) ENGINE = MergeTree()
ORDER BY tx_time;
```

#### Sample Data
```sql
SELECT block_id, tx_time, fee, input_count, output_count
FROM btc.raw_transactions LIMIT 3;
```

| block_id | tx_time | fee | input_count | output_count |
|---------:|---------|----:|:-----------:|:------------:|
| 685,719 | 2021-05-31 23:08:07 | 0 | 1 | 4 |
| 685,719 | 2021-05-31 23:08:07 | 100,000 | 1 | 2 |
| 685,719 | 2021-05-31 23:08:07 | 71,000 | 1 | 2 |

#### Key Queries
```sql
-- Analisis fee transaksi
SELECT
    COUNT(*)          AS total_transactions,
    AVG(fee)          AS avg_fee_satoshi,
    MAX(fee)          AS max_fee_satoshi,
    AVG(input_count)  AS avg_inputs,
    AVG(output_count) AS avg_outputs
FROM btc.raw_transactions;

-- Block dengan transaksi terbanyak
SELECT
    block_id,
    COUNT(*) AS tx_count
FROM btc.raw_transactions
GROUP BY block_id
ORDER BY tx_count DESC
LIMIT 10;
```

---

## 🛠️ Command Reference

### Docker & ClickHouse

```bash
# Cek status container ClickHouse
sudo docker ps | grep clickhouse

# Masuk ke ClickHouse client (interactive)
sudo docker exec -it clickhouse clickhouse-client

# List semua tabel di database btc
sudo docker exec clickhouse clickhouse-client \
    --query "SHOW TABLES FROM btc"

# Describe struktur tabel
sudo docker exec clickhouse clickhouse-client --query "DESCRIBE btc.raw_ohlcv"
sudo docker exec clickhouse clickhouse-client --query "DESCRIBE btc.raw_blocks"
sudo docker exec clickhouse clickhouse-client --query "DESCRIBE btc.raw_transactions"

# Cek log container
sudo docker logs clickhouse --tail 50

# Restart ClickHouse
sudo docker restart clickhouse
```

### Permission & User

```bash
# Tambahkan user ke group docker (jika ada permission denied)
sudo usermod -aG docker ingestion
# → Wajib logout & login ulang setelah command ini
```

---

## 📊 Data Validation

### Query Validasi Lengkap

```sql
-- ① Total rows per tabel
SELECT 'raw_ohlcv'        AS tabel, COUNT(*) AS total_rows FROM btc.raw_ohlcv
UNION ALL
SELECT 'raw_blocks',               COUNT(*)               FROM btc.raw_blocks
UNION ALL
SELECT 'raw_transactions',         COUNT(*)               FROM btc.raw_transactions;

-- ② Ukuran data per tabel (dari system table)
SELECT
    table,
    formatReadableSize(sum(bytes)) AS size,
    sum(rows)                      AS rows,
    count()                        AS parts
FROM system.parts
WHERE database = 'btc' AND active = 1
GROUP BY table;

-- ③ Rentang tanggal per tabel
SELECT 'raw_ohlcv'        AS tabel, MIN(timestamp), MAX(timestamp) FROM btc.raw_ohlcv
UNION ALL
SELECT 'raw_blocks',               MIN(time),       MAX(time)      FROM btc.raw_blocks
UNION ALL
SELECT 'raw_transactions',         MIN(tx_time),    MAX(tx_time)   FROM btc.raw_transactions;
```

---

### ✅ Ringkasan Hasil Ingestion

| Tabel | Total Rows | Data Size | Date Range | Status |
|-------|:----------:|:---------:|-----------|:------:|
| `raw_ohlcv` | 7,607,549 | 174.93 MiB | 2012-01-01 → 2026-06-19 | ✅ OK |
| `raw_blocks` | 265,912 | ~102 MiB | 2021-06-11 → 2026-06-11 | ✅ OK |
| `raw_transactions` | 728,801,169 | 75.16 GiB | 2021-05-31 → 2026-06-11 | ✅ OK |

---

### 📌 Key Statistics

#### 📈 OHLCV (Harga Bitcoin)

| Metrik | Nilai |
|--------|------:|
| Harga Terendah | **$3.80** *(2012)* |
| Harga Tertinggi | **$126,202** *(Okt 2025)* |
| Rata-rata Harga | **$23,378** |
| Volume Terbesar | **5,853 BTC** *(Okt 2014)* |

#### 🧱 Blocks

| Metrik | Nilai |
|--------|------:|
| Total Block | **265,912** |
| Rata-rata Block/Hari | **~146** |
| Rata-rata Tx/Block | **2,741** |
| Top Miner | **Foundry USA Pool** *(65,101 blocks)* |

#### 💸 Transactions

| Metrik | Nilai |
|--------|------:|
| Total Transaksi | **728,801,169** |
| Rata-rata Fee | **6,901 satoshi** |
| Fee Tertinggi | **8,365,497,568 satoshi** |
| Rata-rata Input | **2.42** |
| Rata-rata Output | **2.78** |

---

## ⚠️ Troubleshooting

### ❌ Error 1 — Permission Denied (Docker)

```
Error: permission denied while trying to connect to the Docker API
```

**Penyebab:** User tidak terdaftar di group `docker`.

**Solusi:**
```bash
sudo usermod -aG docker ingestion
# Kemudian logout dan login kembali
```

---

### ❌ Error 2 — Import Gagal (Header Issue)

```
Error: Cannot parse DateTime: unexpected word
```

**Penyebab:** Baris pertama file TSV adalah header, bukan data.

**Solusi:** Gunakan `tail -n +2` untuk skip header:
```bash
tail -n +2 file.tsv \
  | docker exec -i clickhouse clickhouse-client \
      --query "INSERT INTO btc.table FORMAT TSV"
```

---

### ❌ Error 3 — No Space Left on Device

**Penyebab:** Disk penuh karena file `.tsv` tidak dihapus setelah import.

**Solusi:** Bersihkan file `.tsv` yang sudah diproses:
```bash
rm -f /opt/bitcoin-realtime-forecasting-platform/data/imports/incoming/*.tsv
```

---

## 📁 File Locations

| Path | Deskripsi |
|------|-----------|
| `/opt/bitcoin-realtime-forecasting-platform/` | 📂 Root directory project |
| `.../data/imports/incoming/` | 📥 Staging folder untuk file upload |
| `.../data/imports/processed/` | ✅ File yang sudah diproses (jika tidak dihapus) |
| `.../data/imports/rejected/` | ❌ File yang gagal import |
| `.../data/imports/metadata/` | 📋 Log files ingestion |
| `.../scripts/` | ⚙️ Automation scripts |

---

## 🔗 Referensi

| Resource | Link |
|----------|------|
| 📖 ClickHouse Docs | [clickhouse.com/docs](https://clickhouse.com/docs) |
| 💻 Project Repository | [github.com/Azzt17/bitcoin-realtime-forecasting-platform](https://github.com/Azzt17/bitcoin-realtime-forecasting-platform) |
| 🗂️ Sumber Data | [blockchair.com](https://blockchair.com/) |

---

<div align="center">

**📊 Bitcoin Realtime Forecasting Platform**  
*ClickHouse Ingestion Documentation v1.0*  
Last Updated: 2026-06-20

</div>
