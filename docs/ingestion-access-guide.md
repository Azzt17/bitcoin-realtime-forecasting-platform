# Panduan Akses Analytics Node untuk Tim Data Ingestion

## Tujuan

Dokumen ini menjelaskan apa yang perlu disiapkan oleh anggota tim data ingestion agar bisa mengakses `analytics-node` secara aman untuk upload dan staging data historical.

Akses ini hanya untuk kebutuhan ingestion data, bukan untuk mengelola server, Terraform, Kafka, atau konfigurasi infrastruktur.

## Prinsip Akses

Akses yang diberikan harus terbatas.

```text
Node yang boleh diakses: analytics-node
User Linux: ingestion
Akses sudo: tidak
Akses root: tidak
Akses DigitalOcean: tidak
Akses Terraform state/token: tidak
Akses Kafka node langsung: tidak
```

Teman ingestion hanya perlu akses ke folder staging data:

```text
/opt/bitcoin-realtime-forecasting-platform/data/imports/
```

Struktur folder:

```text
/opt/bitcoin-realtime-forecasting-platform/data/imports/
├── incoming/
├── processed/
└── rejected/
```

Keterangan:

```text
incoming/   tempat upload file mentah
processed/  tempat file yang sudah berhasil diproses
rejected/   tempat file gagal/invalid
```

## Yang Harus Diberikan Teman ke Farid

Teman ingestion cukup memberikan:

```text
1. Public SSH key
2. Nama identitas untuk username/catatan akses
3. Jenis data yang akan diupload
4. Estimasi ukuran file
5. Format file
```

Contoh format informasi yang dikirim:

```text
Nama: <nama teman>
Role: data ingestion
Public SSH key:
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI... nama-teman-ingestion

Data yang akan diupload:
OHLCV historical 1-minute

Format:
CSV

Estimasi ukuran:
<misalnya 2 GB / 10 GB / 50 GB>
```

Yang tidak boleh dikirim:

```text
private key
password laptop
token DigitalOcean
token API
file .pem private
screenshot isi private key
```

## Step untuk Teman Ingestion

### 1. Cek apakah sudah punya SSH key

Jalankan di laptop teman:

```bash
ls -lah ~/.ssh
```

Cari file seperti:

```text
id_ed25519
id_ed25519.pub
```

Jika ada file `.pub`, lanjut ke step 3.

### 2. Buat SSH key jika belum punya

```bash
ssh-keygen -t ed25519 -C "nama-teman-ingestion"
```

Saat ditanya lokasi file, tekan Enter untuk default:

```text
~/.ssh/id_ed25519
```

Passphrase boleh diisi untuk keamanan tambahan.

### 3. Kirim public key ke Farid

Jalankan:

```bash
cat ~/.ssh/id_ed25519.pub
```

Kirim output yang muncul ke Farid.

Contoh bentuk public key:

```text
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIxxxxxxxxxxxxxxxxxxxxxxxx nama-teman-ingestion
```

Hanya kirim file `.pub`.

Jangan kirim:

```text
~/.ssh/id_ed25519
```

Karena itu private key.

## Step yang Dilakukan Farid di Analytics Node

### 1. Masuk ke analytics-node sebagai root

Dari laptop Farid:

```bash
ssh -i ~/.ssh/bitcoin_realtime_platform root@<ANALYTICS_NODE_PUBLIC_IP>
```

### 2. Buat user khusus ingestion

Di dalam analytics-node:

```bash
adduser --disabled-password --gecos "" ingestion
```

### 3. Siapkan SSH authorized_keys

```bash
mkdir -p /home/ingestion/.ssh
nano /home/ingestion/.ssh/authorized_keys
```

Paste public key teman ke file tersebut.

Satu public key per baris.

### 4. Set permission SSH

```bash
chown -R ingestion:ingestion /home/ingestion/.ssh
chmod 700 /home/ingestion/.ssh
chmod 600 /home/ingestion/.ssh/authorized_keys
```

### 5. Buat folder staging data

```bash
mkdir -p /opt/bitcoin-realtime-forecasting-platform/data/imports/incoming
mkdir -p /opt/bitcoin-realtime-forecasting-platform/data/imports/processed
mkdir -p /opt/bitcoin-realtime-forecasting-platform/data/imports/rejected
```

### 6. Berikan kepemilikan folder ke user ingestion

```bash
chown -R ingestion:ingestion /opt/bitcoin-realtime-forecasting-platform/data/imports
chmod -R 750 /opt/bitcoin-realtime-forecasting-platform/data/imports
```

### 7. Cek user dan folder

```bash
id ingestion
ls -lah /opt/bitcoin-realtime-forecasting-platform/data/imports
```

## Step Test Login oleh Teman

Setelah Farid menambahkan public key, teman bisa test login:

```bash
ssh ingestion@<ANALYTICS_NODE_PUBLIC_IP>
```

Jika SSH key tidak berada di default path, gunakan:

```bash
ssh -i ~/.ssh/id_ed25519 ingestion@<ANALYTICS_NODE_PUBLIC_IP>
```

Jika berhasil, cek lokasi kerja:

```bash
pwd
ls -lah /opt/bitcoin-realtime-forecasting-platform/data/imports
```

## Step Upload Data oleh Teman

### Upload satu file

```bash
scp -i ~/.ssh/id_ed25519 file_historical.csv ingestion@<ANALYTICS_NODE_PUBLIC_IP>:/opt/bitcoin-realtime-forecasting-platform/data/imports/incoming/
```

### Upload folder

```bash
scp -i ~/.ssh/id_ed25519 -r folder_data/ ingestion@<ANALYTICS_NODE_PUBLIC_IP>:/opt/bitcoin-realtime-forecasting-platform/data/imports/incoming/
```

### Upload dengan rsync

Untuk file besar, `rsync` lebih baik karena bisa resume sebagian transfer.

```bash
rsync -avh --progress -e "ssh -i ~/.ssh/id_ed25519" file_historical.csv ingestion@<ANALYTICS_NODE_PUBLIC_IP>:/opt/bitcoin-realtime-forecasting-platform/data/imports/incoming/
```

## Konvensi Nama File

Gunakan nama file yang jelas:

```text
btc_ohlcv_1m_2012_2026.csv
btc_blocks_2021_2026.csv
btc_transactions_sample_2021_2026.csv
```

Hindari:

```text
data.csv
final.csv
final_banget.csv
fix.csv
baru.csv
```

## Informasi Metadata yang Harus Disertakan

Setiap upload data historical sebaiknya disertai file metadata kecil:

```text
README_<nama_dataset>.txt
```

Isi minimal:

```text
Dataset name:
Source:
Time range:
Timezone:
Rows:
Columns:
File format:
Compression:
Missing value policy:
Owner:
Upload date:
Notes:
```

Contoh:

```text
Dataset name: BTC OHLCV 1-minute
Source: historical market dataset
Time range: 2012-01-01 to 2026-xx-xx
Timezone: UTC
Rows: 7607549
Columns: Timestamp, Open, High, Low, Close, Volume
File format: CSV
Compression: none
Owner: data ingestion team
Upload date: 2026-06-19
Notes: Timestamp is Unix seconds.
```

## Batasan untuk Teman Ingestion

Teman ingestion tidak boleh:

```text
mengubah file sistem
menjalankan sudo
menghapus service Docker
mengubah konfigurasi Kafka/Spark/ClickHouse
mengakses Terraform
mengakses DigitalOcean dashboard
mengupload API key ke server tanpa koordinasi
mengubah folder selain folder imports
```

Jika butuh menjalankan script ingestion, koordinasikan nama script dan command-nya dulu dengan Farid.

## Checklist Farid Sebelum Memberikan Akses

```text
[ ] Public key teman sudah diterima
[ ] Public key adalah .pub, bukan private key
[ ] User ingestion sudah dibuat
[ ] authorized_keys sudah diisi
[ ] Permission .ssh sudah benar
[ ] Folder imports sudah dibuat
[ ] Permission folder imports sudah benar
[ ] Teman berhasil login
[ ] Teman berhasil upload file kecil untuk test
[ ] Tidak ada akses root/sudo untuk user ingestion
```

## Checklist Teman Sebelum Upload Data Besar

```text
[ ] SSH login berhasil
[ ] Upload file kecil berhasil
[ ] Nama file sudah jelas
[ ] Metadata dataset disiapkan
[ ] Ukuran file sudah diinformasikan ke Farid
[ ] Format file sudah diinformasikan ke Farid
[ ] Tidak mengirim private key
```

## Troubleshooting

### Permission denied publickey

Kemungkinan:

```text
public key belum dipasang
public key salah
private key yang dipakai tidak cocok
permission .ssh salah
```

Coba:

```bash
ssh -v -i ~/.ssh/id_ed25519 ingestion@<ANALYTICS_NODE_PUBLIC_IP>
```

### Upload putus di tengah

Gunakan `rsync`:

```bash
rsync -avh --progress -e "ssh -i ~/.ssh/id_ed25519" file_historical.csv ingestion@<ANALYTICS_NODE_PUBLIC_IP>:/opt/bitcoin-realtime-forecasting-platform/data/imports/incoming/
```

### File terlalu besar

Koordinasikan dulu sebelum upload.

Opsi yang bisa dipakai:

```text
split file
compress file
upload bertahap
gunakan rsync
gunakan object storage jika nanti diperlukan
```

## Status

Dokumen ini berlaku untuk tahap awal ingestion historical data.

Akses dapat diperketat lagi setelah pipeline ClickHouse ingestion dan service contract final tersedia.
