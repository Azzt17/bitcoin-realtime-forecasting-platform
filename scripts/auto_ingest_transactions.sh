#!/bin/bash

# ==========================================
# AUTO INGEST TRANSACTIONS (TANPA MV)
# ==========================================

BASE_DIR="/home/ingestion/opt/bitcoin-realtime-forecasting-platform"
INCOMING_DIR="$BASE_DIR/data/imports/incoming"
REJECTED_DIR="$BASE_DIR/data/imports/rejected"
LOG_DIR="$BASE_DIR/data/imports/metadata"

mkdir -p "$REJECTED_DIR" "$LOG_DIR"

LOG_FILE="$LOG_DIR/ingest_$(date +%Y%m%d_%H%M%S).log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

process_file() {
    local gz_file="$1"
    local filename=$(basename "$gz_file")
    local base_name="${filename%.tsv.gz}"
    local tsv_file="$base_name.tsv"
    
    log "=========================================="
    log "Processing: $filename"
    log "=========================================="
    
    # Extract .gz
    log "Extracting $filename..."
    cd "$INCOMING_DIR"
    if ! gunzip -f "$gz_file" 2>>"$LOG_FILE"; then
        log "ERROR: Failed to extract $filename"
        return 1
    fi
    log "Extract complete: $tsv_file"
    
    # Fix ownership
    log "Fixing ownership..."
    chown ingestion:ingestion "$tsv_file" 2>>"$LOG_FILE"
    
    # Check header
    log "Checking file format..."
    local first_line=$(head -1 "$tsv_file" 2>>"$LOG_FILE")
    if [[ "$first_line" == *"block_id"* ]] || [[ "$first_line" == *"hash"* ]] || [[ "$first_line" == *"time"* ]]; then
        log "Header detected, will skip first line"
        local skip_header=true
    else
        local skip_header=false
    fi
    
    # Import ke ClickHouse
    log "Importing $tsv_file to ClickHouse..."
    local import_start=$(date +%s)
    
    if [ "$skip_header" = true ]; then
        if tail -n +2 "$tsv_file" | sudo docker exec -i clickhouse clickhouse-client --query "INSERT INTO btc.raw_transactions FORMAT TSV" 2>>"$LOG_FILE"; then
            local import_status=0
        else
            local import_status=1
        fi
    else
        if sudo docker exec -i clickhouse clickhouse-client --query "INSERT INTO btc.raw_transactions FORMAT TSV" < "$tsv_file" 2>>"$LOG_FILE"; then
            local import_status=0
        else
            local import_status=1
        fi
    fi
    
    local import_end=$(date +%s)
    local import_duration=$((import_end - import_start))
    
    if [ $import_status -eq 0 ]; then
        log "Import complete in ${import_duration}s"
        
        # Validasi
        local row_count=$(sudo docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM btc.raw_transactions" 2>/dev/null | tail -1)
        log "Total rows in raw_transactions: $row_count"
        
        # HAPUS file .tsv (hemat space!)
        log "Deleting $tsv_file (already imported)..."
        rm -f "$tsv_file" 2>>"$LOG_FILE"
        log "File deleted: $tsv_file"
        log "File .gz tetap di incoming/: $filename"
        
        return 0
    else
        log "ERROR: Import failed!"
        log "Moving $tsv_file to rejected/"
        mv "$tsv_file" "$REJECTED_DIR/" 2>>"$LOG_FILE"
        return 1
    fi
}

# ==========================================
# MAIN
# ==========================================
log "=========================================="
log "STARTING AUTO INGESTION (NO MV)"
log "=========================================="
log "Incoming directory: $INCOMING_DIR"
log "Rejected directory: $REJECTED_DIR"
log "=========================================="

total_files=$(find "$INCOMING_DIR" -maxdepth 1 -name "*.tsv.gz" | wc -l)
log "Found $total_files .tsv.gz files to process"

if [ $total_files -eq 0 ]; then
    log "No files to process. Exiting."
    exit 0
fi

processed=0
success=0
failed=0

for gz_file in "$INCOMING_DIR"/*.tsv.gz; do
    if [ -f "$gz_file" ]; then
        processed=$((processed + 1))
        log ""
        log "[$processed/$total_files] Processing file..."
        
        if process_file "$gz_file"; then
            success=$((success + 1))
            log "[$processed/$total_files] SUCCESS"
        else
            failed=$((failed + 1))
            log "[$processed/$total_files] FAILED"
        fi
        
        log "Progress: $processed/$total_files files processed"
        log "   Success: $success"
        log "   Failed: $failed"
    fi
done

log ""
log "=========================================="
log "INGESTION COMPLETE!"
log "=========================================="
log "Total files: $total_files"
log "Success: $success"
log "Failed: $failed"
log "Failed files: $REJECTED_DIR"
log "Log file: $LOG_FILE"
log "=========================================="

final_count=$(sudo docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM btc.raw_transactions" 2>/dev/null | tail -1)
log "Total rows in raw_transactions: $final_count"

log "Script finished!"
