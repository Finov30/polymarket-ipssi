import json
from pathlib import Path
from datetime import datetime

import pandas as pd

# Paths
PARQUET_BASE = Path("data/parquet/polymarket")
RAW_BASE = Path("data/raw/polymarket")

EVENT_TYPES = [
    "price_change",
    "trade",
    "new_market",
    "market_resolved",
    "tick_change",
]

def parse_event_ts(ts):
    """
    Timestamp Polymarket en ms -> datetime
    """
    if ts is None:
        return None
    return pd.to_datetime(int(ts), unit="ms", utc=True)

def process_raw_file(raw_file: Path):
    records = []

    with open(raw_file, "r", encoding="utf-8") as f:
        for line in f:
            record = json.loads(line)
            records.append(record)

    if not records:
        return

    df = pd.json_normalize(records)

    # timestamps
    df["ingestion_ts"] = pd.to_datetime(df["ingestion_ts"], utc=True)
    df["event_ts"] = df["raw.timestamp"].apply(parse_event_ts)

    for event_type in EVENT_TYPES:
        df_event = df[df["event_type"] == event_type]

        if df_event.empty:
            continue

        # Partition par date d’ingestion
        event_date = df_event["ingestion_ts"].dt.date.iloc[0]

        output_dir = (
            PARQUET_BASE
            / event_type
            / f"date={event_date}"
        )
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f"{raw_file.stem}.parquet"

        # Sélection des colonnes communes
        base_cols = [
            "event_type",
            "market_id",
            "event_ts",
            "ingestion_ts",
        ]

        # Colonnes spécifiques dans raw.*
        raw_cols = [c for c in df_event.columns if c.startswith("raw.")]

        df_out = df_event[base_cols + raw_cols].copy()

        # Nettoyage noms colonnes
        df_out.columns = [c.replace("raw.", "") for c in df_out.columns]

        df_out.to_parquet(output_file, index=False)
        print(f"[PARQUET] {event_type} → {output_file}")

def main():
    raw_files = sorted(RAW_BASE.rglob("date=*/hour=*/*.jsonl"))

    if not raw_files:
        print("[INFO] Aucun fichier RAW trouvé")
        return

    for raw_file in raw_files:
        process_raw_file(raw_file)

if __name__ == "__main__":
    main()
