import json
from pathlib import Path
import pandas as pd

# Paths
RAW_BASE = Path("data/raw/truthsocial")
PARQUET_BASE = Path("data/parquet/truthsocial/posts")

def process_raw_file(raw_file: Path):
    records = []

    with open(raw_file, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            payload = json.loads(line)

            raw = payload.get("raw", {})
            media = raw.get("media_attachments", [])

            record = {
                "post_id": payload.get("post_id"),
                "user_id": payload.get("user_id"),
                "username": payload.get("username"),
                "created_at": pd.to_datetime(payload.get("created_at"), utc=True),
                "ingestion_ts": pd.to_datetime(payload.get("ingestion_ts"), utc=True),
                "content": raw.get("content"),
                "replies_count": raw.get("replies_count"),
                "reblogs_count": raw.get("reblogs_count"),
                "favourites_count": raw.get("favourites_count"),
                "upvotes_count": raw.get("upvotes_count"),
                "downvotes_count": raw.get("downvotes_count"),
                "has_media": len(media) > 0,
                "media_types": [m.get("type") for m in media if m.get("type")],
            }

            records.append(record)

    if not records:
        print(f"[SKIP] {raw_file.name} vide")
        return

    df = pd.DataFrame(records)

    event_date = df["ingestion_ts"].dt.date.iloc[0]

    output_dir = PARQUET_BASE / f"date={event_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{raw_file.stem}.parquet"
    df.to_parquet(output_file, index=False)

    print(f"[PARQUET] TruthSocial → {output_file} ({len(df)} posts)")

def main():
    raw_files = sorted(RAW_BASE.rglob("date=*/hour=*/*.jsonl"))

    if not raw_files:
        print("[INFO] Aucun fichier TruthSocial RAW trouvé")
        return

    for raw_file in raw_files:
        process_raw_file(raw_file)

if __name__ == "__main__":
    main()
