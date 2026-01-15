import json
from pathlib import Path
from datetime import datetime
import pandas as pd

# Paths
RAW_BASE = Path("data/raw/truthsocial")
PARQUET_BASE = Path("data/parquet/truthsocial/posts")

def process_raw_file(raw_file: Path):
    with open(raw_file, "r", encoding="utf-8") as f:
        payload = json.load(f)

    user = payload.get("user", {})
    posts = payload.get("posts", [])

    if not posts:
        print(f"[SKIP] Aucun post dans {raw_file.name}")
        return

    records = []
    ingestion_ts = datetime.now()

    for post in posts:
        media = post.get("media_attachments", [])
        media_types = [m.get("type") for m in media if m.get("type")]

        record = {
            "post_id": post.get("id"),
            "created_at": pd.to_datetime(post.get("created_at"), utc=True),
            "author_id": user.get("id"),
            "author_username": user.get("username"),
            "content": post.get("content"),
            "uri": post.get("uri"),
            "replies_count": post.get("replies_count"),
            "reblogs_count": post.get("reblogs_count"),
            "favourites_count": post.get("favourites_count"),
            "upvotes_count": post.get("upvotes_count"),
            "downvotes_count": post.get("downvotes_count"),
            "has_media": len(media) > 0,
            "media_types": media_types,
            "ingestion_ts": ingestion_ts,
        }

        records.append(record)

    df = pd.DataFrame(records)

    if df.empty:
        return

    event_date = ingestion_ts.date()

    output_dir = PARQUET_BASE / f"date={event_date}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{raw_file.stem}.parquet"
    df.to_parquet(output_file, index=False)

    print(f"[PARQUET] TruthSocial → {output_file} ({len(df)} posts)")

def main():
    raw_files = sorted(RAW_BASE.rglob("*.json"))

    if not raw_files:
        print("[INFO] Aucun fichier TruthSocial RAW trouvé")
        return

    for raw_file in raw_files:
        process_raw_file(raw_file)

if __name__ == "__main__":
    main()
