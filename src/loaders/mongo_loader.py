import pandas as pd
from pathlib import Path
from pymongo import MongoClient
import numpy as np
import dotenv
import os

dotenv.load_dotenv()

# Config
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "polymarket_db"
PARQUET_BASE_POLY = Path("data/parquet/polymarket")
PARQUET_BASE_TRUTH = Path("data/parquet/truthsocial/posts")
BATCH_SIZE = 1000  # taille des inserts en batch pour éviter surcharge mémoire


def sanitize_for_mongo(record: dict) -> dict:
    """Convertit automatiquement tous les types non-MongoDB en types compatibles."""
    sanitized = {}
    for key, value in record.items():
        if isinstance(value, np.ndarray):
            sanitized[key] = value.tolist()
        elif isinstance(value, (np.int64, np.int32)):
            sanitized[key] = int(value)
        elif isinstance(value, (np.float64, np.float32)):
            sanitized[key] = float(value)
        elif isinstance(value, pd.Timestamp):
            sanitized[key] = value.isoformat()
        else:
            sanitized[key] = value
    return sanitized


def load_parquet_to_mongo(parquet_path: Path, collection_name: str):
    """Charge un fichier parquet dans MongoDB avec conversion automatique des types."""
    df = pd.read_parquet(parquet_path)

    if df.empty:
        print(f"[SKIP] {parquet_path} vide")
        return

    records = df.to_dict(orient="records")
    sanitized_records = [sanitize_for_mongo(r) for r in records]

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[collection_name]

    # Insert en batch pour éviter overload
    total = len(sanitized_records)
    for i in range(0, total, BATCH_SIZE):
        batch = sanitized_records[i:i+BATCH_SIZE]
        collection.insert_many(batch)
        print(f"[MONGO] Batch {i//BATCH_SIZE+1} → {len(batch)} docs insérés dans {collection_name}")

    client.close()
    print(f"[MONGO] {total} documents insérés au total → {collection_name}")


def main():
    # Polymarket
    for event_type_dir in PARQUET_BASE_POLY.iterdir():
        if not event_type_dir.is_dir():
            continue
        for parquet_file in event_type_dir.glob("date=*/**/*.parquet"):
            load_parquet_to_mongo(parquet_file, event_type_dir.name)

   # TruthSocial 
    for parquet_file in PARQUET_BASE_TRUTH.glob("date=*/**/*.parquet"):
        load_parquet_to_mongo(parquet_file, "truthsocial_posts")    


if __name__ == "__main__":
    main()
