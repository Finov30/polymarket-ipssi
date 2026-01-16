import pandas as pd
from pathlib import Path
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
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

# Clés uniques pour éviter les doublons
UNIQUE_KEYS = {
    "truthsocial_posts": ["post_id"],
    "price_change": ["event_ts", "market_id", "price"],
    "trade": ["event_ts", "market_id", "price"],
    "new_market": ["market_id"],
    "market_resolved": ["event_ts", "market_id"],
    "tick_change": ["event_ts", "market_id"],
}


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


def ensure_unique_index(collection, collection_name: str):
    """Crée un index unique pour éviter les doublons."""
    if collection_name in UNIQUE_KEYS:
        keys = UNIQUE_KEYS[collection_name]
        index_fields = [(k, 1) for k in keys]
        index_name = f"unique_{'_'.join(keys)}"

        existing_indexes = [idx["name"] for idx in collection.list_indexes()]
        if index_name not in existing_indexes:
            collection.create_index(index_fields, unique=True, name=index_name, background=True)
            print(f"[INDEX] Créé index unique sur {collection_name}: {keys}")


def load_parquet_to_mongo(parquet_path: Path, collection_name: str):
    """Charge un fichier parquet dans MongoDB avec déduplication automatique."""
    df = pd.read_parquet(parquet_path)

    if df.empty:
        print(f"[SKIP] {parquet_path} vide")
        return

    records = df.to_dict(orient="records")
    sanitized_records = [sanitize_for_mongo(r) for r in records]

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[collection_name]

    # Créer l'index unique si nécessaire
    ensure_unique_index(collection, collection_name)

    # Utiliser upsert pour éviter les doublons
    unique_keys = UNIQUE_KEYS.get(collection_name, [])

    if unique_keys:
        # Mode UPSERT : met à jour si existe, insère sinon
        total = len(sanitized_records)
        inserted = 0
        updated = 0

        for i in range(0, total, BATCH_SIZE):
            batch = sanitized_records[i:i+BATCH_SIZE]
            operations = []

            for record in batch:
                # Construire le filtre avec les clés uniques
                filter_doc = {k: record.get(k) for k in unique_keys if k in record}
                if filter_doc:
                    operations.append(
                        UpdateOne(filter_doc, {"$set": record}, upsert=True)
                    )

            if operations:
                try:
                    result = collection.bulk_write(operations, ordered=False)
                    inserted += result.upserted_count
                    updated += result.modified_count
                except BulkWriteError as e:
                    # Ignorer les erreurs de doublons
                    inserted += e.details.get("nUpserted", 0)
                    updated += e.details.get("nModified", 0)

            print(f"[MONGO] Batch {i//BATCH_SIZE+1} → {collection_name} (nouveaux: {inserted}, mis à jour: {updated})")

        print(f"[MONGO] {collection_name}: {inserted} nouveaux, {updated} mis à jour, {total - inserted - updated} doublons ignorés")
    else:
        # Fallback: insert simple (pour collections sans clé unique définie)
        total = len(sanitized_records)
        for i in range(0, total, BATCH_SIZE):
            batch = sanitized_records[i:i+BATCH_SIZE]
            try:
                collection.insert_many(batch, ordered=False)
            except BulkWriteError:
                pass  # Ignorer les doublons
            print(f"[MONGO] Batch {i//BATCH_SIZE+1} → {len(batch)} docs traités dans {collection_name}")

    client.close()


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
