import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

PIPELINE = [
    {
        "name": "Ingestion Polymarket WS",
        "cmd": [sys.executable, "src/ingestion/polymarket_ws.py"],
    },
    {
        "name": "Ingestion TruthSocial",
        "cmd": [sys.executable, "src/ingestion/truthsocial_api.py"],
    },
    {
        "name": "Processing Polymarket → Parquet",
        "cmd": [sys.executable, "src/processing/raw_to_parquet_pm.py"],
    },
    {
        "name": "Processing TruthSocial → Parquet",
        "cmd": [sys.executable, "src/processing/raw_to_parquet_ts.py"],
    },
    {
        "name": "Load MongoDB",
        "cmd": [sys.executable, "src/loaders/mongo_loader.py"],
    },
]

def run_step(step):
    print(f"\n{'='*60}")
    print(f"[PIPELINE] START → {step['name']}")
    print(f"{'='*60}")

    result = subprocess.run(
        step["cmd"],
        cwd=ROOT.parent,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"❌ Étape échouée: {step['name']}")

    print(result.stdout)
    print(f"[PIPELINE] OK → {step['name']}")

def main():
    for step in PIPELINE:
        run_step(step)

    print("\n✅ PIPELINE TERMINÉE AVEC SUCCÈS")

if __name__ == "__main__":
    main()
