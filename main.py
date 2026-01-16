import subprocess
import sys
import os
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Configuration: intervalle entre chaque cycle (en secondes)
# Par défaut 5 minutes (300s), configurable via PIPELINE_INTERVAL_SECONDS
PIPELINE_INTERVAL = int(os.getenv("PIPELINE_INTERVAL_SECONDS", "300"))

# Mode continu activé par défaut, désactivable via PIPELINE_CONTINUOUS=false
CONTINUOUS_MODE = os.getenv("PIPELINE_CONTINUOUS", "true").lower() == "true"

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
        cwd=ROOT,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"❌ Étape échouée: {step['name']}")

    print(result.stdout)
    print(f"[PIPELINE] OK → {step['name']}")

def run_pipeline_cycle(cycle_num: int):
    """Exécute un cycle complet du pipeline."""
    print(f"\n{'#'*60}")
    print(f"# CYCLE {cycle_num} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")

    for step in PIPELINE:
        try:
            run_step(step)
        except RuntimeError as e:
            print(f"⚠️ Erreur dans le cycle {cycle_num}: {e}")
            print("Continuation vers l'étape suivante...")
            continue

    print(f"\n✅ CYCLE {cycle_num} TERMINÉ")

def main():
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║          POLYMARKET PIPELINE - MODE CONTINU                  ║
╠══════════════════════════════════════════════════════════════╣
║  Mode continu: {str(CONTINUOUS_MODE).upper():<44} ║
║  Intervalle entre cycles: {PIPELINE_INTERVAL} secondes{' '*(25-len(str(PIPELINE_INTERVAL)))} ║
║  Démarré à: {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<47} ║
╚══════════════════════════════════════════════════════════════╝
    """)

    cycle = 1

    while True:
        run_pipeline_cycle(cycle)

        if not CONTINUOUS_MODE:
            print("\n🏁 Mode single-run: arrêt du pipeline.")
            break

        print(f"\n⏳ Prochain cycle dans {PIPELINE_INTERVAL} secondes...")
        print(f"   (Prochain: {datetime.now().strftime('%H:%M:%S')} + {PIPELINE_INTERVAL}s)")
        time.sleep(PIPELINE_INTERVAL)
        cycle += 1

if __name__ == "__main__":
    main()
