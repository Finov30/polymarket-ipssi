import subprocess
from pathlib import Path

def run_polymarket_ws():
    subprocess.run(
        ["python", "polymarket_ws.py"],
        check=True
    )

def run_raw_to_parquet():
    raw_files = Path("data/raw/polymarket").rglob("*.jsonl")

    for raw_file in raw_files:
        subprocess.run(
            ["python", "raw_to_parquet.py", str(raw_file)],
            check=True
        )

def main():
    try:
        print("[PIPELINE] Démarrage Polymarket WS")
        run_polymarket_ws()

    except KeyboardInterrupt:
        print("[PIPELINE] WS arrêté")

    finally:
        print("[PIPELINE] Conversion RAW → Parquet")
        run_raw_to_parquet()

        print("[PIPELINE] Terminé")

if __name__ == "__main__":
    main()