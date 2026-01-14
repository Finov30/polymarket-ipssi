#!/usr/bin/env python3
"""
Polymarket WebSocket Client
Récupère en temps réel: orderbook, prix, trades, nouveaux marchés
"""

import asyncio
import json
import signal
import sys
import urllib.request
from datetime import datetime
from pathlib import Path
import os
try:
    import websockets
except ImportError:
    print("Installing websockets...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
    import websockets

# Configuration
WSS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API = "https://gamma-api.polymarket.com/markets"
OUTPUT_FILE = Path("polymarket_data.json")
MAX_MARKETS = 20  # Nombre de marchés à suivre

# Structure de données pour stocker les événements
data_store = {   
    "price_changes": [],   # Liste des changements de prix
    "trades": [],          # Liste des trades (last_trade_price)
    "new_markets": [],     # Nouveaux marchés créés
    "market_resolved": [], # Marchés résolus
    "tick_changes": [],    # Changements de tick size
}

# Flag pour arrêt propre
running = True

def on_message(ws, message):
    msg = json.loads(message)

    print("\n==============================")
    print("RAW WS MESSAGE")
    print(json.dumps(msg, indent=2)[:1500])
    print("==============================\n")

def save_data():
    """
    Sauvegarde RAW Polymarket : événements atomiques (append-only)
    """
    
    RAW_BASE_PATH = "data/raw/polymarket"   
    now = datetime.now()

    output_dir = os.path.join(
        "data/raw/polymarket",
        f"date={now:%Y-%m-%d}",
        f"hour={now:%H}"
    )
    os.makedirs(output_dir, exist_ok=True)

    filename = f"polymarket_ws_{now:%Y%m%d_%H%M%S}.jsonl"
    output_path = os.path.join(output_dir, filename)

    event_types = {
        "price_changes": "price_change",
        "trades": "trade",
        "new_markets": "new_market",
        "market_resolved": "market_resolved",
        "tick_changes": "tick_change"
    }

    records_written = 0

    with open(output_path, "a", encoding="utf-8") as f:
        for key, event_type in event_types.items():
            events = data_store.get(key, [])
            for event in events:
                record = {
                    "event_type": event_type,
                    "event_ts": event.get("received_at"),
                    "ingestion_ts": now.isoformat(),
                    "market_id": event.get("id") or event.get("market"),
                    "raw": event
                }
                json.dump(record, f, ensure_ascii=False)
                f.write("\n")
                records_written += 1

    print(f"[SAVE] {records_written} événements RAW sauvegardés → {output_path}")

    # reset buffers
    for key in event_types.keys():
        data_store[key] = []


def fetch_active_markets():
    """Récupère les marchés actifs depuis l'API Gamma"""
    print("[INFO] Récupération des marchés actifs...")
    url = f"{GAMMA_API}?closed=false&active=true&limit={MAX_MARKETS}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as response:
            markets = json.loads(response.read().decode())

        token_ids = []
        for m in markets:
            if m.get("enableOrderBook") and m.get("clobTokenIds"):
                token_ids.extend(json.loads(m["clobTokenIds"]))

        print(f"[INFO] {len(token_ids)} tokens suivis")
        return token_ids

    except Exception as e:
        print(f"[ERROR] fetch_active_markets: {e}")
        return []


def handle_message(message: dict):
    """Traite uniquement les événements utiles (event-sourcing pur)"""
    event_type = message.get("event_type")
    received_at = datetime.now().isoformat()

    if event_type == "price_change":
        for pc in message.get("price_changes", []):
            data_store["price_changes"].append({
                "market": message.get("market"),
                "asset_id": pc.get("asset_id"),
                "price": pc.get("price"),
                "size": pc.get("size"),
                "side": pc.get("side"),
                "timestamp": message.get("timestamp"),
                "received_at": received_at
            })

    elif event_type == "last_trade_price":
        data_store["trades"].append({
            "market": message.get("market"),
            "asset_id": message.get("asset_id"),
            "price": message.get("price"),
            "size": message.get("size"),
            "side": message.get("side"),
            "timestamp": message.get("timestamp"),
            "received_at": received_at
        })

    elif event_type == "new_market":
        data_store["new_markets"].append({
            "id": message.get("id"),
            "question": message.get("question"),
            "market": message.get("market"),
            "slug": message.get("slug"),
            "description": message.get("description"),
            "assets_ids": message.get("assets_ids", []),
            "outcomes": message.get("outcomes", []),
            "timestamp": message.get("timestamp"),
            "received_at": received_at
        })

    elif event_type == "market_resolved":
        data_store["market_resolved"].append({
            "id": message.get("id"),
            "market": message.get("market"),
            "winning_asset_id": message.get("winning_asset_id"),
            "winning_outcome": message.get("winning_outcome"),
            "timestamp": message.get("timestamp"),
            "received_at": received_at
        })

    elif event_type == "tick_size_change":
        data_store["tick_changes"].append({
            "market": message.get("market"),
            "asset_id": message.get("asset_id"),
            "old_tick_size": message.get("old_tick_size"),
            "new_tick_size": message.get("new_tick_size"),
            "timestamp": message.get("timestamp"),
            "received_at": received_at
        })


async def subscribe_to_markets(ws, token_ids):
    """Envoie le message de souscription au canal market"""
    subscribe_msg = {
        "assets_ids": token_ids,
        "custom_feature_enabled": True
    }

    await ws.send(json.dumps(subscribe_msg))
    print(f"[INFO] Souscription envoyée pour {len(token_ids)} tokens")


async def connect_and_listen():
    """Connexion au WebSocket et écoute des messages"""
    global running

    # Récupérer les marchés actifs
    token_ids = fetch_active_markets()
    if not token_ids:
        print("[ERROR] Aucun marché trouvé, arrêt.")
        return

    print(f"\n[INFO] Connexion à {WSS_URL}")

    retry_count = 0
    max_retries = 5

    while running and retry_count < max_retries:
        try:
            async with websockets.connect(
                WSS_URL,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=5
            ) as ws:
                print("[INFO] Connecté!")
                retry_count = 0  # Reset on successful connection

                await subscribe_to_markets(ws, token_ids)

                while running:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=120)

                        # Gérer les messages PING/PONG
                        if message == "PING":
                            await ws.send("PONG")
                            continue

                        try:
                            data = json.loads(message)
                        except json.JSONDecodeError:
                            print(f"[DEBUG] Message non-JSON: {message[:100]}")
                            continue

                        # Peut être un message unique ou une liste
                        if isinstance(data, list):
                            for item in data:
                                handle_message(item)
                        else:
                            handle_message(data)

                    except asyncio.TimeoutError:
                        print("[INFO] Pas de message depuis 120s, envoi PING...")
                        try:
                            await ws.send("PING")
                        except:
                            break

        except websockets.exceptions.ConnectionClosed as e:
            print(f"[WARN] Connexion fermée: {e}")
            retry_count += 1
            if retry_count < max_retries:
                wait_time = min(2 ** retry_count, 30)
                print(f"[INFO] Reconnexion dans {wait_time}s... (tentative {retry_count}/{max_retries})")
                await asyncio.sleep(wait_time)
        except Exception as e:
            print(f"[ERROR] Erreur: {e}")
            retry_count += 1
            if retry_count < max_retries:
                await asyncio.sleep(5)

    save_data()


def signal_handler(sig, frame):
    """Gestion de l'arrêt propre (Ctrl+C)"""
    global running
    print("\n[INFO] Arrêt demandé, sauvegarde en cours...")
    running = False


def main():
    print("=" * 60)
    print("  POLYMARKET WEBSOCKET CLIENT")
    print("  Récupération en temps réel: prix, trades, marchés")
    print("=" * 60)
    print(f"[INFO] Les données seront sauvegardées dans: {OUTPUT_FILE}")
    print("[INFO] Appuyez sur Ctrl+C pour arrêter\n")

    # Gestion du signal d'arrêt
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Lancement de la boucle asyncio
    try:
        asyncio.run(connect_and_listen())
    except KeyboardInterrupt:
        pass
    finally:
        save_data()
        print("[INFO] Terminé.")


if __name__ == "__main__":
    main()
