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
    "metadata": {
        "started_at": None,
        "last_updated": None,
        "total_events": 0,
        "markets_tracked": []
    },
    "orderbooks": {},      # asset_id -> orderbook data
    "price_changes": [],   # Liste des changements de prix
    "trades": [],          # Liste des trades (last_trade_price)
    "new_markets": [],     # Nouveaux marchés créés
    "market_resolved": [], # Marchés résolus
    "tick_changes": [],    # Changements de tick size
    "best_bid_ask": []     # Meilleurs bid/ask
}

# Flag pour arrêt propre
running = True


def save_data():
    """Sauvegarde les données dans le fichier JSON"""
    data_store["metadata"]["last_updated"] = datetime.now().isoformat()
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data_store, f, indent=2, ensure_ascii=False)
    print(f"[SAVE] Données sauvegardées ({data_store['metadata']['total_events']} événements)")


def fetch_active_markets():
    """Récupère les marchés actifs depuis l'API Gamma"""
    print("[INFO] Récupération des marchés actifs...")
    url = f"{GAMMA_API}?closed=false&active=true&limit={MAX_MARKETS}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as response:
            markets = json.loads(response.read().decode())

        token_ids = []
        market_info = []

        for m in markets:
            if m.get("enableOrderBook") and m.get("clobTokenIds"):
                clob_ids = json.loads(m["clobTokenIds"])
                token_ids.extend(clob_ids)
                market_info.append({
                    "question": m.get("question", ""),
                    "slug": m.get("slug", ""),
                    "token_ids": clob_ids
                })

        print(f"[INFO] {len(market_info)} marchés trouvés, {len(token_ids)} tokens")
        data_store["metadata"]["markets_tracked"] = market_info
        return token_ids

    except Exception as e:
        print(f"[ERROR] Impossible de récupérer les marchés: {e}")
        return []


def handle_message(message: dict):
    """Traite les messages reçus selon leur type"""
    event_type = message.get("event_type", "unknown")
    timestamp = datetime.now().isoformat()

    data_store["metadata"]["total_events"] += 1

    if event_type == "book":
        # Orderbook complet
        asset_id = message.get("asset_id", "unknown")
        data_store["orderbooks"][asset_id] = {
            "market": message.get("market"),
            "bids": message.get("bids", []),
            "asks": message.get("asks", []),
            "timestamp": message.get("timestamp"),
            "received_at": timestamp
        }
        bids_count = len(message.get("bids", []))
        asks_count = len(message.get("asks", []))
        print(f"[BOOK] Asset: {asset_id[:20]}... | Bids: {bids_count} | Asks: {asks_count}")

    elif event_type == "price_change":
        # Changement de prix
        price_changes = message.get("price_changes", [])
        for pc in price_changes:
            entry = {
                "market": message.get("market"),
                "asset_id": pc.get("asset_id"),
                "price": pc.get("price"),
                "size": pc.get("size"),
                "side": pc.get("side"),
                "best_bid": pc.get("best_bid"),
                "best_ask": pc.get("best_ask"),
                "timestamp": message.get("timestamp"),
                "received_at": timestamp
            }
            data_store["price_changes"].append(entry)
            print(f"[PRICE] {pc.get('side', 'N/A'):4} | Price: {str(pc.get('price', 'N/A')):8} | Size: {pc.get('size', 'N/A')}")

    elif event_type == "last_trade_price":
        # Trade exécuté
        entry = {
            "market": message.get("market"),
            "asset_id": message.get("asset_id"),
            "price": message.get("price"),
            "size": message.get("size"),
            "side": message.get("side"),
            "fee_rate_bps": message.get("fee_rate_bps"),
            "timestamp": message.get("timestamp"),
            "received_at": timestamp
        }
        data_store["trades"].append(entry)
        print(f"[TRADE] {message.get('side', 'N/A'):4} | Price: {str(message.get('price', 'N/A')):8} | Size: {message.get('size', 'N/A')}")

    elif event_type == "new_market":
        # Nouveau marché
        entry = {
            "id": message.get("id"),
            "question": message.get("question"),
            "market": message.get("market"),
            "slug": message.get("slug"),
            "description": message.get("description"),
            "assets_ids": message.get("assets_ids", []),
            "outcomes": message.get("outcomes", []),
            "timestamp": message.get("timestamp"),
            "received_at": timestamp
        }
        data_store["new_markets"].append(entry)
        print(f"[NEW MARKET] {message.get('question', 'N/A')[:50]}...")

    elif event_type == "market_resolved":
        # Marché résolu
        entry = {
            "id": message.get("id"),
            "question": message.get("question"),
            "market": message.get("market"),
            "winning_asset_id": message.get("winning_asset_id"),
            "winning_outcome": message.get("winning_outcome"),
            "timestamp": message.get("timestamp"),
            "received_at": timestamp
        }
        data_store["market_resolved"].append(entry)
        print(f"[RESOLVED] {message.get('question', 'N/A')[:40]}... -> {message.get('winning_outcome')}")

    elif event_type == "tick_size_change":
        # Changement de tick size
        entry = {
            "asset_id": message.get("asset_id"),
            "market": message.get("market"),
            "old_tick_size": message.get("old_tick_size"),
            "new_tick_size": message.get("new_tick_size"),
            "timestamp": message.get("timestamp"),
            "received_at": timestamp
        }
        data_store["tick_changes"].append(entry)
        print(f"[TICK] {message.get('old_tick_size')} -> {message.get('new_tick_size')}")

    elif event_type == "best_bid_ask":
        # Meilleur bid/ask
        entry = {
            "market": message.get("market"),
            "asset_id": message.get("asset_id"),
            "best_bid": message.get("best_bid"),
            "best_ask": message.get("best_ask"),
            "spread": message.get("spread"),
            "timestamp": message.get("timestamp"),
            "received_at": timestamp
        }
        data_store["best_bid_ask"].append(entry)
        print(f"[BID/ASK] Bid: {message.get('best_bid')} | Ask: {message.get('best_ask')} | Spread: {message.get('spread')}")

    else:
        # Message non reconnu - on l'affiche pour debug
        print(f"[MSG] Type: {event_type} | Keys: {list(message.keys())[:5]}")

    # Sauvegarde toutes les 20 événements
    if data_store["metadata"]["total_events"] % 20 == 0:
        save_data()


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
    data_store["metadata"]["started_at"] = datetime.now().isoformat()

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
    print("  Récupération en temps réel: orderbook, prix, trades, marchés")
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
