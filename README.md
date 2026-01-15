# Polymarket Pipeline

Pipeline de données pour l'ingestion, le traitement et le stockage de données provenant de **Polymarket** (marchés prédictifs) et **Truth Social** (posts).

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│   POLYMARKET WEBSOCKET              TRUTH SOCIAL API            │
│   (Événements temps réel)           (Posts d'un compte)         │
└───────────────┬─────────────────────────────┬───────────────────┘
                │                             │
                ▼                             ▼
        ┌─────────────────────────────────────────────┐
        │  ÉTAPE 1 & 2: INGESTION                     │
        │  Sauvegarde RAW en JSONL (partitionné)      │
        └───────────────┬─────────────────────────────┘
                        │
                        ▼
        ┌─────────────────────────────────────────────┐
        │  ÉTAPE 3 & 4: PROCESSING                    │
        │  Conversion JSONL → Parquet                 │
        └───────────────┬─────────────────────────────┘
                        │
                        ▼
        ┌─────────────────────────────────────────────┐
        │  ÉTAPE 5: CHARGEMENT                        │
        │  Insert batch dans MongoDB                  │
        └───────────────┬─────────────────────────────┘
                        │
                        ▼
                ┌───────────────┐
                │   MongoDB     │
                │ polymarket_db │
                └───────────────┘
```

## Prérequis

- Docker & Docker Compose
- Make

## Installation

1. Cloner le repository :
```bash
git clone <repo-url>
cd polymarket-ipssi
```

2. Configurer les variables d'environnement :
```bash
cp .env.example .env
# Éditer .env avec vos credentials Truth Social
```

3. Build et lancement :
```bash
make build && make run
```

## Configuration

Fichier `.env` :

| Variable | Description | Défaut |
|----------|-------------|--------|
| `TRUTHSOCIAL_USERNAME` | Nom d'utilisateur Truth Social | - |
| `TRUTHSOCIAL_PASSWORD` | Mot de passe Truth Social | - |
| `TRUTHSOCIAL_TOKEN` | Token OAuth (optionnel) | - |
| `INGESTION_DURATION_SECONDS` | Durée d'ingestion Polymarket | `60` |

## Commandes Make

### Gestion des containers

| Commande | Description |
|----------|-------------|
| `make build` | Build l'image Docker |
| `make up` | Démarre les services (MongoDB + Pipeline) |
| `make down` | Arrête les services |
| `make restart` | Redémarre les services |
| `make status` | Affiche le statut des containers |

### Exécution de la pipeline

| Commande | Description |
|----------|-------------|
| `make run` | Lance la pipeline complète (5 étapes) |
| `make ingestion` | Lance uniquement l'ingestion (étapes 1-2) |
| `make processing` | Lance uniquement le processing (étapes 3-4) |
| `make load` | Lance uniquement le chargement MongoDB (étape 5) |

### Debug et monitoring

| Commande | Description |
|----------|-------------|
| `make logs` | Affiche les logs de la pipeline |
| `make logs-mongo` | Affiche les logs de MongoDB |
| `make shell` | Ouvre un shell dans le container pipeline |
| `make mongo-shell` | Ouvre un shell MongoDB |

### Maintenance

| Commande | Description |
|----------|-------------|
| `make clean` | Supprime containers et volumes Docker |
| `make clean-data` | Supprime les données locales (raw + parquet) |
| `make prune` | Nettoie les ressources Docker inutilisées |

## Connexion MongoDB

### Avec MongoDB Compass

URI : `mongodb://localhost:27017`

> **Note :** Assurez-vous qu'aucun MongoDB local ne tourne sur le port 27017 :
> ```bash
> sudo systemctl stop mongod
> ```

### Collections disponibles

| Collection | Description |
|------------|-------------|
| `price_change` | Changements de prix Polymarket |
| `trade` | Trades exécutés |
| `new_market` | Nouveaux marchés créés |
| `market_resolved` | Marchés résolus |
| `tick_change` | Changements de tick size |
| `truthsocial_posts` | Posts Truth Social |

## Structure des données

```
data/
├── raw/                          # Données brutes JSONL
│   ├── polymarket/
│   │   └── date=YYYY-MM-DD/
│   │       └── hour=HH/
│   │           └── *.jsonl
│   └── truthsocial/
│       └── date=YYYY-MM-DD/
│           └── hour=HH/
│               └── *.jsonl
│
└── parquet/                      # Données transformées
    ├── polymarket/
    │   ├── price_change/
    │   ├── trade/
    │   ├── new_market/
    │   ├── market_resolved/
    │   └── tick_change/
    └── truthsocial/
        └── posts/
```

## Workflow typique

```bash
# Premier lancement
make build && make run

# Relancer la pipeline (données fraîches)
make run

# Repartir de zéro
make clean && make clean-data && make build && make run

# Debug
make logs          # Voir les logs
make shell         # Entrer dans le container
make mongo-shell   # Explorer la DB
```

## Stack technique

- **Python 3.12** avec UV (gestionnaire de paquets)
- **MongoDB 7** (base de données)
- **Docker & Docker Compose** (containerisation)
- **Pandas / PyArrow** (traitement de données)
- **WebSockets** (ingestion temps réel Polymarket)
