# Makefile pour le projet Polymarket Pipeline
# ============================================

.PHONY: help build up down run logs clean shell mongo-shell status restart ingestion processing load

# Couleurs pour les messages
BLUE := \033[34m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
NC := \033[0m

# Configuration
COMPOSE := docker compose
PIPELINE_CONTAINER := polymarket-pipeline
MONGO_CONTAINER := polymarket-mongodb

# Aide par défaut
help:
	@echo "$(BLUE)═══════════════════════════════════════════════════════════$(NC)"
	@echo "$(BLUE)        Polymarket Pipeline - Commandes Disponibles        $(NC)"
	@echo "$(BLUE)═══════════════════════════════════════════════════════════$(NC)"
	@echo ""
	@echo "$(GREEN)Gestion des containers:$(NC)"
	@echo "  make build        - Build l'image Docker de la pipeline"
	@echo "  make up           - Démarre tous les services (MongoDB + Pipeline)"
	@echo "  make down         - Arrête tous les services"
	@echo "  make restart      - Redémarre tous les services"
	@echo "  make status       - Affiche le statut des containers"
	@echo ""
	@echo "$(GREEN)Exécution de la pipeline:$(NC)"
	@echo "  make run          - Lance la pipeline complète (5 étapes)"
	@echo "  make ingestion    - Lance uniquement l'ingestion (étapes 1-2)"
	@echo "  make processing   - Lance uniquement le processing (étapes 3-4)"
	@echo "  make load         - Lance uniquement le chargement MongoDB (étape 5)"
	@echo ""
	@echo "$(GREEN)Debug et monitoring:$(NC)"
	@echo "  make logs         - Affiche les logs de la pipeline"
	@echo "  make logs-mongo   - Affiche les logs de MongoDB"
	@echo "  make shell        - Ouvre un shell dans le container pipeline"
	@echo "  make mongo-shell  - Ouvre un shell MongoDB"
	@echo ""
	@echo "$(GREEN)Maintenance:$(NC)"
	@echo "  make clean        - Supprime containers et volumes"
	@echo "  make clean-data   - Supprime les données locales (raw + parquet)"
	@echo "  make prune        - Nettoie les ressources Docker inutilisées"
	@echo ""

# ============================================
# Gestion des containers
# ============================================

build:
	@echo "$(BLUE)[BUILD]$(NC) Construction de l'image Docker..."
	$(COMPOSE) build --no-cache

up:
	@echo "$(BLUE)[UP]$(NC) Démarrage des services..."
	$(COMPOSE) up -d
	@echo "$(GREEN)[OK]$(NC) Services démarrés"
	@$(MAKE) status

down:
	@echo "$(BLUE)[DOWN]$(NC) Arrêt des services..."
	$(COMPOSE) down
	@echo "$(GREEN)[OK]$(NC) Services arrêtés"

restart: down up

status:
	@echo "$(BLUE)[STATUS]$(NC) État des containers:"
	@$(COMPOSE) ps

# ============================================
# Exécution de la pipeline
# ============================================

run:
	@echo "$(BLUE)[RUN]$(NC) Lancement de la pipeline complète..."
	$(COMPOSE) run --rm pipeline main.py

ingestion:
	@echo "$(BLUE)[INGESTION]$(NC) Lancement de l'ingestion..."
	@echo "$(YELLOW)→ Étape 1: Polymarket WebSocket$(NC)"
	$(COMPOSE) run --rm pipeline src/ingestion/polymarket_ws.py
	@echo "$(YELLOW)→ Étape 2: Truth Social API$(NC)"
	$(COMPOSE) run --rm pipeline src/ingestion/truthsocial_api.py
	@echo "$(GREEN)[OK]$(NC) Ingestion terminée"

processing:
	@echo "$(BLUE)[PROCESSING]$(NC) Lancement du processing..."
	@echo "$(YELLOW)→ Étape 3: Polymarket RAW → Parquet$(NC)"
	$(COMPOSE) run --rm pipeline src/processing/raw_to_parquet_pm.py
	@echo "$(YELLOW)→ Étape 4: TruthSocial RAW → Parquet$(NC)"
	$(COMPOSE) run --rm pipeline src/processing/raw_to_parquet_ts.py
	@echo "$(GREEN)[OK]$(NC) Processing terminé"

load:
	@echo "$(BLUE)[LOAD]$(NC) Chargement dans MongoDB..."
	$(COMPOSE) run --rm pipeline src/loaders/mongo_loader.py
	@echo "$(GREEN)[OK]$(NC) Chargement terminé"

# ============================================
# Debug et monitoring
# ============================================

logs:
	$(COMPOSE) logs -f pipeline

logs-mongo:
	$(COMPOSE) logs -f mongodb

shell:
	@echo "$(BLUE)[SHELL]$(NC) Ouverture du shell dans le container pipeline..."
	$(COMPOSE) run --rm --entrypoint /bin/bash pipeline

mongo-shell:
	@echo "$(BLUE)[MONGO]$(NC) Ouverture du shell MongoDB..."
	docker exec -it $(MONGO_CONTAINER) mongosh polymarket_db

# ============================================
# Maintenance
# ============================================

clean:
	@echo "$(RED)[CLEAN]$(NC) Suppression des containers et volumes..."
	$(COMPOSE) down -v --remove-orphans
	docker image rm -f polymarket-ipssi-pipeline 2>/dev/null || true
	@echo "$(GREEN)[OK]$(NC) Nettoyage terminé"

clean-data:
	@echo "$(RED)[CLEAN-DATA]$(NC) Suppression des données locales..."
	rm -rf data/raw/*
	rm -rf data/parquet/*
	@echo "$(GREEN)[OK]$(NC) Données supprimées"

prune:
	@echo "$(RED)[PRUNE]$(NC) Nettoyage des ressources Docker inutilisées..."
	docker system prune -f
	@echo "$(GREEN)[OK]$(NC) Nettoyage terminé"
