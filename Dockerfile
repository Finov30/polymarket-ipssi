# Dockerfile pour la pipeline Polymarket
# Utilise UV pour la gestion des dépendances Python

FROM python:3.12-slim

# Installer les dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Installer UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers de dépendances
COPY pyproject.toml uv.lock ./

# Installer les dépendances (sans le projet lui-même)
RUN uv sync --frozen --no-install-project

# Copier le code source
COPY src/ ./src/
COPY main.py ./

# Installer le projet
RUN uv sync --frozen

# Créer le répertoire data (sera monté en volume)
RUN mkdir -p /app/data/raw/polymarket \
    /app/data/raw/truthsocial \
    /app/data/parquet/polymarket \
    /app/data/parquet/truthsocial

# Variables d'environnement par défaut
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Point d'entrée
ENTRYPOINT ["uv", "run", "python"]
CMD ["main.py"]
