# MSPR Energy

Application de prévision et de monitoring de la consommation électrique française.

Le projet combine :

- un pipeline de données RTE éco2mix et SQR Météo France ;
- un modèle SARIMAX pour la prédiction journalière ;
- une API FastAPI pour l'inférence ;
- une interface Streamlit de monitoring ;
- des tests automatisés et un déploiement Docker.

## Architecture

```text
Données RTE/SQR
  -> Bronze
  -> Silver
  -> Gold
  -> Entraînement SARIMAX
  -> Artefacts modèle dans src/models
  -> API FastAPI + Frontend Streamlit
```

Services principaux :

- API FastAPI : `src/api/main.py`
- Frontend Streamlit : `src/frontend/Accueil.py`
- Scripts data/model : `src/scripts/`
- Tests : `tests/`

## Installation locale

Créer l'environnement Python puis installer les dépendances :

```bash
pip install -r requirements.txt
```

Créer le fichier d'environnement :

```bash
cp .env.template .env
```

Adapter ensuite `PATH_DATA` dans `.env` si nécessaire.

## Lancer l'application

Avec Docker Compose :

```bash
docker-compose up -d --build
```

Accès local :

- API : `http://localhost:8000/docs`
- Streamlit : `http://localhost:8501`

Arrêter les services :

```bash
docker-compose down
```

## Lancer sans Docker

API :

```bash
uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

Streamlit :

```bash
streamlit run src/frontend/Accueil.py
```

## Tests

Installer Chromium pour Playwright :

```bash
python -m playwright install chromium
```

Lancer tous les tests :

```bash
pytest
```

Lancer les vérifications locales complètes :

```bash
./scripts/dev_check.sh
```

Le script exécute :

- le lint avec Ruff ;
- les tests unitaires ;
- les tests d'intégration ;
- le test E2E Streamlit avec Playwright.

Pour observer le test E2E dans un navigateur visible :

```bash
PLAYWRIGHT_HEADLESS=0 PLAYWRIGHT_SLOW_MO=500 pytest -s tests/e2e/test_e2e.py
```

## CI/CD

La CI GitHub Actions exécute automatiquement :

- installation des dépendances ;
- lint Ruff ;
- tests unitaires, intégration et E2E ;
- build Docker.

Le déploiement production est manuel via GitHub Actions.

La production utilise :

- VPS OVH Ubuntu 24.04 ;
- Docker Compose ;
- Caddy pour HTTPS ;
- Streamlit : `https://app.mspr-energy.fr`
- API : `https://api.mspr-energy.fr`

## Commandes utiles

Health check API :

```bash
curl http://localhost:8000/health
```

Voir les conteneurs :

```bash
docker-compose ps
```

Voir les logs :

```bash
docker-compose logs -f api
docker-compose logs -f frontend
```
