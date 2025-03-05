import os
from dotenv import load_dotenv
from loguru import logger

# Déterminer l'environnement (local par défaut)
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")
ENV_PATH = f"src/environment/{'local' if ENVIRONMENT == 'local' else 'prod'}.env"

# Charger les variables d'environnement
load_dotenv(ENV_PATH)
logger.info(f"📋 Chargement des variables d'environnement depuis {ENV_PATH}")

# Variables MongoDB
MONGO_URI_SOURCE = os.getenv("MONGO_URI_SOURCE")
MONGO_URI_DEST = os.getenv("MONGO_URI_DEST")

# Variables Backblaze B2 (remplacées par AWS_S3_* dans votre fichier original)
B2_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
B2_ENDPOINT = os.getenv("AWS_S3_ENDPOINT")
B2_ACCESS_KEY = os.getenv("AWS_S3_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("AWS_S3_SECRET_KEY")

# Validation des variables critiques
required_vars = {
    "MONGO_URI_SOURCE": MONGO_URI_SOURCE,
    "MONGO_URI_DEST": MONGO_URI_DEST,
    "B2_BUCKET_NAME": B2_BUCKET_NAME,
    "B2_ACCESS_KEY": B2_ACCESS_KEY,
    "B2_SECRET_KEY": B2_SECRET_KEY,
}

for var_name, var_value in required_vars.items():
    if not var_value:
        logger.critical(f"❌ La variable d'environnement {var_name} n'est pas définie dans {ENV_PATH}")
        raise ValueError(f"Variable {var_name} manquante")

# Variables optionnelles (non critiques pour l'upload d'images, mais utiles pour d'autres fonctionnalités)
IP_ROYAL_PROXY_HOST = os.getenv("IP_ROYAL_PROXY_HOST")
IP_ROYAL_PROXY_PORT = os.getenv("IP_ROYAL_PROXY_PORT")
IP_ROYAL_PROXY_USER = os.getenv("IP_ROYAL_PROXY_USER")
IP_ROYAL_PROXY_PASS_BASE = os.getenv("IP_ROYAL_PROXY_PASS_BASE")
TWO_CAPTCHA_API_KEY = os.getenv("TWO_CAPTCHA_API_KEY")
CAPSOLVER_API_KEY = os.getenv("CAPSOLVER_API_KEY")

# Logs pour confirmer les variables chargées (valeurs masquées pour la sécurité)
logger.info("✅ Variables d'environnement chargées avec succès")
logger.debug(f"Environment: {ENVIRONMENT}")
logger.debug(f"MongoDB Source: {MONGO_URI_SOURCE}")
logger.debug(f"MongoDB Dest: {MONGO_URI_DEST}")
logger.debug(f"B2 Bucket: {B2_BUCKET_NAME}")