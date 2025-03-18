import os
from dotenv import load_dotenv
from loguru import logger

ENVIRONMENT = os.getenv("ENVIRONMENT", "local")
ENV_PATH = f"src/environment/{'local' if ENVIRONMENT == 'local' else 'prod'}.env"

load_dotenv(ENV_PATH)
logger.info(f"📋 Chargement des variables d'environnement depuis {ENV_PATH}")

MONGO_URI = os.getenv("MONGO_URI")
B2_BUCKET_NAME = os.getenv("AWS_S3_BUCKET_NAME")
B2_ENDPOINT = os.getenv("AWS_S3_ENDPOINT")
B2_ACCESS_KEY = os.getenv("AWS_S3_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("AWS_S3_SECRET_KEY")

required_vars = {
    "MONGO_URI": MONGO_URI,
    "B2_BUCKET_NAME": B2_BUCKET_NAME,
    "B2_ACCESS_KEY": B2_ACCESS_KEY,
    "B2_SECRET_KEY": B2_SECRET_KEY,
}

for var_name, var_value in required_vars.items():
    if not var_value:
        logger.critical(f"❌ La variable {var_name} n'est pas définie dans {ENV_PATH}")
        raise ValueError(f"Variable {var_name} manquante")

logger.info("✅ Variables d'environnement chargées avec succès")