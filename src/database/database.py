import os
from motor.motor_asyncio import AsyncIOMotorClient
from loguru import logger

# Charger l'URI depuis les variables d'environnement
MONGO_URI = os.getenv("MONGO_URI")

# Initialisation des variables globales
mongo_client = None
database = None

async def init_db():
    """Initialisation de la connexion à MongoDB."""
    global mongo_client, database
    try:
        if not MONGO_URI:
            raise ValueError("❌ MONGO_URI n'est pas défini dans les variables d'environnement")
        
        # Connexion à MongoDB
        mongo_client = AsyncIOMotorClient(MONGO_URI)
        
        # Extraire le nom de la base depuis l'URI
        db_name = MONGO_URI.split("/")[-1].split("?")[0]
        database = mongo_client[db_name]

        logger.success(f"✅ Connexion à MongoDB établie avec la base '{db_name}'")
    except Exception as e:
        logger.critical(f"🚨 Erreur de connexion MongoDB : {e}")
        raise SystemExit(1)

async def close_db():
    """Fermeture propre de la connexion MongoDB."""
    global mongo_client
    if mongo_client:
        mongo_client.close()
        logger.info("🔌 Connexion MongoDB fermée")

def get_db():
    """Retourne l'instance de la base de données."""
    if database is None:
        raise RuntimeError("❌ La base de données n'est pas initialisée")
    return database
