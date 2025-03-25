from motor.motor_asyncio import AsyncIOMotorClient
from src.config.settings import MONGO_URI
from loguru import logger

source_client = None
destination_client = None
source_db = None
destination_db = None


async def init_db():
    global source_client, destination_client, source_db, destination_db
    try:
        source_client = AsyncIOMotorClient(MONGO_URI)
        source_db = source_client["xtracto-io-prod"]
        destination_client = AsyncIOMotorClient(MONGO_URI)
        destination_db = destination_client["xtracto-io-prod"]
        logger.success("✅ Connexion aux bases MongoDB établie")
    except Exception as e:
        logger.critical(f"🚨 Erreur de connexion MongoDB : {e}")
        raise SystemExit(1)

async def close_db():
    global source_client, destination_client
    if source_client:
        source_client.close()
    if destination_client:
        destination_client.close()
    logger.info("🔌 Connexions MongoDB fermées")

def get_source_db():
    if source_db is None:
        raise RuntimeError("Base source non initialisée")
    return source_db

def get_destination_db():
    if destination_db is None:
        raise RuntimeError("Base destination non initialisée")
    return destination_db