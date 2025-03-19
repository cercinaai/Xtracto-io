import asyncio
from loguru import logger
from src.scrapers.leboncoin.image_processor import process_and_transfer_images
from src.database.database import init_db, close_db
import os
import sys

# Configuration des logs
if not os.path.exists("logs/leboncoin"):
    os.makedirs("logs/leboncoin")
if not os.path.exists("logs/capture/leboncoin"):
    os.makedirs("logs/capture/leboncoin")

logger.remove()  # Supprime la configuration par défaut
logger.add(sys.stdout, level="DEBUG")  # Affiche tout dans la console
logger.add(
    "logs/leboncoin/cron_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="7 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

async def process_images_job():
    """Tâche pour traiter les images des annonces de la collection realStateWithAgence."""
    try:
        logger.info("📸 Vérification des annonces dans realStateWithAgence...")
        await init_db()  # Initialiser la connexion à la base de données
        result = await process_and_transfer_images(max_concurrent_tasks=20)
        if result["processed"] > 0:
            logger.info(f"✅ Traitement terminé : {result['processed']} annonces traitées et transférées.")
        else:
            logger.info("ℹ️ Aucune nouvelle annonce à traiter.")
    except Exception as e:
        logger.error(f"⚠️ Erreur lors du traitement des images : {e}")
    finally:
        await close_db()  # Fermer la connexion à la base de données

async def start_cron():
    """Démarre une boucle continue pour vérifier et traiter les nouvelles annonces."""
    logger.info("⏰ Processus de traitement des images démarré en mode continu.")
    while True:
        await process_images_job()
        # Attendre 30 secondes avant la prochaine vérification
        await asyncio.sleep(30)

if __name__ == "__main__":
    # Lancer le processus
    asyncio.run(start_cron())