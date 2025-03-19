import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
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

# Variable pour suivre l'état de la tâche
running_task = False

async def process_images_job():
    """Tâche planifiée pour traiter les images des annonces de la collection realStateWithAgence."""
    global running_task
    if running_task:
        logger.info("⏳ Tâche de traitement des images déjà en cours, saut de cette exécution.")
        return

    running_task = True
    try:
        logger.info("📸 Début du traitement des images des annonces (realStateWithAgence -> realStateFinale)...")
        await init_db()  # Initialiser la connexion à la base de données
        result = await process_and_transfer_images(max_concurrent_tasks=20)
        logger.info(f"✅ Traitement terminé : {result['processed']} annonces traitées et transférées.")
    except Exception as e:
        logger.error(f"⚠️ Erreur lors du traitement des images : {e}")
    finally:
        await close_db()  # Fermer la connexion à la base de données
        running_task = False

async def start_cron():
    """Démarre le planificateur pour exécuter la tâche de traitement des images périodiquement."""
    scheduler = AsyncIOScheduler()
    # Planifier la tâche toutes les 10 minutes
    scheduler.add_job(process_images_job, "interval", minutes=10)
    scheduler.start()
    logger.info("⏰ Planificateur démarré : traitement des images toutes les 10 minutes.")

    # Garder le script en vie
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("🛑 Planificateur arrêté.")

if __name__ == "__main__":
    # Lancer le planificateur
    asyncio.run(start_cron())