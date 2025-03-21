import asyncio
from datetime import datetime
import logging
from src.database.database import init_db
from src.scrapers.leboncoin.firstScrapper import open_leboncoin
from src.scrapers.leboncoin.leboncoinLoopScrapper import open_leboncoin_loop
from src.scrapers.leboncoin.agenceBrute_scraper import scrape_agences
from src.scrapers.leboncoin.agence_notexisting import scrape_annonce_agences
from src.scrapers.leboncoin.image_processor import process_and_transfer_images

logger = logging.getLogger(__name__)

running_tasks = {
    "first_scraper": False,
    "loop_scraper": False,
    "agence_brute": False,
    "agence_notexisting": False,
    "process_and_transfer": False
}

async def first_scraper_task():
    logger.info("🚀 Lancement de first_scraper_task...")
    while True:
        try:
            current_hour = datetime.now().hour
            if 10 <= current_hour < 22:  # Entre 10h00 et 22h00
                logger.info("▶️ Exécution de open_leboncoin...")
                queue = asyncio.Queue()
                await open_leboncoin(queue)
                result = await queue.get()
                logger.info(f"📥 Résultat de first_scraper_task: {result}")
            else:
                logger.debug("⏰ Hors plage horaire pour first_scraper_task (10h-22h).")
            await asyncio.sleep(60 * 60)  # Vérifie toutes les heures
        except Exception as e:
            logger.error(f"⚠️ Erreur dans first_scraper_task: {e}")
            await asyncio.sleep(60 * 60)

async def loop_scraper_task():
    logger.info("⏳ Début de l'attente initiale de 5 minutes pour loop_scraper_task...")
    await asyncio.sleep(5 * 60)  # Attendre 5 minutes au démarrage
    logger.info("✅ Fin de l'attente initiale, boucle de loop_scraper_task démarrée.")
    while True:
        try:
            current_hour = datetime.now().hour
            if 10 <= current_hour < 22:  # Entre 10h05 et 22h00
                logger.info("🔄 Lancement de open_leboncoin_loop...")
                queue = asyncio.Queue()
                await open_leboncoin_loop(queue)
                result = await queue.get()
                logger.info(f"📥 Résultat de loop_scraper_task: {result}")
            else:
                logger.debug("⏰ Hors plage horaire pour loop_scraper_task (10h-22h).")
            await asyncio.sleep(5 * 60)  # Attendre 5 minutes entre chaque tentative
        except Exception as e:
            logger.error(f"⚠️ Erreur dans loop_scraper_task: {e}")
            await asyncio.sleep(5 * 60)

async def agence_brute_task():
    logger.info("⏳ Début de la tâche agence_brute_task...")
    while True:
        try:
            current_hour = datetime.now().hour
            if current_hour < 10 or current_hour >= 22:  # Entre 22h00 et 10h00
                logger.info("🔧 Lancement de scrape_agences...")
                queue = asyncio.Queue()
                await scrape_agences(queue)
                result = await queue.get()
                logger.info(f"📥 Résultat de agence_brute_task: {result}")
            else:
                logger.debug("⏰ Hors plage horaire pour agence_brute_task (22h-10h).")
            await asyncio.sleep(60 * 60)  # Vérifie toutes les heures
        except Exception as e:
            logger.error(f"⚠️ Erreur dans agence_brute_task: {e}")
            await asyncio.sleep(60 * 60)

async def agence_notexisting_task():
    logger.info("⏳ Début de la tâche agence_notexisting_task...")
    while True:
        try:
            current_hour = datetime.now().hour
            if current_hour < 10 or current_hour >= 22:  # Entre 22h00 et 10h00
                logger.info("🕵️ Lancement de scrape_annonce_agences...")
                queue = asyncio.Queue()
                await scrape_annonce_agences(queue)
                result = await queue.get()
                logger.info(f"📥 Résultat de agence_notexisting_task: {result}")
            else:
                logger.debug("⏰ Hors plage horaire pour agence_notexisting_task (22h-10h).")
            await asyncio.sleep(60 * 60)  # Vérifie toutes les heures
        except Exception as e:
            logger.error(f"⚠️ Erreur dans agence_notexisting_task: {e}")
            await asyncio.sleep(60 * 60)

async def process_images_task():
    logger.info("📸 Lancement continu du traitement des images...")
    await process_and_transfer_images()

async def start_cron():
    logger.info("🚀 Démarrage des tâches cron...")
    await init_db()
    asyncio.create_task(first_scraper_task())
    asyncio.create_task(loop_scraper_task())
    asyncio.create_task(agence_brute_task())
    asyncio.create_task(agence_notexisting_task())
    asyncio.create_task(process_images_task())

if __name__ == "__main__":
    asyncio.run(start_cron())