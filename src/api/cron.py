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

# Gestion des tâches avec leur état et navigateur
class TaskState:
    def __init__(self):
        self.running = False
        self.browser = None  # Référence au navigateur si utilisé
        self.task = None     # Référence à la tâche asyncio

running_tasks = {
    "first_scraper": TaskState(),
    "loop_scraper": TaskState(),
    "agence_brute": TaskState(),
    "agence_notexisting": TaskState(),
    "process_and_transfer": TaskState()
}

async def first_scraper_task():
    state = running_tasks["first_scraper"]
    logger.info("🚀 Lancement de first_scraper_task...")
    while True:
        try:
            current_hour = datetime.now().hour
            if 10 <= current_hour < 22:  # Entre 10h00 et 22h00
                if not state.running:
                    logger.info("▶️ Exécution de open_leboncoin...")
                    state.running = True
                    queue = asyncio.Queue()
                    await open_leboncoin(queue)  # Supposons que cette fonction gère son propre navigateur
                    result = await queue.get()
                    logger.info(f"📥 Résultat de first_scraper_task: {result}")
                    state.running = False
            else:
                if state.running:
                    logger.info("⏹️ Arrêt de first_scraper_task car hors plage horaire (22h-10h).")
                    state.running = False
                    # Ici, il faut fermer le navigateur si open_leboncoin ne le fait pas
                logger.debug("⏰ Hors plage horaire pour first_scraper_task (10h-22h).")
            await asyncio.sleep(60)  # Vérifie toutes les minutes pour une transition rapide
        except Exception as e:
            logger.error(f"⚠️ Erreur dans first_scraper_task: {e}")
            state.running = False
            await asyncio.sleep(60)

async def loop_scraper_task():
    state = running_tasks["loop_scraper"]
    logger.info("⏳ Début de l'attente initiale de 5 minutes pour loop_scraper_task...")
    await asyncio.sleep(5 * 60)  # Attendre 5 minutes au démarrage
    logger.info("✅ Fin de l'attente initiale, boucle de loop_scraper_task démarrée.")
    while True:
        try:
            current_hour = datetime.now().hour
            if 10 <= current_hour < 22:  # Entre 10h05 et 22h00
                if not state.running:
                    logger.info("🔄 Lancement de open_leboncoin_loop...")
                    state.running = True
                    queue = asyncio.Queue()
                    await open_leboncoin_loop(queue)
                    result = await queue.get()
                    logger.info(f"📥 Résultat de loop_scraper_task: {result}")
                    state.running = False
            else:
                if state.running:
                    logger.info("⏹️ Arrêt de loop_scraper_task car hors plage horaire (22h-10h).")
                    state.running = False
                    # Ici, il faut fermer le navigateur si open_leboncoin_loop ne le fait pas
                logger.debug("⏰ Hors plage horaire pour loop_scraper_task (10h-22h).")
            await asyncio.sleep(60)  # Vérifie toutes les minutes
        except Exception as e:
            logger.error(f"⚠️ Erreur dans loop_scraper_task: {e}")
            state.running = False
            await asyncio.sleep(60)

async def agence_brute_task():
    state = running_tasks["agence_brute"]
    logger.info("⏳ Début de la tâche agence_brute_task...")
    while True:
        try:
            current_hour = datetime.now().hour
            if current_hour < 10 or current_hour >= 22:  # Entre 22h00 et 10h00
                if not state.running:
                    logger.info("🔧 Lancement de scrape_agences...")
                    state.running = True
                    queue = asyncio.Queue()
                    await scrape_agences(queue)
                    result = await queue.get()
                    logger.info(f"📥 Résultat de agence_brute_task: {result}")
                    state.running = False
            else:
                if state.running:
                    logger.info("⏹️ Arrêt de agence_brute_task car hors plage horaire (10h-22h).")
                    state.running = False
                logger.debug("⏰ Hors plage horaire pour agence_brute_task (22h-10h).")
            await asyncio.sleep(60)  # Vérifie toutes les minutes
        except Exception as e:
            logger.error(f"⚠️ Erreur dans agence_brute_task: {e}")
            state.running = False
            await asyncio.sleep(60)

async def agence_notexisting_task():
    state = running_tasks["agence_notexisting"]
    logger.info("⏳ Début de la tâche agence_notexisting_task...")
    while True:
        try:
            current_hour = datetime.now().hour
            if current_hour < 10 or current_hour >= 22:  # Entre 22h00 et 10h00
                if not state.running:
                    logger.info("🕵️ Lancement de scrape_annonce_agences...")
                    state.running = True
                    queue = asyncio.Queue()
                    await scrape_annonce_agences(queue)
                    result = await queue.get()
                    logger.info(f"📥 Résultat de agence_notexisting_task: {result}")
                    state.running = False
            else:
                if state.running:
                    logger.info("⏹️ Arrêt de agence_notexisting_task car hors plage horaire (10h-22h).")
                    state.running = False
                logger.debug("⏰ Hors plage horaire pour agence_notexisting_task (22h-10h).")
            await asyncio.sleep(60)  # Vérifie toutes les minutes
        except Exception as e:
            logger.error(f"⚠️ Erreur dans agence_notexisting_task: {e}")
            state.running = False
            await asyncio.sleep(60)

async def process_images_task():
    state = running_tasks["process_and_transfer"]
    logger.info("📸 Lancement continu du traitement des images...")
    state.running = True
    await process_and_transfer_images()
    state.running = False

async def start_cron():
    logger.info("🚀 Démarrage des tâches cron...")
    await init_db()
    running_tasks["first_scraper"].task = asyncio.create_task(first_scraper_task())
    running_tasks["loop_scraper"].task = asyncio.create_task(loop_scraper_task())
    running_tasks["agence_brute"].task = asyncio.create_task(agence_brute_task())
    running_tasks["agence_notexisting"].task = asyncio.create_task(agence_notexisting_task())
    running_tasks["process_and_transfer"].task = asyncio.create_task(process_images_task())

if __name__ == "__main__":
    asyncio.run(start_cron())