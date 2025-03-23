import asyncio
from datetime import datetime
import random
import logging
from src.database.database import init_db
from src.scrapers.leboncoin.firstScrapper import open_leboncoin
from src.scrapers.leboncoin.leboncoinLoopScrapper import open_leboncoin_loop
from src.scrapers.leboncoin.agenceBrute_scraper import scrape_agences
from src.scrapers.leboncoin.agence_notexisting import scrape_annonce_agences
from src.scrapers.leboncoin.image_processor import process_and_transfer_images
from src.config.browserConfig import cleanup_browser

logger = logging.getLogger(__name__)

class TaskState:
    def __init__(self):
        self.running = False
        self.browser = None
        self.context = None
        self.client = None
        self.profile_id = None
        self.playwright = None
        self.task = None
        self.last_run = None

running_tasks = {
    "first_scraper": TaskState(),
    "loop_scraper": TaskState(),
    "agence_brute": TaskState(),
    "agence_notexisting": TaskState(),
    "process_and_transfer": TaskState()
}

async def cleanup_task(task_name):
    state = running_tasks[task_name]
    if state.task and not state.task.done():
        state.task.cancel()
        try:
            await state.task
        except asyncio.CancelledError:
            logger.info(f"🛑 Tâche {task_name} annulée avec succès.")
    if state.browser:
        await cleanup_browser(state.client, state.profile_id, state.playwright, state.browser)
        state.browser = state.context = state.client = state.profile_id = state.playwright = None
    state.running = False

async def first_scraper_task():
    state = running_tasks["first_scraper"]
    logger.info("🚀 Initialisation de first_scraper_task...")
    while True:
        try:
            current_hour = datetime.now().hour
            logger.info(f"⏰ Vérification horaire - Heure actuelle : {current_hour}h")
            
            if 10 <= current_hour < 22:
                if not state.running or (state.last_run and (datetime.now() - state.last_run).total_seconds() >= random.uniform(600, 900)):
                    logger.info("▶️ Lancement de first_scraper...")
                    state.running = True
                    queue = asyncio.Queue()
                    await open_leboncoin(queue)
                    result = await queue.get()
                    state.last_run = datetime.now()
                    logger.info(f"📥 Résultat de first_scraper: {result}")
                    if result["status"] == "error":
                        await cleanup_task("first_scraper")
            else:
                if state.running:
                    logger.info("⏹️ Arrêt de first_scraper (horaire nocturne)")
                    await cleanup_task("first_scraper")
            
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"⚠️ Erreur dans first_scraper_task: {e}")
            await cleanup_task("first_scraper")
            await asyncio.sleep(60)

async def loop_scraper_task():
    state = running_tasks["loop_scraper"]
    logger.info("⏳ Attente initiale de 5 minutes pour loop_scraper_task...")
    await asyncio.sleep(300)
    while True:
        try:
            current_hour = datetime.now().hour
            logger.info(f"⏰ Vérification horaire - Heure actuelle : {current_hour}h")
            
            if 10 <= current_hour < 22:
                if not state.running:
                    logger.info("▶️ Lancement de loop_scraper...")
                    state.running = True
                    queue = asyncio.Queue()
                    await open_leboncoin_loop(queue)
                    result = await queue.get()
                    logger.info(f"📥 Résultat de loop_scraper: {result}")
                    if result["status"] == "error":
                        await cleanup_task("loop_scraper")
            else:
                if state.running:
                    logger.info("⏹️ Arrêt de loop_scraper (horaire nocturne)")
                    await cleanup_task("loop_scraper")
            
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"⚠️ Erreur dans loop_scraper_task: {e}")
            await cleanup_task("loop_scraper")
            await asyncio.sleep(60)

async def agence_brute_task():
    state = running_tasks["agence_brute"]
    while True:
        try:
            current_hour = datetime.now().hour
            logger.info(f"⏰ Vérification horaire - Heure actuelle : {current_hour}h")
            
            if current_hour < 10 or current_hour >= 22:
                if not state.running:
                    logger.info("▶️ Lancement de agence_brute...")
                    state.running = True
                    queue = asyncio.Queue()
                    state.task = asyncio.create_task(scrape_agences(queue))
                    result = await queue.get()
                    logger.info(f"📥 Résultat de agence_brute: {result}")
                    if result["status"] == "error":
                        await cleanup_task("agence_brute")
                    state.task = None
            else:
                if state.running:
                    logger.info("⏹️ Arrêt de agence_brute (horaire diurne)")
                    await cleanup_task("agence_brute")
            
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"⚠️ Erreur dans agence_brute_task: {e}")
            await cleanup_task("agence_brute")
            await asyncio.sleep(60)

async def agence_notexisting_task():
    state = running_tasks["agence_notexisting"]
    while True:
        try:
            current_hour = datetime.now().hour
            logger.info(f"⏰ Vérification horaire - Heure actuelle : {current_hour}h")
            
            if current_hour < 10 or current_hour >= 22:
                if not state.running:
                    logger.info("▶️ Lancement de agence_notexisting...")
                    state.running = True
                    queue = asyncio.Queue()
                    state.task = asyncio.create_task(scrape_annonce_agences(queue))
                    result = await queue.get()
                    logger.info(f"📥 Résultat de agence_notexisting: {result}")
                    if result["status"] == "error":
                        await cleanup_task("agence_notexisting")
                    state.task = None
            else:
                if state.running:
                    logger.info("⏹️ Arrêt de agence_notexisting (horaire diurne)")
                    await cleanup_task("agence_notexisting")
            
            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"⚠️ Erreur dans agence_notexisting_task: {e}")
            await cleanup_task("agence_notexisting")
            await asyncio.sleep(60)

async def process_images_task():
    state = running_tasks["process_and_transfer"]
    logger.info("📸 Lancement continu du traitement des images avec 5 instances...")
    state.running = True
    while True:
        try:
            await process_and_transfer_images(instances=5)
        except Exception as e:
            logger.error(f"⚠️ Erreur dans process_images_task: {e}")
            state.running = False
            await asyncio.sleep(10)
            state.running = True

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