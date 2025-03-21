import asyncio
import random
from datetime import datetime, time
from src.scrapers.leboncoin.image_processor import process_and_transfer_images
from src.scrapers.leboncoin.firstScrapper import open_leboncoin
from src.scrapers.leboncoin.leboncoinLoopScrapper import open_leboncoin_loop
from src.scrapers.leboncoin.agenceBrute_scraper import scrape_agences
from src.scrapers.leboncoin.agence_notexisting import scrape_annonce_agences
from src.database.database import get_source_db
from multiprocessing import Process, Queue
from loguru import logger
from src.api.apis import running_tasks

DAY_START = time(10, 0)  # 10:00 AM
DAY_END = time(22, 0)    # 10:00 PM
NIGHT_START = time(22, 0)  # 10:00 PM
NIGHT_END = time(10, 0)    # 10:00 AM

def is_within_day_window():
    now = datetime.now().time()
    return DAY_START <= now < DAY_END

def is_within_night_window():
    now = datetime.now().time()
    return now >= NIGHT_START or now < NIGHT_END

def run_async_in_process(func, queue):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(func(queue))
    finally:
        loop.close()

async def run_scraper_in_process(func, task_name: str) -> dict:
    queue = Queue()
    process = Process(target=run_async_in_process, args=(func, queue))
    process.start()
    process.join()
    if not queue.empty():
        return queue.get()
    return {"status": "error", "message": f"{task_name} did not return a result"}

async def first_scraper_task():
    while True:
        if is_within_day_window():
            if not running_tasks["scrape_100_pages"]:
                logger.info("🚀 Lancement de firstScraper...")
                result = await run_scraper_in_process(open_leboncoin, "firstScraper")
                if result["status"] == "success":
                    logger.info("✅ firstScraper terminé avec succès.")
                else:
                    logger.error(f"⚠️ firstScraper échoué : {result['message']}")
                wait_time = random.uniform(15 * 60, 20 * 60)  # 15-20 minutes
                logger.info(f"⏳ Pause de {wait_time/60:.1f} minutes avant relance de firstScraper...")
                await asyncio.sleep(wait_time)
        else:
            now = datetime.now()
            next_start = datetime.combine(now.date(), DAY_START)
            if now.time() >= DAY_START:
                next_start = next_start.replace(day=next_start.day + 1)
            seconds_until_next = (next_start - now).total_seconds()
            logger.info(f"⏳ Hors fenêtre jour. Attente jusqu'à 10h00 ({seconds_until_next:.0f} secondes)...")
            await asyncio.sleep(seconds_until_next)

async def loop_scraper_task():
    logger.info("⏳ Attente de 5 minutes avant le démarrage de loopScraper...")
    await asyncio.sleep(5 * 60)  # Attendre 5 minutes après le démarrage
    while True:
        if is_within_day_window():
            if not running_tasks["scrape_loop"]:
                logger.info("🔄 Lancement de loopScraper...")
                result = await run_scraper_in_process(open_leboncoin_loop, "loopScraper")
                if result["status"] == "success":
                    logger.info("✅ loopScraper terminé avec succès.")
                else:
                    logger.error(f"⚠️ loopScraper échoué : {result['message']}")
                wait_time = random.uniform(2 * 60, 5 * 60)  # 2-5 minutes
                logger.info(f"⏳ Pause de {wait_time/60:.1f} minutes avant relance de loopScraper...")
                await asyncio.sleep(wait_time)
        else:
            now = datetime.now()
            next_start = datetime.combine(now.date(), DAY_START)
            if now.time() >= DAY_START:
                next_start = next_start.replace(day=next_start.day + 1)
            seconds_until_next = (next_start - now).total_seconds()
            logger.info(f"⏳ Hors fenêtre jour. Attente jusqu'à 10h00 ({seconds_until_next:.0f} secondes)...")
            await asyncio.sleep(seconds_until_next)

async def agence_brute_scraper_task():
    source_db = get_source_db()
    agences_brute_collection = source_db["agencesBrute"]
    while True:
        if is_within_night_window():
            remaining = await agences_brute_collection.count_documents({"scraped": {"$ne": True}})
            if remaining > 0 and not running_tasks["scrape_agence_brute"]:
                logger.info("🔍 Lancement de agenceBrute_scraper...")
                result = await run_scraper_in_process(scrape_agences, "agenceBrute_scraper")
                if result["status"] == "success":
                    logger.info("✅ agenceBrute_scraper terminé avec succès.")
                else:
                    logger.error(f"⚠️ agenceBrute_scraper échoué : {result['message']}")
                await asyncio.sleep(5 * 60)  # 5 minutes
            else:
                logger.info("ℹ️ Aucune agence brute restante ou tâche en cours.")
                await asyncio.sleep(60)  # Vérifier toutes les minutes
        else:
            now = datetime.now()
            next_start = datetime.combine(now.date(), NIGHT_START)
            if now.time() >= NIGHT_START or now.time() < NIGHT_END:
                next_start = next_start.replace(day=next_start.day + 1)
            seconds_until_next = (next_start - now).total_seconds()
            logger.info(f"⏳ Hors fenêtre nuit. Attente jusqu'à 22h00 ({seconds_until_next:.0f} secondes)...")
            await asyncio.sleep(seconds_until_next)

async def agence_notexisting_task():
    source_db = get_source_db()
    realstate_collection = source_db["realState"]
    while True:
        if is_within_night_window():
            remaining = await realstate_collection.count_documents({"idAgence": {"$exists": False}})
            if remaining > 0 and not running_tasks["scrape_agence_notexisting"]:
                logger.info("🔍 Lancement de agence_notexisting...")
                result = await run_scraper_in_process(scrape_annonce_agences, "agence_notexisting")
                if result["status"] == "success":
                    logger.info("✅ agence_notexisting terminé avec succès.")
                else:
                    logger.error(f"⚠️ agence_notexisting échoué : {result['message']}")
                await asyncio.sleep(5 * 60)  # 5 minutes
            else:
                logger.info("ℹ️ Aucune annonce sans agence restante ou tâche en cours.")
                await asyncio.sleep(60)  # Vérifier toutes les minutes
        else:
            now = datetime.now()
            next_start = datetime.combine(now.date(), NIGHT_START)
            if now.time() >= NIGHT_START or now.time() < NIGHT_END:
                next_start = next_start.replace(day=next_start.day + 1)
            seconds_until_next = (next_start - now).total_seconds()
            logger.info(f"⏳ Hors fenêtre nuit. Attente jusqu'à 22h00 ({seconds_until_next:.0f} secondes)...")
            await asyncio.sleep(seconds_until_next)

async def start_cron():
    # Lancer chaque tâche de manière indépendante avec create_task
    asyncio.create_task(process_and_transfer_images())
    logger.info("📸 Lancement continu du traitement des images.")
    
    asyncio.create_task(first_scraper_task())
    logger.info("⏰ Planification de firstScraper pour 10h00-22h00.")
    
    asyncio.create_task(loop_scraper_task())
    logger.info("⏰ Planification de loopScraper pour 10h05-22h00.")
    
    asyncio.create_task(agence_brute_scraper_task())
    logger.info("⏰ Planification de agenceBrute_scraper pour 22h00-10h00.")
    
    asyncio.create_task(agence_notexisting_task())
    logger.info("⏰ Planification de agence_notexisting pour 22h00-10h00.")
    
    # Garder l'événement principal actif
    while True:
        await asyncio.sleep(60)  # Vérifier toutes les minutes que les tâches tournent
        logger.debug("🟢 Vérification : cron est toujours actif.")

if __name__ == "__main__":
    asyncio.run(start_cron())