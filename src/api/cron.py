import asyncio
from datetime import datetime, time, timedelta
from loguru import logger
from src.scrapers.leboncoin.image_processor import process_and_transfer_images
from src.scrapers.leboncoin.firstScrapper import open_leboncoin
from src.scrapers.leboncoin.leboncoinLoopScrapper import open_leboncoin_loop
from src.database.database import init_db, close_db, get_source_db
import os
import sys
from multiprocessing import Process, Queue

# Configuration des logs
if not os.path.exists("logs/leboncoin"):
    os.makedirs("logs/leboncoin")
if not os.path.exists("logs/capture/leboncoin"):
    os.makedirs("logs/capture/leboncoin")

logger.remove()  # Supprime la configuration par défaut
logger.add(sys.stdout, level="INFO")  # Affiche uniquement les logs INFO dans la console
logger.add(
    "logs/leboncoin/cron_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="7 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

async def process_images_job():
    """Tâche pour traiter les images des annonces de la collection realStateWithAgence."""
    try:
        await init_db()
        source_db = get_source_db()

        # Reset the 'processed' flag for annonces that have been updated since last processing
        await source_db["realStateWithAgence"].update_many(
            {
                "idAgence": {"$exists": True},
                "images": {
                    "$exists": True,
                    "$ne": [],
                    "$not": {"$elemMatch": {"$regex": "https://f003.backblazeb2.com"}}
                },
                "processed": True,
                "scraped_at": {"$gt": "$processed_at"}  # If scraped_at is more recent than processed_at
            },
            {"$set": {"processed": False}}
        )

        # Process all unprocessed annonces in batches
        skip = 0
        batch_size = 1000  # Process 1000 annonces at a time
        while True:
            result = await process_and_transfer_images(max_concurrent_tasks=50, skip=skip, limit=batch_size)
            processed = result["processed"]
            if processed > 0:
                logger.info(f"{processed} annonces traitees dans ce lot")
            else:
                logger.info("Aucune annonce a traiter dans ce lot")
                break  # Exit the loop if no more annonces to process
            skip += batch_size

    except Exception as e:
        logger.error(f"Erreur lors du traitement des images : {e}")
    finally:
        await close_db()

async def run_scraper_100_pages(queue: Queue):
    """Lance le scraper 100 pages et met le résultat dans la queue."""
    try:
        result = await open_leboncoin(queue)
        logger.info("Scraping 100 pages terminé")
        return result
    except Exception as e:
        logger.error(f"Erreur lors du scraping 100 pages : {e}")
        queue.put({"status": "error", "message": str(e)})
        return {"status": "error", "message": str(e)}

async def run_scraper_loop(queue: Queue):
    """Lance le scraper en boucle et met le résultat dans la queue."""
    try:
        result = await open_leboncoin_loop(queue)
        logger.info("Scraping en boucle terminé")
        return result
    except Exception as e:
        logger.error(f"Erreur lors du scraping en boucle : {e}")
        queue.put({"status": "error", "message": str(e)})
        return {"status": "error", "message": str(e)}

async def scraper_cron():
    """Cron pour gérer les scrapers entre 10h et 22h."""
    loop_scraper_process = None
    loop_scraper_queue = Queue()
    last_100_pages_run = None  # Timestamp of the last 100-pages scraper run

    while True:
        now = datetime.now()
        current_time = now.time()

        # Heure de début (10h00) et de fin (22h00)
        start_time = time(10, 0)  # 10:00 AM
        end_time = time(22, 0)    # 10:00 PM

        # Vérifier si l'heure actuelle est dans la plage 10h-22h
        if start_time <= current_time <= end_time:
            # Lancer le scraper 100 pages à 10h00 ou toutes les 30 minutes après la dernière exécution
            if (current_time.hour == 10 and current_time.minute == 0) or \
               (last_100_pages_run and (now - last_100_pages_run) >= timedelta(minutes=30)):
                logger.info("Lancement du scraper 100 pages")
                scraper_100_queue = Queue()
                scraper_100_process = Process(target=asyncio.run, args=(run_scraper_100_pages(scraper_100_queue),))
                scraper_100_process.start()
                scraper_100_process.join()
                result = scraper_100_queue.get()
                logger.info(f"Résultat du scraper 100 pages : {result}")
                last_100_pages_run = datetime.now()

            # Lancer le scraper en boucle à 10h05
            if current_time.hour == 10 and current_time.minute == 5 and loop_scraper_process is None:
                logger.info("Lancement du scraper en boucle à 10h05")
                loop_scraper_process = Process(target=asyncio.run, args=(run_scraper_loop(loop_scraper_queue),))
                loop_scraper_process.start()

        # Arrêter le scraper en boucle à 22h00
        if current_time.hour == 22 and current_time.minute == 0 and loop_scraper_process is not None:
            logger.info("Arrêt du scraper en boucle à 22h00")
            loop_scraper_process.terminate()
            loop_scraper_process.join()
            loop_scraper_process = None
            result = loop_scraper_queue.get() if not loop_scraper_queue.empty() else {"status": "stopped", "message": "Scraper en boucle arrêté"}
            logger.info(f"Résultat final du scraper en boucle : {result}")

        # Attendre 1 minute avant de vérifier à nouveau
        await asyncio.sleep(60)

async def start_cron():
    """Démarre les deux crons : un pour les images et un pour les scrapers."""
    # Lancer les deux tâches en parallèle
    await asyncio.gather(
        process_images_job_loop(),
        scraper_cron()
    )

async def process_images_job_loop():
    """Boucle pour le traitement des images."""
    while True:
        await process_images_job()
        await asyncio.sleep(300)  # Attendre 5 minutes avant la prochaine vérification

if __name__ == "__main__":
    asyncio.run(start_cron())