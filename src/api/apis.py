from fastapi import APIRouter, HTTPException, BackgroundTasks
from src.scrapers.leboncoin.image_processor import process_and_transfer_images
from src.scrapers.leboncoin.firstScrapper import open_leboncoin
from src.scrapers.leboncoin.traiteAnnonces import traite_annonces
from src.scrapers.leboncoin.leboncoinLoopScrapper import open_leboncoin_loop
from loguru import logger
from multiprocessing import Process, Queue
from typing import Dict
import asyncio

api_router = APIRouter()

running_tasks: Dict[str, bool] = {
    "scrape_100_pages": False,
    "process_agences": False,
    "process_and_transfer": False,
    "scrape_loop": False
}

# Stockage des processus actifs
active_processes: Dict[str, Process] = {}

def run_in_process(queue: Queue, func, task_name: str, skip: int = 0, limit: int = None):
    """Exécute une fonction dans un processus et met le résultat dans la queue."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(func(queue, skip=skip, limit=limit))
    except Exception as e:
        queue.put({"status": "error", "message": str(e)})
    finally:
        running_tasks[task_name] = False
        loop.close()
        if task_name in active_processes:
            del active_processes[task_name]

@api_router.get("/scrape/leboncoin/100_pages")
async def scrape_100_pages_endpoint(background_tasks: BackgroundTasks):
    if running_tasks["scrape_100_pages"]:
        return {"message": "Le scraping des 100 pages est déjà en cours", "status": "running"}
    running_tasks["scrape_100_pages"] = True
    queue = Queue()
    process = Process(target=run_in_process, args=(queue, open_leboncoin, "scrape_100_pages"))
    active_processes["scrape_100_pages"] = process
    process.start()
    background_tasks.add_task(monitor_queue, queue, "scrape_100_pages")
    logger.info("🌐 Lancement du scraping des 100 pages en arrière-plan")
    return {"message": "Scraping des 100 pages lancé en arrière-plan", "status": "started"}

@api_router.get("/scrape/leboncoin/stop_100_pages")
async def stop_100_pages_endpoint():
    if not running_tasks["scrape_100_pages"]:
        return {"message": "Aucun scraping des 100 pages en cours", "status": "idle"}
    if "scrape_100_pages" in active_processes:
        process = active_processes["scrape_100_pages"]
        process.terminate()
        process.join()
        del active_processes["scrape_100_pages"]
        running_tasks["scrape_100_pages"] = False
        logger.info("🛑 Scraping des 100 pages arrêté manuellement")
        return {"message": "Scraping des 100 pages arrêté", "status": "stopped"}
    return {"message": "Erreur : processus non trouvé", "status": "error"}

@api_router.get("/scrape/leboncoin/process_agences")
async def process_agences_endpoint(background_tasks: BackgroundTasks):
    if running_tasks["process_agences"]:
        return {"message": "Le traitement des annonces avec agences est déjà en cours", "status": "running"}
    running_tasks["process_agences"] = True
    background_tasks.add_task(process_agences_task)
    logger.info("📤 Lancement du traitement des annonces avec agences en arrière-plan")
    return {"message": "Traitement des annonces avec agences lancé en arrière-plan", "status": "started"}

async def process_agences_task():
    try:
        result = await traite_annonces()
        logger.info(f"✅ Traitement terminé : {result['processed']} annonces traitées")
    except Exception as e:
        logger.error(f"⚠️ Erreur lors du traitement : {e}")
    finally:
        running_tasks["process_agences"] = False

@api_router.get("/scrape/leboncoin/process_and_transfer")
async def process_and_transfer_images_endpoint(background_tasks: BackgroundTasks, skip: int = 0, limit: int = None):
    if running_tasks["process_and_transfer"]:
        return {"message": "Le traitement et transfert des images est déjà en cours", "status": "running"}
    running_tasks["process_and_transfer"] = True
    background_tasks.add_task(process_and_transfer_task, skip=skip, limit=limit)
    logger.info(f"📸 Lancement du traitement et transfert des images en arrière-plan (skip={skip}, limit={limit})")
    return {"message": f"Traitement et transfert des images lancé en arrière-plan (skip={skip}, limit={limit})", "status": "started"}

async def process_and_transfer_task(skip: int = 0, limit: int = None):
    try:
        result = await process_and_transfer_images(max_concurrent_tasks=20, skip=skip, limit=limit)
        logger.info(f"✅ Traitement terminé : {result['processed']} annonces traitées")
    except Exception as e:
        logger.error(f"⚠️ Erreur lors du traitement : {e}")
    finally:
        running_tasks["process_and_transfer"] = False

@api_router.get("/scrape/leboncoin/loop")
async def scrape_loop_endpoint(background_tasks: BackgroundTasks):
    running_tasks["scrape_loop"] = True
    queue = Queue()
    process = Process(target=run_in_process, args=(queue, open_leboncoin_loop, "scrape_loop"))
    active_processes["scrape_loop"] = process
    process.start()
    background_tasks.add_task(monitor_queue, queue, "scrape_loop")
    logger.info("🔄 Lancement du scraping en boucle en arrière-plan")
    return {"message": "Scraping en boucle lancé en arrière-plan", "status": "started"}

@api_router.get("/scrape/leboncoin/stop_loop")
async def stop_loop_endpoint():
    if not running_tasks["scrape_loop"]:
        return {"message": "Aucun scraping en boucle en cours", "status": "idle"}
    if "scrape_loop" in active_processes:
        process = active_processes["scrape_loop"]
        process.terminate()
        process.join()
        del active_processes["scrape_loop"]
        running_tasks["scrape_loop"] = False
        logger.info("🛑 Scraping en boucle arrêté manuellement")
        return {"message": "Scraping en boucle arrêté", "status": "stopped"}
    return {"message": "Erreur : processus non trouvé", "status": "error"}

async def monitor_queue(queue: Queue, task_name: str):
    """Surveille la queue pour les résultats et met à jour l'état."""
    while running_tasks[task_name]:
        if not queue.empty():
            result = queue.get()
            logger.info(f"📥 Résultat reçu pour {task_name} : {result}")
            if result["status"] == "error":
                logger.error(f"⚠️ Erreur dans {task_name} : {result['message']}")
            break
        await asyncio.sleep(1)

@api_router.get("/status")
async def get_task_status():
    return {
        "scrape_100_pages": "running" if running_tasks["scrape_100_pages"] else "idle",
        "process_agences": "running" if running_tasks["process_agences"] else "idle",
        "process_and_transfer": "running" if running_tasks["process_and_transfer"] else "idle",
        "scrape_loop": "running" if running_tasks["scrape_loop"] else "idle"
    }