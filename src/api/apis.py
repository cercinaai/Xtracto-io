from fastapi import APIRouter, BackgroundTasks
from src.scrapers.leboncoin.image_processor import process_and_transfer_images
from src.scrapers.leboncoin.firstScrapper import open_leboncoin
from src.scrapers.leboncoin.leboncoinLoopScrapper import open_leboncoin_loop
from src.scrapers.leboncoin.agenceBrute_scraper import scrape_agences
from src.scrapers.leboncoin.agence_notexisting import scrape_annonce_agences
from loguru import logger
from multiprocessing import Process, Queue
import asyncio

api_router = APIRouter()

running_tasks = {
    "scrape_100_pages": False,
    "scrape_loop": False,
    "scrape_agence_brute": False,
    "scrape_agence_notexisting": False,
    "process_and_transfer": False
}

active_processes = {}

def run_in_process(queue: Queue, func, task_name: str):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(func(queue))
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
        return {"message": "Scraping 100 pages en cours", "status": "running"}
    running_tasks["scrape_100_pages"] = True
    queue = Queue()
    process = Process(target=run_in_process, args=(queue, open_leboncoin, "scrape_100_pages"))
    active_processes["scrape_100_pages"] = process
    process.start()
    background_tasks.add_task(monitor_queue, queue, "scrape_100_pages")
    logger.info("🌐 Lancement du scraping des 100 pages")
    return {"message": "Scraping des 100 pages lancé", "status": "started"}

@api_router.get("/scrape/leboncoin/loop")
async def scrape_loop_endpoint(background_tasks: BackgroundTasks):
    if running_tasks["scrape_loop"]:
        return {"message": "Scraping en boucle en cours", "status": "running"}
    running_tasks["scrape_loop"] = True
    queue = Queue()
    process = Process(target=run_in_process, args=(queue, open_leboncoin_loop, "scrape_loop"))
    active_processes["scrape_loop"] = process
    process.start()
    background_tasks.add_task(monitor_queue, queue, "scrape_loop")
    logger.info("🔄 Lancement du scraping en boucle")
    return {"message": "Scraping en boucle lancé", "status": "started"}

@api_router.get("/scrape/leboncoin/agence_brute")
async def scrape_agence_brute_endpoint(background_tasks: BackgroundTasks):
    if running_tasks["scrape_agence_brute"]:
        return {"message": "Scraping agences brutes en cours", "status": "running"}
    running_tasks["scrape_agence_brute"] = True
    queue = Queue()
    process = Process(target=run_in_process, args=(queue, scrape_agences, "scrape_agence_brute"))
    active_processes["scrape_agence_brute"] = process
    process.start()
    background_tasks.add_task(monitor_queue, queue, "scrape_agence_brute")
    logger.info("🔍 Lancement du scraping des agences brutes")
    return {"message": "Scraping des agences brutes lancé", "status": "started"}

@api_router.get("/scrape/leboncoin/agence_notexisting")
async def scrape_agence_notexisting_endpoint(background_tasks: BackgroundTasks):
    if running_tasks["scrape_agence_notexisting"]:
        return {"message": "Scraping agences non existantes en cours", "status": "running"}
    running_tasks["scrape_agence_notexisting"] = True
    queue = Queue()
    process = Process(target=run_in_process, args=(queue, scrape_annonce_agences, "scrape_agence_notexisting"))
    active_processes["scrape_agence_notexisting"] = process
    process.start()
    background_tasks.add_task(monitor_queue, queue, "scrape_agence_notexisting")
    logger.info("🔍 Lancement du scraping des agences non existantes")
    return {"message": "Scraping des agences non existantes lancé", "status": "started"}

@api_router.get("/scrape/leboncoin/process_and_transfer")
async def process_and_transfer_images_endpoint(background_tasks: BackgroundTasks):
    if running_tasks["process_and_transfer"]:
        return {"message": "Traitement images en cours", "status": "running"}
    running_tasks["process_and_transfer"] = True
    background_tasks.add_task(process_and_transfer_images)
    logger.info("📸 Lancement du traitement des images")
    return {"message": "Traitement des images lancé", "status": "started"}

@api_router.get("/stop/{task_name}")
async def stop_task(task_name: str):
    if task_name in active_processes and running_tasks[task_name]:
        process = active_processes[task_name]
        process.terminate()
        running_tasks[task_name] = False
        del active_processes[task_name]
        logger.info(f"🛑 Tâche {task_name} arrêtée.")
        return {"message": f"Tâche {task_name} arrêtée", "status": "stopped"}
    return {"message": f"Tâche {task_name} non en cours", "status": "idle"}

async def monitor_queue(queue: Queue, task_name: str):
    while running_tasks[task_name]:
        if not queue.empty():
            result = queue.get()
            logger.info(f"📥 Résultat pour {task_name} : {result}")
            if result["status"] == "error":
                logger.error(f"⚠️ Erreur dans {task_name} : {result['message']}")
            break
        elif task_name not in active_processes or not active_processes[task_name].is_alive():
            logger.error(f"⚠️ Processus {task_name} a planté.")
            running_tasks[task_name] = False
            break
        await asyncio.sleep(1)

@api_router.get("/status")
async def get_task_status():
    return {
        "scrape_100_pages": "running" if running_tasks["scrape_100_pages"] else "idle",
        "scrape_loop": "running" if running_tasks["scrape_loop"] else "idle",
        "scrape_agence_brute": "running" if running_tasks["scrape_agence_brute"] else "idle",
        "scrape_agence_notexisting": "running" if running_tasks["scrape_agence_notexisting"] else "idle",
        "process_and_transfer": "running" if running_tasks["process_and_transfer"] else "idle"
    }