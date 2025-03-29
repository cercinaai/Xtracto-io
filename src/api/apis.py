from fastapi import APIRouter, BackgroundTasks, HTTPException
from src.scrapers.leboncoin.image_processor import process_and_transfer_images
from src.scrapers.leboncoin.firstScrapper import open_leboncoin
from src.scrapers.leboncoin.leboncoinLoopScrapper import open_leboncoin_loop
from src.scrapers.leboncoin.agenceBrute_scraper import scrape_agences
from src.scrapers.leboncoin.agence_notexisting import scrape_annonce_agences
from loguru import logger
from multiprocessing import Process, Queue
import asyncio
from datetime import datetime
from src.api.cron import running_tasks, cleanup_task
from src.api.transfer_agencies import transfer_agencies  

api_router = APIRouter()

def run_in_process(queue: Queue, func, task_name: str):
    """Exécute une fonction asynchrone dans un processus séparé."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(func(queue))
    except Exception as e:
        queue.put({"status": "error", "message": str(e)})
    finally:
        running_tasks[task_name].running = False
        loop.close()

async def run_scraper_task(queue: Queue, func, task_name: str, background_tasks: BackgroundTasks):
    """Lance un scraper et surveille son exécution."""
    if running_tasks[task_name].running:
        return {"message": f"{task_name.replace('_', ' ').title()} en cours", "status": "running"}
    
    running_tasks[task_name].running = True
    process = Process(target=run_in_process, args=(queue, func, task_name))
    process.start()
    background_tasks.add_task(monitor_queue, queue, task_name)
    logger.info(f"🚀 Lancement de {task_name}")
    return {"message": f"{task_name.replace('_', ' ').title()} lancé", "status": "started"}

@api_router.get("/scrape/leboncoin/100_pages")
async def scrape_100_pages_endpoint(background_tasks: BackgroundTasks):
    """Lance le scraper des 100 premières pages."""
    return await run_scraper_task(Queue(), open_leboncoin, "first_scraper", background_tasks)

@api_router.get("/scrape/leboncoin/loop")
async def scrape_loop_endpoint(background_tasks: BackgroundTasks):
    """Lance le scraper en boucle."""
    return await run_scraper_task(Queue(), open_leboncoin_loop, "loop_scraper", background_tasks)

@api_router.get("/scrape/leboncoin/agence_brute")
async def scrape_agence_brute_endpoint(background_tasks: BackgroundTasks):
    """Lance le scraper des agences brutes."""
    return await run_scraper_task(Queue(), scrape_agences, "agence_brute", background_tasks)

@api_router.get("/scrape/leboncoin/agence_notexisting")
async def scrape_agence_notexisting_endpoint(background_tasks: BackgroundTasks):
    """Lance le scraper des agences non existantes."""
    return await run_scraper_task(Queue(), scrape_annonce_agences, "agence_notexisting", background_tasks)

@api_router.get("/scrape/leboncoin/process_and_transfer")
async def process_and_transfer_images_endpoint(background_tasks: BackgroundTasks, instances: int = 5):
    """Lance le traitement des images avec un nombre configurable d'instances."""
    if running_tasks["process_and_transfer"].running:
        return {"message": "Traitement des images en cours", "status": "running"}
    
    if instances < 1 or instances > 10:  # Limite raisonnable pour éviter surcharge
        raise HTTPException(status_code=400, detail="Le nombre d'instances doit être entre 1 et 10.")
    
    running_tasks["process_and_transfer"].running = True
    background_tasks.add_task(process_and_transfer_images, instances)
    logger.info(f"📸 Lancement du traitement des images avec {instances} instances")
    return {"message": f"Traitement des images lancé avec {instances} instances", "status": "started"}

@api_router.get("/stop/{task_name}")
async def stop_task(task_name: str):
    """Arrête une tâche spécifique."""
    valid_tasks = ["first_scraper", "loop_scraper", "agence_brute", "agence_notexisting", "process_and_transfer"]
    if task_name not in valid_tasks:
        raise HTTPException(status_code=400, detail=f"Tâche {task_name} invalide. Tâches valides : {valid_tasks}")
    
    state = running_tasks[task_name]
    if state.running:
        await cleanup_task(task_name)
        if state.task:
            try:
                state.task.cancel()
                await asyncio.wait([state.task])
            except asyncio.CancelledError:
                pass
        logger.info(f"🛑 Tâche {task_name} arrêtée")
        return {"message": f"Tâche {task_name} arrêtée", "status": "stopped"}
    return {"message": f"Tâche {task_name} n'est pas en cours", "status": "idle"}

async def monitor_queue(queue: Queue, task_name: str):
    """Surveille la file d'attente pour les résultats ou erreurs."""
    timeout = 3600  # 1 heure max d'attente
    start_time = asyncio.get_event_loop().time()
    
    while running_tasks[task_name].running:
        if not queue.empty():
            result = queue.get()
            logger.info(f"📥 Résultat pour {task_name} : {result}")
            if result["status"] == "error":
                logger.error(f"⚠️ Erreur dans {task_name} : {result['message']}")
                await cleanup_task(task_name)
            break
        if asyncio.get_event_loop().time() - start_time > timeout:
            logger.warning(f"⌛ Timeout atteint pour {task_name}, arrêt forcé")
            await cleanup_task(task_name)
            break
        await asyncio.sleep(1)

# @api_router.get("/transfer/agences")
# async def transfer_agencies_endpoint(background_tasks: BackgroundTasks):
#     """Lance le transfert des agences vers agencesFinale."""
#     return await run_scraper_task(Queue(), transfer_agencies, "transfer_agencies", background_tasks)

@api_router.get("/status")
async def get_task_status():
    """Retourne l'état de toutes les tâches."""
    return {
        "first_scraper": "running" if running_tasks["first_scraper"].running else "idle",
        "loop_scraper": "running" if running_tasks["loop_scraper"].running else "idle",
        "agence_brute": "running" if running_tasks["agence_brute"].running else "idle",
        "agence_notexisting": "running" if running_tasks["agence_notexisting"].running else "idle",
        "process_and_transfer": "running" if running_tasks["process_and_transfer"].running else "idle",
        # "transfer_agencies": "running" if running_tasks["transfer_agencies"].running else "idle"
    }

@api_router.get("/health")
async def health_check():
    """Vérifie la santé globale de l'API."""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}