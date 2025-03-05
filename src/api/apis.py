from fastapi import APIRouter, HTTPException, BackgroundTasks
from src.scrapers.leboncoin.image_processor import transfer_processed_annonces, process_and_transfer_images
from loguru import logger
import asyncio
from typing import Dict

api_router = APIRouter()

running_tasks: Dict[str, bool] = {
    "transfer_processed": False,
    "process_and_transfer": False
}

@api_router.get("/transfer/processed")
async def transfer_processed_annonces_endpoint(background_tasks: BackgroundTasks):
    if running_tasks["transfer_processed"]:
        return {"message": "Le transfert des annonces traitées est déjà en cours", "status": "running"}
    running_tasks["transfer_processed"] = True
    try:
        background_tasks.add_task(transfer_processed_task)
        logger.info("📤 Lancement du transfert des annonces traitées en arrière-plan")
        return {"message": "Transfert des annonces traitées lancé en arrière-plan", "status": "started"}
    except Exception as e:
        running_tasks["transfer_processed"] = False
        logger.error(f"⚠️ Erreur lors du lancement du transfert : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne : {str(e)}")

async def transfer_processed_task():
    try:
        result = await transfer_processed_annonces(max_concurrent_tasks=20)
        logger.info(f"✅ Transfert terminé : {result['transferred']} annonces transférées")
    except Exception as e:
        logger.error(f"⚠️ Erreur lors du transfert : {e}")
    finally:
        running_tasks["transfer_processed"] = False
        logger.info("🔄 État de la tâche de transfert réinitialisé")

@api_router.get("/scrape/leboncoin/process_and_transfer")
async def process_and_transfer_images_endpoint(background_tasks: BackgroundTasks):
    if running_tasks["process_and_transfer"]:
        return {"message": "Le traitement et transfert des images est déjà en cours", "status": "running"}
    running_tasks["process_and_transfer"] = True
    try:
        background_tasks.add_task(process_and_transfer_task)
        logger.info("📸 Lancement du traitement et transfert des images en arrière-plan")
        return {"message": "Traitement et transfert des images lancé en arrière-plan", "status": "started"}
    except Exception as e:
        running_tasks["process_and_transfer"] = False
        logger.error(f"⚠️ Erreur lors du lancement du traitement : {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne : {str(e)}")

async def process_and_transfer_task():
    try:
        result = await process_and_transfer_images(max_concurrent_tasks=20)
        logger.info(f"✅ Traitement et transfert terminé : {result['processed']} annonces traitées")
    except Exception as e:
        logger.error(f"⚠️ Erreur lors du traitement et transfert : {e}")
    finally:
        running_tasks["process_and_transfer"] = False
        logger.info("🔄 État de la tâche de traitement réinitialisé")

@api_router.get("/status")
async def get_task_status():
    return {
        "transfer_processed": "running" if running_tasks["transfer_processed"] else "idle",
        "process_and_transfer": "running" if running_tasks["process_and_transfer"] else "idle"
    }