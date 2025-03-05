from src.database.database import source_db
from src.scrapers.leboncoin.image_processor import process_all_images
from loguru import logger
import asyncio
from typing import Optional

# Indicateur d'exécution du job
jobs_running: dict[str, bool] = {"images": False}

async def process_images_job():
    """Job asynchrone pour traiter automatiquement les images."""
    if jobs_running["images"]:
        logger.info("📸 Job de traitement des images déjà en cours, ignoré")
        return

    jobs_running["images"] = True
    try:
        result = await process_all_images(max_concurrent_tasks=20)
        logger.info(f"✅ Job de traitement des images terminé : {len(result)} annonces traitées")
    except Exception as e:
        logger.error(f"⚠️ Erreur dans le job de traitement des images : {e}")
    finally:
        jobs_running["images"] = False
        logger.info("🔄 État du job de traitement des images réinitialisé")

async def start_background_jobs():
    """Boucle infinie pour vérifier et lancer le job de traitement des images automatiquement."""
    logger.info("🕒 Démarrage du job automatique de traitement des images en arrière-plan")
    while True:
        try:
            # Vérification des annonces à traiter (images non uploadées sur Backblaze)
            annonces_count = await source_db["realStateLbc"].count_documents({
                "images": {
                    "$not": {
                        "$elemMatch": {"$regex": "https://f003.backblazeb2.com"}
                    }
                }
            })

            # Lancement du job si des annonces sont détectées et que le job n'est pas déjà en cours
            if annonces_count > 0 and not jobs_running["images"]:
                logger.info(f"📸 {annonces_count} annonces détectées, lancement du job de traitement des images...")
                asyncio.create_task(process_images_job())

            # Attente avant la prochaine vérification
            await asyncio.sleep(60)  # Vérifie toutes les 60 secondes
        except Exception as e:
            logger.error(f"⚠️ Erreur dans la boucle des jobs : {e}")
            await asyncio.sleep(300)  # Pause de 5 minutes en cas d’erreur pour éviter une surcharge