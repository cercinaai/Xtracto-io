import asyncio
from datetime import datetime, timedelta
import logging
from src.database.database import init_db, get_source_db, get_destination_db
from src.database.realState import transfer_from_withagence_to_finale

logger = logging.getLogger(__name__)

async def process_and_transfer_images(instances: int = 5) -> None:
    """Traite et transfère les annonces en continu avec plusieurs instances."""
    logger.info(f"🚀 Début du traitement continu des images avec {instances} instances...")
    try:
        await init_db()
        logger.info("✅ Base de données initialisée avec succès.")
    except Exception as e:
        logger.error(f"❌ Échec de l'initialisation de la base de données: {e}")
        return

    tasks = [process_instance(i) for i in range(instances)]
    await asyncio.gather(*tasks)

async def process_instance(instance_id: int) -> None:
    """Instance individuelle de traitement des annonces avec vérification horaire."""
    source_db = get_source_db()
    dest_db = get_destination_db()
    last_check = datetime.now() - timedelta(hours=1)

    while True:
        try:
            current_time = datetime.now()
            if (current_time - last_check).total_seconds() >= 3600:
                logger.info(f"⏰ Instance {instance_id} : Vérification horaire des nouvelles annonces.")
                last_check = current_time

            finale_ids = await dest_db["realStateFinale"].distinct("idSec")
            query = {
                "idAgence": {"$exists": True},
                "images": {"$exists": True, "$ne": [], "$nin": [[], ["N/A"]]},
                "processed": {"$ne": True},
                "idSec": {"$nin": finale_ids}
            }
            batch_size = 20
            annonces = await source_db["realStateWithAgence"].find(query).limit(batch_size).to_list(length=None)

            if not annonces:
                await asyncio.sleep(10)
                continue

            tasks = [transfer_from_withagence_to_finale(annonce) for annonce in annonces]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for annonce, result in zip(annonces, results):
                annonce_id = annonce["idSec"]
                if isinstance(result, Exception):
                    logger.debug(f"Instance {instance_id} : Erreur pour {annonce_id}, passage à la suivante : {result}")
                    continue
                if not result["skipped"]:
                    await source_db["realStateWithAgence"].update_one(
                        {"idSec": annonce_id},
                        {"$set": {"processed": True, "processed_at": datetime.utcnow()}}
                    )
                # Pas de log ici pour éviter surcharge, sauf si nécessaire

            await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"⚠️ Instance {instance_id} : Erreur générale, poursuite après attente : {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(process_and_transfer_images())