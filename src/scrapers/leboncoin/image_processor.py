import asyncio
from datetime import datetime
import logging
from src.database.database import init_db, get_source_db, get_destination_db
from src.database.realState import transfer_from_withagence_to_finale

logger = logging.getLogger(__name__)

async def process_and_transfer_images() -> None:
    logger.info("🚀 Début du traitement continu des images...")
    try:
        await init_db()
        logger.info("✅ Base de données initialisée avec succès.")
    except Exception as e:
        logger.error(f"❌ Échec de l'initialisation de la base de données: {e}")
        return

    source_db = get_source_db()
    dest_db = get_destination_db()

    while True:
        try:
            logger.debug("🔄 Nouvelle itération de traitement des images.")

            # Récupérer les idSec déjà présents dans realStateFinale
            finale_ids = await dest_db["realStateFinale"].distinct("idSec")

            # Traitement des annonces sans images (marquer comme traitées, pas de transfert)
            zero_images_query = {
                "idAgence": {"$exists": True},
                "images": {"$in": [[], None]},  # Images vides ou absentes
                "processed": {"$ne": True}
            }
            zero_images_count = await source_db["realStateWithAgence"].count_documents(zero_images_query)
            logger.info(f"ℹ️ {zero_images_count} annonces sans images à marquer comme traitées.")

            if zero_images_count > 0:
                zero_images_annonces = await source_db["realStateWithAgence"].find(zero_images_query).to_list(length=None)
                for annonce in zero_images_annonces:
                    annonce_id = annonce["idSec"]
                    logger.info(f"✅ Marquage de l'annonce sans images {annonce_id} comme traitée (non transférée).")
                    await source_db["realStateWithAgence"].update_one(
                        {"idSec": annonce_id},
                        {"$set": {"processed": True, "processed_at": datetime.utcnow()}}
                    )

            # Traitement des annonces avec images non présentes dans realStateFinale
            query = {
                "idAgence": {"$exists": True},
                "images": {"$exists": True, "$ne": [], "$nin": [[], ["N/A"]]},  # Au moins une image valide
                "processed": {"$ne": True},
                "idSec": {"$nin": finale_ids}  # Exclure celles déjà dans realStateFinale
            }
            remaining_count = await source_db["realStateWithAgence"].count_documents(query)
            logger.info(f"ℹ️ {remaining_count} annonces avec images valides à traiter et transférer.")

            if remaining_count == 0:
                logger.debug("⏳ Aucune annonce avec images valides à traiter, attente de 10 secondes.")
                await asyncio.sleep(10)
                continue

            # Traiter toutes les annonces correspondantes dans cette itération
            annonces = await source_db["realStateWithAgence"].find(query).to_list(length=None)
            for annonce in annonces:
                annonce_id = annonce["idSec"]
                annonce_title = annonce.get("title", "Sans titre")
                logger.info(f"🔍 Début du traitement de l'annonce {annonce_id} ({annonce_title}).")

                # Transférer vers realStateFinale avec traitement des images
                result = await transfer_from_withagence_to_finale(annonce)
                if not result["skipped"]:
                    logger.info(f"✅ Annonce {annonce_id} traitée et transférée vers realStateFinale.")
                    # Supprimer de realStateWithAgence après transfert réussi
                else:
                    logger.info(f"ℹ️ Annonce {annonce_id} non transférée (doublon ou échec images), reste dans realStateWithAgence.")

            await asyncio.sleep(1)  # Pause entre itérations

        except Exception as e:
            logger.error(f"⚠️ Erreur dans la boucle de traitement: {e}")
            await asyncio.sleep(10)  # Attendre avant de réessayer en cas d'erreur

if __name__ == "__main__":
    asyncio.run(process_and_transfer_images())