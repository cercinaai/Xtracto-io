import asyncio
from datetime import datetime
from src.database.realState import transfer_from_withagence_to_finale
from src.database.database import init_db, close_db, get_source_db, get_destination_db
from loguru import logger

# Configurer un logger détaillé avec sortie console et fichier
logger.remove()  # Supprimer le handler par défaut
logger.add(lambda msg: print(f"Annonces remaining to process: {msg.record['message']}"), level="INFO", format="{message}")
logger.add("image_processor.log", level="DEBUG", rotation="10 MB")

async def process_and_transfer_images() -> None:
    """
    Continuously monitor realStateWithAgence for new or updated annonces, process their images,
    and transfer them to realStateFinale, avoiding duplicates based on idSec and title.
    Annonces with no images (images: []) are skipped and marked as processed.
    """
    logger.info("🚀 Début du traitement continu des images...")
    try:
        await init_db()
        logger.info("✅ Base de données initialisée avec succès.")
    except Exception as e:
        logger.error(f"❌ Échec de l'initialisation de la base de données : {e}")
        return  # Arrêter si la connexion échoue

    source_db = get_source_db()
    dest_db = get_destination_db()

    while True:  # Boucle infinie pour rester toujours actif
        try:
            logger.debug("🔄 Nouvelle itération de traitement des images.")
            # Handle annonces with images: [] by marking them as processed
            zero_images_query = {
                "idAgence": {"$exists": True},
                "images": [],
                "processed": {"$ne": True}
            }
            zero_images_annonces = await source_db["realStateWithAgence"].find(zero_images_query).to_list(length=None)
            logger.debug(f"📊 Nombre d'annonces sans images trouvées : {len(zero_images_annonces)}")
            for annonce in zero_images_annonces:
                annonce_id = annonce["idSec"]
                annonce_title = annonce.get("title", "Sans titre")
                logger.info(f"ℹ️ Annonce {annonce_id} ({annonce_title}) sans images, marquage comme traitée.")
                await source_db["realStateWithAgence"].update_one(
                    {"idSec": annonce_id},
                    {"$set": {"processed": True, "processed_at": datetime.utcnow()}}
                )

            # Query for unprocessed annonces with non-empty images
            query = {
                "idAgence": {"$exists": True},
                "images": {"$exists": True, "$ne": []},
                "processed": {"$ne": True}
            }

            # Count remaining annonces to process
            remaining_count = await source_db["realStateWithAgence"].count_documents(query)
            logger.info(remaining_count)

            if remaining_count == 0:
                logger.debug("ℹ️ Aucune annonce à traiter, attente de 10 secondes.")
                await asyncio.sleep(10)  # Attendre avant de vérifier à nouveau
                continue

            # Fetch the oldest unprocessed annonce
            annonce = await source_db["realStateWithAgence"].find_one(
                query,
                sort=[("scraped_at", 1)]  # Plus ancienne d'abord
            )

            if not annonce:
                logger.debug("ℹ️ Aucune annonce trouvée avec ce filtre, attente de 10 secondes.")
                await asyncio.sleep(10)
                continue

            annonce_id = annonce["idSec"]
            annonce_title = annonce.get("title", "Sans titre")
            logger.debug(f"🔍 Traitement de l'annonce {annonce_id} ({annonce_title}).")

            # Check if the annonce already exists in realStateFinale
            existing = await dest_db["realStateFinale"].find_one({"idSec": annonce_id, "title": annonce_title})
            if existing:
                logger.info(f"ℹ️ Annonce {annonce_id} ({annonce_title}) déjà dans realStateFinale, marquage comme traitée.")
                await source_db["realStateWithAgence"].update_one(
                    {"idSec": annonce_id},
                    {"$set": {"processed": True, "processed_at": datetime.utcnow()}}
                )
                continue

            # Process the annonce
            try:
                logger.debug(f"📤 Transfert de l'annonce {annonce_id} vers realStateFinale.")
                result = await transfer_from_withagence_to_finale(annonce)
                if result["skipped"]:
                    logger.info(f"ℹ️ Annonce {annonce_id} déjà traitée (vérification redondante).")
                else:
                    logger.info(f"✅ Annonce {annonce_id} transférée avec {len(result['images'])} images.")
                await source_db["realStateWithAgence"].update_one(
                    {"idSec": annonce_id},
                    {"$set": {"processed": True, "processed_at": datetime.utcnow()}}
                )
            except Exception as e:
                logger.error(f"⚠️ Erreur lors du traitement de l'annonce {annonce_id} : {e}")
                if "network" in str(e).lower():
                    logger.debug("ℹ️ Erreur réseau, passage à l’itération suivante.")
                    await asyncio.sleep(5)  # Attendre un peu avant de réessayer
                    continue
                # Marquer comme traité pour éviter une boucle infinie sur une erreur persistante
                await source_db["realStateWithAgence"].update_one(
                    {"idSec": annonce_id},
                    {"$set": {"processed": True, "processed_at": datetime.utcnow()}}
                )

        except Exception as e:
            logger.error(f"⚠️ Erreur générale dans la boucle de traitement : {e}")
            await asyncio.sleep(10)  # Attendre avant de réessayer en cas d’erreur

    # Note : Pas de finally avec close_db() car on veut que ça reste toujours actif

if __name__ == "__main__":
    asyncio.run(process_and_transfer_images())