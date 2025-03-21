import asyncio
from datetime import datetime
from src.database.realState import transfer_from_withagence_to_finale
from src.database.database import init_db, close_db, get_source_db, get_destination_db
from loguru import logger

# Configure logger to output only the remaining annonces count
logger.remove()  # Remove default handler
logger.add(lambda msg: print(f"Annonces remaining to process: {msg.record['message']}"), level="INFO", format="{message}")

async def process_and_transfer_images() -> None:
    """
    Continuously monitor realStateWithAgence for new or updated annonces, process their images,
    and transfer them to realStateFinale, avoiding duplicates based on idSec and title.
    Annonces with no images (images: []) are skipped and marked as processed.
    """
    await init_db()
    source_db = get_source_db()
    dest_db = get_destination_db()

    while True:
        try:
            # First, handle annonces with images: [] by marking them as processed
            zero_images_query = {
                "idAgence": {"$exists": True},
                "images": [],
                "processed": {"$ne": True}
            }
            zero_images_annonces = await source_db["realStateWithAgence"].find(zero_images_query).to_list(length=None)
            for annonce in zero_images_annonces:
                annonce_id = annonce["idSec"]
                annonce_title = annonce.get("title")
                logger.info(f"ℹ️ Annonce {annonce_id} ({annonce_title}) has no images (images: []), skipping transfer and marking as processed.")
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
                await asyncio.sleep(10)  # Wait 10 seconds before checking again
                continue

            # Fetch the oldest unprocessed annonce (based on scraped_at)
            annonce = await source_db["realStateWithAgence"].find_one(
                query,
                sort=[("scraped_at", 1)]  # Sort by scraped_at ascending (oldest first)
            )

            if not annonce:
                await asyncio.sleep(10)  # Wait 10 seconds if no annonces are found
                continue

            annonce_id = annonce["idSec"]
            annonce_title = annonce.get("title")

            # Check if the annonce already exists in realStateFinale
            existing = await dest_db["realStateFinale"].find_one({"idSec": annonce_id, "title": annonce_title})
            if existing:
                logger.info(f"ℹ️ Annonce {annonce_id} ({annonce_title}) déjà présente dans realStateFinale, marquage comme traitée.")
                # Mark as processed to avoid reprocessing
                await source_db["realStateWithAgence"].update_one(
                    {"idSec": annonce_id},
                    {"$set": {"processed": True, "processed_at": datetime.utcnow()}}
                )
                continue

            # Process the annonce
            try:
                result = await transfer_from_withagence_to_finale(annonce)
                if result["skipped"]:
                    logger.info(f"ℹ️ Annonce {annonce_id} déjà traitée (vérification redondante).")
                else:
                    logger.info(f"✅ Annonce {annonce_id} transférée à realStateFinale avec {len(result['images'])} images.")
                # Mark as processed
                await source_db["realStateWithAgence"].update_one(
                    {"idSec": annonce_id},
                    {"$set": {"processed": True, "processed_at": datetime.utcnow()}}
                )
            except Exception as e:
                logger.error(f"⚠️ Erreur lors du traitement de l'annonce {annonce_id} : {e}")
                # Mark as processed to avoid infinite loops
                await source_db["realStateWithAgence"].update_one(
                    {"idSec": annonce_id},
                    {"$set": {"processed": True, "processed_at": datetime.utcnow()}}
                )

        except Exception as e:
            logger.error(f"⚠️ Erreur générale dans la boucle de traitement : {e}")
            await asyncio.sleep(10)  # Wait 10 seconds before retrying on error
            continue

if __name__ == "__main__":
    asyncio.run(process_and_transfer_images())