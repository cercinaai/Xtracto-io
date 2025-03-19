import asyncio
from typing import List, Dict
from urllib.parse import urlparse
from src.database.realState import RealState, update_annonce_images, transfer_annonce
from src.database.agence import transfer_agence
from src.utils.b2_utils import upload_image_to_b2
from src.database.database import get_source_db, get_destination_db
from loguru import logger
from datetime import datetime
from src.database.database import init_db, close_db

async def transfer_processed_annonces(max_concurrent_tasks: int = 20) -> Dict:
    source_db = get_source_db()
    query = {
        "images": {"$elemMatch": {"$regex": "^https://f003\\.backblazeb2\\.com/file/cercina-real-estate-files"}},
        "idAgence": {"$exists": True}
    }
    annonces = await source_db["realStateLbc"].find(query).to_list(length=None)
    total_annonces = len(annonces)
    logger.info(f"📤 {total_annonces} annonces déjà traitées avec idAgence à transférer")

    if total_annonces == 0:
        return {"transferred": 0}

    transferred_count = 0
    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def transfer_annonce_wrapper(annonce: Dict) -> bool:
        async with semaphore:
            try:
                # Utiliser storeId ou idAgence pour l'agence
                storeId = annonce.get("storeId")
                idAgence = annonce.get("idAgence")
                if storeId:
                    await transfer_agence(storeId, annonce.get("agenceName"))
                elif idAgence:
                    await transfer_agence(idAgence, annonce.get("agenceName"))
                return await transfer_annonce(annonce)
            except Exception as e:
                logger.error(f"⚠️ Erreur transfert annonce {annonce['idSec']} : {e}")
                return False

    tasks = [transfer_annonce_wrapper(annonce) for annonce in annonces]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    transferred_count = sum(1 for res in results if res is True)
    logger.info(f"✅ {transferred_count} annonces transférées")
    return {"transferred": transferred_count}

async def process_and_transfer_images(max_concurrent_tasks: int = 20) -> Dict:
    await init_db()
    source_db = get_source_db()
    dest_db = get_destination_db()

    # Vérifier si la collection realStateFinale existe
    collections = await dest_db.list_collection_names()
    if "realStateFinale" not in collections:
        logger.warning("⚠️ La collection realStateFinale n'existe pas, elle sera créée automatiquement lors de la première insertion.")

    # Étape 1 : Récupérer les annonces de realStateWithAgence avec des images non traitées
    query = {
        "idAgence": {"$exists": True},
        "images": {
            "$exists": True,
            "$ne": [],  # S'assurer que les annonces ont des images
            "$not": {"$elemMatch": {"$regex": "https://f003.backblazeb2.com"}}  # Images non traitées
        }
    }
    annonces_with_agence = await source_db["realStateWithAgence"].find(query).to_list(length=None)
    total_annonces = len(annonces_with_agence)
    logger.info(f"📸 {total_annonces} annonces dans realStateWithAgence avec des images non traitées.")

    if total_annonces == 0:
        await close_db()
        return {"processed": 0}

    # Étape 2 : Récupérer les IDs des annonces déjà présentes dans realStateFinale
    existing_ids = await dest_db["realStateFinale"].distinct("idSec")
    logger.info(f"🔍 {len(existing_ids)} annonces déjà présentes dans realStateFinale.")

    # Étape 3 : Filtrer les annonces qui ne sont pas dans realStateFinale
    annonces_to_process = [annonce for annonce in annonces_with_agence if annonce["idSec"] not in existing_ids]
    total_to_process = len(annonces_to_process)
    logger.info(f"📸 {total_to_process} annonces à traiter (non présentes dans realStateFinale).")

    if total_to_process == 0:
        await close_db()
        return {"processed": 0}

    # Étape 4 : Traiter les images des annonces
    processed_count = 0
    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def process_annonce_wrapper(annonce: Dict) -> bool:
        async with semaphore:
            annonce_id = annonce["idSec"]
            raw_images = annonce.get("images", [])
            if not raw_images:
                logger.info(f"ℹ️ Annonce {annonce_id} sans images, ignorée.")
                return False
            result = await process_annonce_images(annonce, annonce_id, raw_images)
            return bool(result)

    tasks = [process_annonce_wrapper(annonce) for annonce in annonces_to_process]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    processed_count = sum(1 for res in results if res is True)

    # Vérifier le nombre d'annonces dans realStateFinale après traitement
    final_count = await dest_db["realStateFinale"].count_documents({})
    logger.info(f"📊 Après traitement, realStateFinale contient {final_count} annonces.")

    await close_db()
    logger.info(f"✅ {processed_count} annonces traitées et transférées vers realStateFinale.")
    return {"processed": processed_count}

async def process_annonce_images(annonce: Dict, annonce_id: str, image_urls: List[str]) -> Dict:
    try:
        # Étape 1 : Télécharger et uploader les images vers Backblaze
        upload_tasks = [
            upload_image_to_b2(
                image_url,
                "".join(c if c.isalnum() or c in "-_." else "_" for c in urlparse(image_url).path.split('/')[-1] or "default.jpg")
            )
            for image_url in image_urls if image_url.startswith('http')
        ]
        uploaded_urls = await asyncio.gather(*upload_tasks, return_exceptions=True)
        updated_image_urls = []
        failed_uploads = 0
        for url in uploaded_urls:
            if isinstance(url, str) and url != "N/A":
                updated_image_urls.append(url)
            else:
                updated_image_urls.append("N/A")
                failed_uploads += 1

        # Vérifier si toutes les images ont échoué
        if failed_uploads == len(uploaded_urls):
            logger.error(f"⚠️ Toutes les images de l'annonce {annonce_id} ont échoué à l'upload, abandon du transfert.")
            return None

        # Étape 2 : Mettre à jour les images dans l'annonce
        annonce["images"] = updated_image_urls
        annonce["nbrImages"] = len(updated_image_urls)
        annonce["scraped_at"] = datetime.utcnow()

        # Étape 3 : Transférer l'annonce vers realStateFinale
        success = await transfer_annonce(annonce)
        if not success:
            logger.error(f"⚠️ Échec du transfert de l'annonce {annonce_id} vers realStateFinale.")
            return None
        logger.info(f"✅ Annonce {annonce_id} transférée vers realStateFinale avec {len(updated_image_urls)} images.")

        # Étape 4 : Mettre à jour l'annonce dans realStateWithAgence avec les nouvelles URLs
        source_db = get_source_db()
        await source_db["realStateWithAgence"].update_one(
            {"idSec": annonce_id},
            {"$set": {"images": updated_image_urls, "nbrImages": len(updated_image_urls), "scraped_at": datetime.utcnow()}}
        )
        logger.info(f"📝 Annonce {annonce_id} mise à jour dans realStateWithAgence avec les nouvelles URLs d'images.")

        return {"idSec": annonce_id, "images": updated_image_urls}

    except Exception as e:
        logger.error(f"⚠️ Erreur pour annonce {annonce_id} : {e}")
        return None

if __name__ == "__main__":
    asyncio.run(process_and_transfer_images())