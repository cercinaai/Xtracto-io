import asyncio
from typing import List, Dict
from urllib.parse import urlparse
from src.database.realState import RealStateLBCModel, update_annonce_images, transfer_annonce
from src.database.agence import transfer_agence
from src.utils.b2_utils import upload_image_to_b2
from src.database.database import get_source_db
from loguru import logger
from datetime import datetime

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
                idAgence = annonce.get("idAgence")  # Utiliser idAgence, pas storeId ici
                if idAgence:
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
    source_db = get_source_db()
    query = {
        "idAgence": {"$exists": True},
        "images": {"$not": {"$elemMatch": {"$regex": "https://f003.backblazeb2.com"}}}
    }
    annonces = await source_db["realStateLbc"].find(query).to_list(length=None)
    total_annonces = len(annonces)
    logger.info(f"📸 {total_annonces} annonces avec idAgence à traiter")

    if total_annonces == 0:
        return {"processed": 0}

    processed_count = 0
    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def process_annonce_wrapper(annonce: Dict) -> bool:
        async with semaphore:
            annonce_id = annonce["idSec"]
            raw_images = annonce.get("images", [])
            if not raw_images:
                logger.info(f"ℹ️ Annonce {annonce_id} sans images, ignorée")
                return False
            result = await process_annonce_images(annonce, annonce_id, raw_images)
            return bool(result)

    tasks = [process_annonce_wrapper(annonce) for annonce in annonces]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    processed_count = sum(1 for res in results if res is True)
    logger.info(f"✅ {processed_count} annonces traitées et transférées")
    return {"processed": processed_count}

async def process_annonce_images(annonce: Dict, annonce_id: str, image_urls: List[str]) -> Dict:
    try:
        upload_tasks = [
            upload_image_to_b2(
                image_url,
                "".join(c if c.isalnum() or c in "-_." else "_" for c in urlparse(image_url).path.split('/')[-1] or "default.jpg")
            )
            for image_url in image_urls if image_url.startswith('http')
        ]
        uploaded_urls = await asyncio.gather(*upload_tasks, return_exceptions=True)
        updated_image_urls = [url if isinstance(url, str) else "N/A" for url in uploaded_urls]

        # Mettre à jour les images dans la base source
        await update_annonce_images(annonce_id, updated_image_urls, len(updated_image_urls))

        # Récupérer l'annonce complète depuis la base source
        source_db = get_source_db()
        full_annonce = await source_db["realStateLbc"].find_one({"idSec": annonce_id})
        if not full_annonce:
            logger.error(f"❌ Annonce {annonce_id} non trouvée dans la base source après mise à jour des images")
            return None

        # Mettre à jour les champs liés aux images dans l'annonce complète
        full_annonce["images"] = updated_image_urls
        full_annonce["nbrImages"] = len(updated_image_urls)
        full_annonce["scraped_at"] = datetime.utcnow()

        # Transférer l'agence associée
        idAgence = full_annonce.get("idAgence")  # Utiliser idAgence de l'annonce complète
        if idAgence:
            await transfer_agence(idAgence, full_annonce.get("agenceName"))

        # Transférer l'annonce complète
        await transfer_annonce(full_annonce)
        return {"idSec": annonce_id, "images": updated_image_urls, "idAgence": idAgence}

    except Exception as e:
        logger.error(f"⚠️ Erreur pour annonce {annonce_id} : {e}")
        return None