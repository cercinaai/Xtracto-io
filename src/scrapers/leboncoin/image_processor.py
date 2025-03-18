import asyncio
from typing import List, Dict
from urllib.parse import urlparse
from src.database.realState import RealState, update_annonce_images, transfer_annonce
from src.database.agence import transfer_agence
from src.utils.b2_utils import upload_image_to_b2
from src.database.database import get_source_db
from loguru import logger
from datetime import datetime
from src.database.database import init_db, close_db, get_source_db, get_destination_db
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
    query = {
        "idAgence": {"$exists": True},
        "images": {"$not": {"$elemMatch": {"$regex": "https://f003.backblazeb2.com"}}}
    }
    annonces = await source_db["realStateWithAgence"].find(query).to_list(length=None)
    total_annonces = len(annonces)
    logger.info(f"📸 {total_annonces} annonces avec idAgence à traiter")

    if total_annonces == 0:
        await close_db()
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
    await close_db()
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
        source_db = get_source_db()
        await source_db["realStateWithAgence"].update_one(
            {"idSec": annonce_id},
            {"$set": {"images": updated_image_urls, "nbrImages": len(updated_image_urls), "scraped_at": datetime.utcnow()}}
        )

        # Transférer vers la base destination
        annonce["images"] = updated_image_urls
        annonce["nbrImages"] = len(updated_image_urls)
        annonce["scraped_at"] = datetime.utcnow()
        await transfer_annonce(annonce)
        return {"idSec": annonce_id, "images": updated_image_urls}

    except Exception as e:
        logger.error(f"⚠️ Erreur pour annonce {annonce_id} : {e}")
        return None

if __name__ == "__main__":
    asyncio.run(process_and_transfer_images())