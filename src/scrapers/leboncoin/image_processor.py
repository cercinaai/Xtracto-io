import asyncio
from typing import List, Dict
from urllib.parse import urlparse
from src.database.realState import transfer_annonce
from src.database.agence import transfer_agence
from src.utils.b2_utils import upload_image_to_b2
from src.database.database import init_db, close_db, get_source_db, get_destination_db
from loguru import logger
from datetime import datetime

async def process_and_transfer_images(max_concurrent_tasks: int = 20) -> Dict:
    await init_db()
    source_db = get_source_db()
    dest_db = get_destination_db()

    # Fetch annonces from realStateWithAgence that need processing
    query = {
        "idAgence": {"$exists": True},
        "images": {
            "$exists": True,
            "$ne": [],
            "$not": {"$elemMatch": {"$regex": "https://f003.backblazeb2.com"}}
        }
    }
    annonces_with_agence = await source_db["realStateWithAgence"].find(query).to_list(length=None)
    total_annonces = len(annonces_with_agence)
    logger.info(f"{total_annonces} annonce a traite")

    if total_annonces == 0:
        await close_db()
        return {"processed": 0}

    # Filter out annonces already in realStateFinale
    existing_ids = await dest_db["realStateFinale"].distinct("idSec")
    annonces_to_process = [annonce for annonce in annonces_with_agence if annonce["idSec"] not in existing_ids]
    total_to_process = len(annonces_to_process)
    logger.info(f"{total_to_process} annonce a traite")

    if total_to_process == 0:
        await close_db()
        return {"processed": 0}

    processed_count = 0
    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def process_annonce_wrapper(annonce: Dict) -> bool:
        async with semaphore:
            result = await process_annonce_images(annonce, annonce["idSec"], annonce.get("images", []))
            remaining = semaphore._value + len(semaphore._waiters) if semaphore._waiters else semaphore._value
            logger.info(f"{remaining + (total_to_process - processed_count - 1)} annonce a traite")
            return bool(result)

    tasks = [process_annonce_wrapper(annonce) for annonce in annonces_to_process]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    processed_count = sum(1 for res in results if res is True)

    await close_db()
    return {"processed": processed_count}

async def process_annonce_images(annonce: Dict, annonce_id: str, image_urls: List[str]) -> Dict:
    # Step 1: Upload images to Backblaze
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

    # Check if all uploads failed
    if failed_uploads == len(uploaded_urls):
        return None

    # Step 2: Update the annonce with new image URLs
    annonce["images"] = updated_image_urls
    annonce["nbrImages"] = len(updated_image_urls)
    annonce["scraped_at"] = datetime.utcnow()

    # Step 3: Transfer the annonce to realStateFinale
    source_db = get_source_db()
    dest_db = get_destination_db()
    
    # Ensure all attributes are copied, including _id
    annonce_to_transfer = annonce.copy()
    if "_id" in annonce_to_transfer:
        annonce_to_transfer["_id"] = annonce["_id"]  # Explicitly preserve the _id

    # Transfer to realStateFinale
    success = await transfer_annonce(annonce_to_transfer)
    if not success:
        return None

    # Step 4: Update the annonce in realStateWithAgence with new image URLs
    await source_db["realStateWithAgence"].update_one(
        {"idSec": annonce_id},
        {"$set": {"images": updated_image_urls, "nbrImages": len(updated_image_urls), "scraped_at": datetime.utcnow()}}
    )

    return {"idSec": annonce_id, "images": updated_image_urls}

if __name__ == "__main__":
    asyncio.run(process_and_transfer_images())