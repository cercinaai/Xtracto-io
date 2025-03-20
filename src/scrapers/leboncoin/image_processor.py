import asyncio
from typing import Dict
from datetime import datetime
from src.database.realState import transfer_from_withagence_to_finale
from src.database.database import init_db, close_db, get_source_db, get_destination_db
from loguru import logger

# Configure logger to output only the desired format
logger.remove()  # Remove default handler
logger.add(lambda msg: print(f"annonce a traite : {msg.record['message']}"), level="INFO", format="{message}")

async def process_and_transfer_images(max_concurrent_tasks: int = 50, skip: int = 0, limit: int = None, batch_size: int = 100) -> Dict:
    """
    Process images for annonces in realStateWithAgence and transfer them to realStateFinale.
    
    Args:
        max_concurrent_tasks (int): Maximum number of concurrent tasks for processing annonces.
        skip (int): Number of documents to skip (for pagination).
        limit (int): Maximum number of documents to process (for pagination). If None, process all.
        batch_size (int): Number of documents to process in each batch.
    
    Returns:
        Dict: A dictionary containing the number of processed annonces.
    """
    await init_db()
    source_db = get_source_db()
    dest_db = get_destination_db()

    # Fetch total count of unprocessed annonces
    query = {
        "idAgence": {"$exists": True},
        "images": {
            "$exists": True,
            "$ne": [],
            "$not": {"$elemMatch": {"$regex": "https://f003.backblazeb2.com"}}
        },
        "processed": {"$ne": True}
    }
    total_annonces = await source_db["realStateWithAgence"].count_documents(query)
    logger.info(total_annonces)

    if total_annonces == 0:
        await close_db()
        return {"processed": 0}

    # Apply skip and limit to the query
    cursor = source_db["realStateWithAgence"].find(query).skip(skip)
    if limit is not None:
        cursor = cursor.limit(limit)

    # Process annonces in batches
    processed_count = 0
    total_to_process = min(limit, total_annonces - skip) if limit else total_annonces - skip
    logger.info(total_to_process)

    if total_to_process <= 0:
        await close_db()
        return {"processed": 0}

    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def process_annonce_wrapper(annonce: Dict) -> bool:
        """
        Wrapper function to process an annonce with semaphore for concurrency control.
        
        Args:
            annonce (Dict): The annonce document to process.
        
        Returns:
            bool: True if the annonce was processed successfully, False otherwise.
        """
        async with semaphore:
            try:
                result = await transfer_from_withagence_to_finale(annonce)
                if result and not result.get("skipped", False):
                    # Mark the annonce as processed in realStateWithAgence with a processed_at timestamp
                    await source_db["realStateWithAgence"].update_one(
                        {"idSec": annonce["idSec"]},
                        {"$set": {"processed": True, "processed_at": datetime.utcnow()}}
                    )
                    return True
                return False
            except Exception as e:
                logger.error(f"Erreur lors du traitement de l'annonce {annonce['idSec']}: {e}")
                return False

    # Process in batches
    batch = []
    async for annonce in cursor:
        batch.append(annonce)
        if len(batch) >= batch_size:
            # Filter out annonces already in realStateFinale
            existing_ids = await dest_db["realStateFinale"].distinct("idSec", {"idSec": {"$in": [a["idSec"] for a in batch]}})
            annonces_to_process = [a for a in batch if a["idSec"] not in existing_ids]

            if annonces_to_process:
                tasks = [process_annonce_wrapper(annonce) for annonce in annonces_to_process]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                processed_count += sum(1 for res in results if res is True)

            remaining = total_to_process - processed_count
            logger.info(remaining)
            batch = []

    # Process remaining annonces in the last batch
    if batch:
        existing_ids = await dest_db["realStateFinale"].distinct("idSec", {"idSec": {"$in": [a["idSec"] for a in batch]}})
        annonces_to_process = [a for a in batch if a["idSec"] not in existing_ids]

        if annonces_to_process:
            tasks = [process_annonce_wrapper(annonce) for annonce in annonces_to_process]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            processed_count += sum(1 for res in results if res is True)

        remaining = total_to_process - processed_count
        logger.info(remaining)

    await close_db()
    return {"processed": processed_count}

if __name__ == "__main__":
    import sys
    skip = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
    asyncio.run(process_and_transfer_images(max_concurrent_tasks=50, skip=skip, limit=limit))