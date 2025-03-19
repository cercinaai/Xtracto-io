import asyncio
from typing import Dict
from src.database.realState import transfer_from_withagence_to_finale
from src.database.database import init_db, close_db, get_source_db, get_destination_db
from loguru import logger

# Configure logger to only show INFO messages in the desired format
logger.remove()  # Remove default handler
logger.add(lambda msg: print(msg, end=""), level="INFO", format="annonce a traite : {message}")

async def process_and_transfer_images(max_concurrent_tasks: int = 20) -> Dict:
    """
    Process images for annonces in realStateWithAgence and transfer them to realStateFinale.
    
    Args:
        max_concurrent_tasks (int): Maximum number of concurrent tasks for processing annonces.
    
    Returns:
        Dict: A dictionary containing the number of processed annonces.
    """
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
    logger.info(total_annonces)

    if total_annonces == 0:
        await close_db()
        return {"processed": 0}

    # Filter out annonces already in realStateFinale
    existing_ids = await dest_db["realStateFinale"].distinct("idSec")
    annonces_to_process = [annonce for annonce in annonces_with_agence if annonce["idSec"] not in existing_ids]
    total_to_process = len(annonces_to_process)
    logger.info(total_to_process)

    if total_to_process == 0:
        await close_db()
        return {"processed": 0}

    processed_count = 0
    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    async def process_annonce_wrapper(annonce: Dict) -> bool:
        """
        Wrapper function to process an annonce with semaphore for concurrency control.
        
        Args:
            annonce (Dict): The annonce document to process.
        
        Returns:
            bool: True if the annonce was processed successfully, False otherwise.
        """
        nonlocal processed_count
        async with semaphore:
            result = await transfer_from_withagence_to_finale(annonce)
            processed_count += 1
            remaining = total_to_process - processed_count
            logger.info(remaining)
            return bool(result) and not result.get("skipped", False)

    tasks = [process_annonce_wrapper(annonce) for annonce in annonces_to_process]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    processed_count = sum(1 for res in results if res is True)

    await close_db()
    return {"processed": processed_count}

if __name__ == "__main__":
    asyncio.run(process_and_transfer_images())