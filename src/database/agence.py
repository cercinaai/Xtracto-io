from pydantic import BaseModel
from typing import Optional
from loguru import logger
from src.database.database import get_source_db, get_destination_db

class AgenceModel(BaseModel):
    storeId: str
    name: str
    lien: str
    CodeSiren: Optional[str] = None
    logo: Optional[str] = None
    adresse: Optional[str] = None
    zone_intervention: Optional[str] = None
    siteWeb: Optional[str] = None
    horaires: Optional[str] = None
    number: Optional[str] = None
    description: Optional[str] = None

async def transfer_agence(storeId: str, name: Optional[str] = None) -> Optional[str]:
    source_db = get_source_db()
    dest_db = get_destination_db()
    source_collection = source_db["agencesBrute"]
    dest_collection = dest_db["agencesFinale"]

    existing = await dest_collection.find_one({"storeId": storeId, "name": name})
    if existing:
        logger.info(f"ℹ️ Agence {storeId} ({name}) déjà présente dans agencesFinale avec _id: {existing['_id']}.")
        return str(existing["_id"])

    agence_source = await source_collection.find_one({"storeId": storeId, "name": name})
    if not agence_source:
        logger.warning(f"⚠️ Agence {storeId} non trouvée dans agencesBrute. Création minimale.")
        agence_source = {
            "storeId": storeId,
            "name": name if name else f"Agence {storeId}",
            "lien": f"https://www.leboncoin.fr/boutique/{storeId}",
            "CodeSiren": None,
            "logo": None,
            "adresse": None,
            "zone_intervention": None,
            "siteWeb": None,
            "horaires": None,
            "number": None,
            "description": None,
            "scraped": False,
            "scraped_at": None
        }
        result = await dest_collection.insert_one(agence_source)
        logger.info(f"✅ Agence {storeId} créée dans agencesFinale avec _id: {result.inserted_id}")
        return str(result.inserted_id)

    # Conserver l'_id original
    agence_source_id = agence_source["_id"]
    agence_data = agence_source.copy()
    del agence_data["_id"]  # Supprimer temporairement pour éviter conflit
    await dest_collection.update_one(
        {"storeId": storeId, "name": name},
        {"$set": agence_data},
        upsert=True
    )
    # Restaurer l'_id original
    await dest_collection.update_one(
        {"storeId": storeId, "name": name},
        {"$set": {"_id": agence_source_id}}
    )
    logger.info(f"✅ Agence {storeId} transférée dans agencesFinale avec _id original: {agence_source_id}")
    return str(agence_source_id)

async def get_or_create_agence(store_id: str, store_name: str, store_logo: Optional[str] = None) -> str:
    source_db = get_source_db()
    agences_collection = source_db["agencesBrute"]

    existing_agence = await agences_collection.find_one({"storeId": store_id, "name": store_name})
    if existing_agence:
        logger.info(f"ℹ️ Agence {store_id} ({store_name}) déjà présente dans agencesBrute avec _id: {existing_agence['_id']}")
        return str(existing_agence["_id"])

    agence_data = {
        "storeId": store_id,
        "name": store_name,
        "lien": f"https://www.leboncoin.fr/boutique/{store_id}",
        "CodeSiren": None,
        "logo": store_logo,
        "adresse": None,
        "zone_intervention": None,
        "siteWeb": None,
        "horaires": None,
        "number": None,
        "description": None,
        "scraped": False,
        "scraped_at": None
    }
    result = await agences_collection.insert_one(agence_data)
    logger.info(f"✅ Agence {store_id} créée dans agencesBrute avec _id: {result.inserted_id}")
    return str(result.inserted_id)