from pydantic import BaseModel
from typing import Optional
from loguru import logger
from src.database.database import get_source_db, get_destination_db
from datetime import datetime

BLACKLISTED_STORE_IDS = {"5608823"}

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
    scraped: Optional[bool] = False
    scraped_at: Optional[datetime] = None

def calculate_completeness(data: dict) -> int:
    """Calcule un score de complétude basé sur les champs remplis."""
    fields = ["CodeSiren", "logo", "adresse", "zone_intervention", "siteWeb", "horaires", "number", "description"]
    return sum(1 for field in fields if data.get(field) not in [None, "Non trouvé", ""])

async def transfer_agence(storeId: str, name: Optional[str] = None) -> Optional[str]:
    source_db = get_source_db()
    dest_db = get_destination_db()
    source_collection = source_db["agencesBrute"]
    dest_collection = dest_db["agencesFinale"]

    if storeId in BLACKLISTED_STORE_IDS:
        logger.info(f"⏭ Agence {storeId} ignorée (dans la liste noire).")
        return None

    agence_source = await source_collection.find_one({"storeId": storeId, "name": name})
    if not agence_source:
        logger.warning(f"⚠️ Agence {storeId} non trouvée dans agencesBrute. Création dans agencesBrute.")
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
        result = await source_collection.insert_one(agence_source)
        agence_source["_id"] = result.inserted_id
        logger.info(f"✅ Agence {storeId} créée dans agencesBrute avec _id: {result.inserted_id}")

    agence_data = agence_source.copy()
    agence_source_id = agence_data.pop("_id", None)

    # Utiliser update_one avec upsert pour éviter les doublons
    result = await dest_collection.update_one(
        {"storeId": storeId},
        {"$set": agence_data},
        upsert=True
    )
    if result.matched_count > 0:
        logger.info(f"✅ Agence {storeId} mise à jour dans agencesFinale.")
    else:
        await dest_collection.update_one({"storeId": storeId}, {"$set": {"_id": agence_source_id}})
        logger.info(f"✅ Agence {storeId} insérée dans agencesFinale avec _id: {agence_source_id}")
    return str(agence_source_id)

async def get_or_create_agence(store_id: str, store_name: str, store_logo: Optional[str] = None) -> str:
    source_db = get_source_db()
    agences_collection = source_db["agencesBrute"]
    
    if store_id in BLACKLISTED_STORE_IDS:
        logger.info(f"⏭ Agence {store_id} ignorée (dans la liste noire).")
        return None

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