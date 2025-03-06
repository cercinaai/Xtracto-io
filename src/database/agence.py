from pydantic import BaseModel
from typing import Optional
from loguru import logger
from src.database.database import get_source_db, get_destination_db

class AgenceModel(BaseModel):
    storeId: str
    name: str
    lien: str
    logo: Optional[str] = None
    adresse: Optional[str] = None
    zone_intervention: Optional[str] = None
    siteWeb: Optional[str] = None
    horaires: Optional[str] = None
    number: Optional[str] = None
    description: Optional[str] = None

async def transfer_agence(storeId: str, name: Optional[str] = None) -> bool:
    """Transfère une agence complète depuis la source si elle n'existe pas dans la destination, sans mise à jour si existante."""
    source_db = get_source_db()
    dest_db = get_destination_db()
    source_collection = source_db["agences"]
    dest_collection = dest_db["agences"]

    # Vérifier si l'agence existe déjà dans la base destination
    existing = await dest_collection.find_one({"storeId": storeId})
    if existing:
        logger.info(f"ℹ️ Agence {storeId} déjà présente dans la base destination, aucune action effectuée.")
        return False

    # Récupérer l'agence complète depuis la base source
    agence_source = await source_collection.find_one({"_id": storeId}) or \
                    await source_collection.find_one({"storeId": storeId})

    if not agence_source:
        logger.warning(f"⚠️ Agence {storeId} non trouvée dans la base source. Création d'une agence minimale.")
        agence_source = {
            "storeId": storeId,
            "name": name if name else f"Agence {storeId}",
            "lien": f"https://www.leboncoin.fr/boutique/{storeId}"
        }

    # S'assurer que storeId est cohérent
    agence_source["storeId"] = storeId

    # Insérer l'agence complète dans la destination
    await dest_collection.insert_one(agence_source)
    logger.info(f"✅ Agence {storeId} transférée avec tous les attributs vers la base destination.")
    return True