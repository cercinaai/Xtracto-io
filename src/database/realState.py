from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict
from datetime import datetime
from src.database.database import get_source_db, get_destination_db
from src.utils.b2_utils import upload_image_to_b2
import asyncio
import aiohttp
from loguru import logger
from bson.objectid import ObjectId

BLACKLISTED_STORE_IDS = {"5608823"}

class RealState(BaseModel):
    idSec: str
    publication_date: Optional[datetime] = None
    index_date: Optional[datetime] = None
    expiration_date: Optional[datetime] = None
    status: Optional[str] = None
    ad_type: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    body: Optional[str] = Field(None, max_length=100000)
    url: Optional[str] = None
    category_id: Optional[str] = None
    category_name: Optional[str] = None
    price: Optional[float] = None
    nbrImages: Optional[int] = None
    images: Optional[List[str]] = None
    typeBien: Optional[str] = None
    meuble: Optional[str] = None
    surface: Optional[str] = None
    nombreDepiece: Optional[str] = None
    nombreChambres: Optional[str] = None
    nombreSalleEau: Optional[str] = None
    nb_salles_de_bain: Optional[str] = None
    nb_parkings: Optional[str] = None
    nb_niveaux: Optional[str] = None
    disponibilite: Optional[str] = None
    annee_construction: Optional[str] = None
    classeEnergie: Optional[str] = None
    ges: Optional[str] = None
    ascenseur: Optional[str] = None
    etage: Optional[str] = None
    nombreEtages: Optional[str] = None
    exterieur: Optional[List[str]] = None
    charges_incluses: Optional[str] = None
    depot_garantie: Optional[str] = None
    loyer_mensuel_charges: Optional[str] = None
    caracteristiques: Optional[List[str]] = None
    region: Optional[str] = None
    city: Optional[str] = None
    zipcode: Optional[str] = None
    departement: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    region_id: Optional[str] = None
    departement_id: Optional[str] = None
    store_name: Optional[str] = None
    storeId: Optional[str] = None
    idAgence: Optional[str] = None
    agenceName: Optional[str] = None
    scraped_at: Optional[datetime] = None
    processed: Optional[bool] = None
    processed_at: Optional[datetime] = None

    @validator("publication_date", "index_date", "expiration_date", "scraped_at", "processed_at", pre=True, always=True)
    def parse_date(cls, v):
        if not v or v == "":
            return None
        try:
            return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    class Config:
        extra = "ignore"

async def save_annonce_to_db(annonce: RealState) -> bool:
    db = get_source_db()
    collection = db["realState"]
    if await annonce_exists_by_unique_key(annonce.idSec, annonce.title, annonce.price):
        return False
    annonce_dict = annonce.dict(exclude_unset=True)
    result = await collection.insert_one(annonce_dict)
    return True

async def annonce_exists(annonce_id: str) -> bool:
    db = get_source_db()
    collection = db["realState"]
    return await collection.find_one({"idSec": annonce_id}) is not None

async def annonce_exists_by_unique_key(idSec: str, title: str, price: float) -> bool:
    db = get_source_db()
    collection = db["realState"]
    query = {"idSec": idSec, "title": title, "price": price}
    return await collection.find_one(query) is not None

async def update_annonce_images(annonce_id: str, images: List[str], nbrImages: int) -> bool:
    db = get_source_db()
    collection = db["realState"]
    result = await collection.update_one(
        {"idSec": annonce_id},
        {"$set": {"images": images, "nbrImages": nbrImages, "scraped_at": datetime.utcnow()}}
    )
    return result.modified_count > 0

async def transfer_annonce(annonce: Dict) -> bool:
    source_db = get_source_db()
    dest_db = get_destination_db()
    source_collection = source_db["realState"]
    dest_collection = dest_db["realState"]
    unique_key_query = {"idSec": annonce["idSec"], "title": annonce.get("title"), "price": annonce.get("price")}
    result = await dest_collection.update_one(unique_key_query, {"$set": annonce}, upsert=True)
    return result.modified_count > 0 or result.upserted_id is not None

async def check_image_url(url: str) -> bool:
    """Check if an image URL is accessible."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.head(url, timeout=5) as response:
                return response.status == 200
        except Exception:
            return False

async def transfer_from_withagence_to_finale(annonce: Dict) -> Dict:
    source_db = get_source_db()
    dest_db = get_destination_db()
    annonce_id = annonce["idSec"]
    dest_collection = dest_db["realStateFinale"]

    if annonce.get("storeId") in BLACKLISTED_STORE_IDS:
        logger.info(f"⏭ Annonce {annonce_id} ignorée (storeId dans la liste noire).")
        return {"idSec": annonce_id, "images": annonce.get("images", []), "skipped": True}

    unique_key_query = {
        "idSec": annonce["idSec"],
        "title": annonce.get("title"),
        "price": annonce.get("price")
    }

    # Gestion des images
    image_urls = annonce.get("images", [])
    if not image_urls or all(url == "N/A" for url in image_urls):
        logger.debug(f"Annonce {annonce_id} n'a pas d'images valides.")
        return {"idSec": annonce_id, "images": image_urls or [], "skipped": True}

    tasks = [
        upload_image_to_b2(url, f"{annonce_id}_{idx}.jpg") if url != "N/A" and not url.startswith("https://f003.backblazeb2.com") 
        else asyncio.sleep(0, result=url) 
        for idx, url in enumerate(image_urls)
    ]
    updated_image_urls = await asyncio.gather(*tasks, return_exceptions=True)
    final_urls = [url if isinstance(result, Exception) or result == "N/A" else result for url, result in zip(image_urls, updated_image_urls)]

    # Gestion de l'agence
    id_agence = annonce.get("idAgence")
    agence_name = annonce.get("agenceName") or annonce.get("store_name")
    final_id_agence = id_agence

    if id_agence:
        try:
            object_id_agence = ObjectId(id_agence)
        except Exception:
            object_id_agence = id_agence

        agence_exists = await dest_db["agencesFinale"].find_one({"_id": object_id_agence})
        if not agence_exists:
            agence_exists_in_brute = await source_db["agencesBrute"].find_one({"_id": object_id_agence})
            if not agence_exists_in_brute:
                if agence_name and annonce.get("storeId"):
                    agence_data = {
                        "storeId": annonce.get("storeId"),
                        "name": agence_name,
                        "lien": f"https://www.leboncoin.fr/boutique/{annonce.get('storeId')}",
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
                    result = await source_db["agencesBrute"].insert_one(agence_data)
                    final_id_agence = str(result.inserted_id)
                    logger.info(f"✅ Nouvelle agence créée dans agencesBrute pour {annonce_id} avec _id: {final_id_agence}")
                else:
                    logger.warning(f"⚠️ Pas assez d'infos (storeId ou name) pour créer une agence pour {annonce_id}.")
                    return {"idSec": annonce_id, "images": final_urls, "skipped": True}
            else:
                await dest_db["agencesFinale"].update_one(
                    {"_id": object_id_agence},
                    {"$set": agence_exists_in_brute},
                    upsert=True
                )
                logger.info(f"✅ Agence {id_agence} transférée de agencesBrute à agencesFinale.")
    else:
        logger.debug(f"Annonce {annonce_id} n'a pas d'idAgence.")
        return {"idSec": annonce_id, "images": final_urls, "skipped": True}

    # Préparer les données à transférer
    annonce_to_transfer = annonce.copy()
    annonce_to_transfer["idAgence"] = final_id_agence
    annonce_to_transfer["images"] = final_urls
    annonce_to_transfer["nbrImages"] = len([url for url in final_urls if url != "N/A"])
    annonce_to_transfer["scraped_at"] = datetime.utcnow()
    annonce_to_transfer["processed"] = True
    annonce_to_transfer["processed_at"] = datetime.utcnow()

    # Utiliser update_one avec upsert pour éviter les doublons
    result = await dest_collection.update_one(
        unique_key_query,
        {"$set": annonce_to_transfer},
        upsert=True
    )
    if result.matched_count > 0:
        logger.info(f"✅ Annonce {annonce_id} mise à jour dans realStateFinale avec idAgence {final_id_agence}.")
    else:
        logger.info(f"✅ Annonce {annonce_id} insérée dans realStateFinale avec idAgence {final_id_agence}.")

    return {"idSec": annonce_id, "images": final_urls, "skipped": False}

async def run_transfer_loop():
    source_db = get_source_db()
    dest_db = get_destination_db()
    withagence_collection = source_db["realStateWithAgence"]
    while True:
        try:
            finale_ids = await dest_db["realStateFinale"].distinct("idSec")
            annonces = await withagence_collection.find({
                "idSec": {"$nin": finale_ids},
                "processed": {"$ne": True}
            }).to_list(length=None)
            total_annonces = len(annonces)
            logger.info(f"📊 Nombre total d'annonces à transférer : {total_annonces}")

            if total_annonces == 0:
                logger.info("ℹ️ Aucune annonce à transférer dans realStateWithAgence.")
                await asyncio.sleep(60)
                continue

            for annonce in annonces:
                await transfer_from_withagence_to_finale(annonce)

            await asyncio.sleep(60)
        except Exception as e:
            logger.error(f"⚠️ Erreur lors du transfert : {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(run_transfer_loop())