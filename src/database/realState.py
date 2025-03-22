from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict
from datetime import datetime
from src.database.database import get_source_db, get_destination_db
from src.utils.b2_utils import upload_image_to_b2
from urllib.parse import urlparse
import asyncio
import aiohttp
from loguru import logger

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
    existing = await dest_collection.find_one({"idSec": annonce["idSec"], "title": annonce["title"], "price": annonce["price"]})
    if existing:
        update_data = {}
        for key, value in annonce.items():
            if key != "_id" and (key not in existing or existing[key] is None):
                update_data[key] = value
        if update_data:
            result = await dest_collection.update_one(
                {"idSec": annonce["idSec"], "title": annonce["title"], "price": annonce["price"]},
                {"$set": update_data}
            )
            if result.modified_count > 0:
                return True
        return False
    else:
        await dest_collection.insert_one(annonce)
        return True

async def check_image_url(url: str) -> bool:
    """Check if an image URL is accessible."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.head(url, timeout=5) as response:
                return response.status == 200
        except Exception:
            return False

async def transfer_from_withagence_to_finale(annonce: Dict) -> Dict:
    """
    Transfer an annonce from realStateWithAgence to realStateFinale, processing images if not uploaded to Backblaze.
    Only transfers if all valid images are successfully uploaded.

    Args:
        annonce (Dict): The annonce document to transfer.

    Returns:
        Dict: A dictionary containing the annonce ID, updated image URLs, and skipped status.
              - "skipped": True if already in realStateFinale or image processing fails.
              - "skipped": False if successfully transferred.
    """
    source_db = get_source_db()
    dest_db = get_destination_db()
    annonce_id = annonce["idSec"]

    # Check for duplicates in realStateFinale based solely on idSec
    dest_collection = dest_db["realStateFinale"]
    existing = await dest_collection.find_one({"idSec": annonce_id})
    if existing:
        logger.info(f"ℹ️ Annonce {annonce_id} déjà présente dans realStateFinale, mise à jour si nécessaire.")
        update_data = {}
        for key, value in annonce.items():
            if key != "_id" and (key not in existing or existing[key] != value):
                update_data[key] = value
        if update_data:
            await dest_collection.update_one({"idSec": annonce_id}, {"$set": update_data})
        return {"idSec": annonce_id, "images": annonce.get("images", []), "skipped": True}

    image_urls = annonce.get("images", [])
    if not image_urls or all(url == "N/A" for url in image_urls):
        logger.info(f"ℹ️ Annonce {annonce_id} n'a pas d'images valides à traiter, pas de transfert.")
        return {"idSec": annonce_id, "images": image_urls or [], "skipped": True}

    # Process images without modifying the original annonce yet
    updated_image_urls = []
    all_images_successful = True

    for url in image_urls:
        if url == "N/A" or url.startswith("https://f003.backblazeb2.com"):
            updated_image_urls.append(url)
            continue

        if not await check_image_url(url):
            logger.warning(f"⚠️ Image inaccessible pour {annonce_id}: {url}")
            updated_image_urls.append(url)  # Garder l'original
            all_images_successful = False
            continue

        try:
            file_name = "".join(c if c.isalnum() or c in "-_." else "_" for c in urlparse(url).path.split('/')[-1] or "default.jpg")
            uploaded_url = await upload_image_to_b2(url, file_name)
            if uploaded_url == "N/A":
                logger.warning(f"⚠️ Échec upload image {url} pour {annonce_id}: URL invalide après tentative")
                updated_image_urls.append(url)  # Garder l'original
                all_images_successful = False
            else:
                updated_image_urls.append(uploaded_url)
        except Exception as e:
            logger.warning(f"⚠️ Échec upload image {url} pour {annonce_id}: {e}")
            updated_image_urls.append(url)  # Garder l'original
            all_images_successful = False

    # Si toutes les images valides n'ont pas été téléchargées avec succès, ne pas transférer
    if not all_images_successful:
        logger.info(f"ℹ️ Annonce {annonce_id} non transférée : échec du traitement d'au moins une image.")
        return {"idSec": annonce_id, "images": image_urls, "skipped": True}

    # Si on arrive ici, toutes les images valides ont été téléchargées avec succès
    annonce_to_transfer = annonce.copy()
    annonce_to_transfer["images"] = updated_image_urls
    annonce_to_transfer["nbrImages"] = len(updated_image_urls)
    annonce_to_transfer["scraped_at"] = datetime.utcnow()
    annonce_to_transfer["processed"] = True
    annonce_to_transfer["processed_at"] = datetime.utcnow()

    if "_id" in annonce_to_transfer:
        del annonce_to_transfer["_id"]  # Supprimer _id pour éviter conflit

    # Transférer vers realStateFinale
    await dest_collection.insert_one(annonce_to_transfer)
    logger.info(f"✅ Annonce {annonce_id} transférée avec succès vers realStateFinale.")

    return {"idSec": annonce_id, "images": updated_image_urls, "skipped": False}