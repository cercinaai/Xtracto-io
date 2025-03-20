from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict
from datetime import datetime
from src.database.database import get_source_db, get_destination_db
from src.utils.b2_utils import upload_image_to_b2
from urllib.parse import urlparse
import asyncio

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
    processed: Optional[bool] = None  # Add processed field
    processed_at: Optional[datetime] = None  # Add processed_at field

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

async def transfer_from_withagence_to_finale(annonce: Dict) -> Dict:
    """
    Transfer an annonce from realStateWithAgence to realStateFinale, processing images if not uploaded to Backblaze.
    
    Args:
        annonce (Dict): The annonce document to transfer.
    
    Returns:
        Dict: A dictionary containing the annonce ID and updated image URLs, or None if processing fails.
    """
    source_db = get_source_db()
    dest_db = get_destination_db()
    annonce_id = annonce["idSec"]
    image_urls = annonce.get("images", [])

    # Check if images are valid (not all "N/A")
    if not image_urls or all(url == "N/A" for url in image_urls):
        # Skip this annonce as it has no valid images to process
        return {"idSec": annonce_id, "images": image_urls, "skipped": True}

    # Step 1: Process images if not already uploaded to Backblaze
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

    # Use original URLs if all uploads fail
    if failed_uploads == len(uploaded_urls) and upload_tasks:
        updated_image_urls = image_urls  # Use original URLs if all uploads fail
    else:
        # Replace failed uploads with original URLs
        updated_image_urls = [
            updated_url if updated_url != "N/A" else original_url
            for updated_url, original_url in zip(updated_image_urls, image_urls)
        ]

    # Step 2: Update the annonce with new image URLs
    annonce["images"] = updated_image_urls
    annonce["nbrImages"] = len(updated_image_urls)
    annonce["scraped_at"] = datetime.utcnow()

    # Step 3: Transfer the annonce to realStateFinale
    dest_collection = dest_db["realStateFinale"]
    existing = await dest_collection.find_one({"idSec": annonce["idSec"]})
    if existing:
        # If the annonce already exists, update it with new attributes
        update_data = {k: v for k, v in annonce.items() if k != "_id"}
        await dest_collection.update_one(
            {"idSec": annonce["idSec"]},
            {"$set": update_data}
        )
    else:
        # Ensure all attributes are copied, including _id
        annonce_to_transfer = annonce.copy()
        if "_id" in annonce_to_transfer:
            annonce_to_transfer["_id"] = annonce["_id"]  # Preserve the _id
        await dest_collection.insert_one(annonce_to_transfer)

    # Step 4: Update the annonce in realStateWithAgence with new image URLs
    await source_db["realStateWithAgence"].update_one(
        {"idSec": annonce_id},
        {"$set": {"images": updated_image_urls, "nbrImages": len(updated_image_urls), "scraped_at": datetime.utcnow()}}
    )

    return {"idSec": annonce_id, "images": updated_image_urls}