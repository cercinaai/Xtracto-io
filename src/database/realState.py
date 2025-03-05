from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict
from datetime import datetime
from loguru import logger
from src.database.database import get_source_db, get_destination_db

class RealStateLBCModel(BaseModel):
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

    @validator("publication_date", "index_date", "expiration_date", pre=True, always=True)
    def parse_date(cls, v):
        if not v or v == "":
            return None
        try:
            return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    class Config:
        extra = "ignore"

async def save_annonce_to_db(annonce: RealStateLBCModel) -> bool:
    db = get_source_db()
    collection = db["realStateLbc"]
    if await collection.find_one({"idSec": annonce.idSec}):
        logger.info(f"ℹ️ Annonce {annonce.idSec} déjà existante dans la base source")
        return False
    annonce_dict = annonce.dict(exclude_unset=True)
    result = await collection.insert_one(annonce_dict)
    logger.info(f"✅ Annonce {annonce.idSec} enregistrée dans la base source avec _id: {result.inserted_id}")
    return True

async def annonce_exists(annonce_id: str) -> bool:
    db = get_source_db()
    collection = db["realStateLbc"]
    return await collection.find_one({"idSec": annonce_id}) is not None

async def update_annonce_images(annonce_id: str, images: List[str], nbrImages: int) -> bool:
    db = get_source_db()
    collection = db["realStateLbc"]
    result = await collection.update_one(
        {"idSec": annonce_id},
        {"$set": {"images": images, "nbrImages": nbrImages, "scraped_at": datetime.utcnow()}}
    )
    if result.modified_count > 0:
        logger.info(f"✅ Images de l'annonce {annonce_id} mises à jour")
        return True
    logger.warning(f"⚠️ Aucune mise à jour effectuée pour l'annonce {annonce_id}")
    return False

async def transfer_annonce(annonce: Dict) -> bool:
    """Transfère une annonce sans supprimer la source, met à jour les attributs manquants si elle existe."""
    source_db = get_source_db()
    dest_db = get_destination_db()
    source_collection = source_db["realStateLbc"]
    dest_collection = dest_db["realStateLbc"]

    existing = await dest_collection.find_one({"idSec": annonce["idSec"]})
    if existing:
        update_data = {}
        for key, value in annonce.items():
            if key not in existing or existing[key] is None:
                update_data[key] = value
        if update_data:
            result = await dest_collection.update_one(
                {"idSec": annonce["idSec"]},
                {"$set": update_data}
            )
            if result.modified_count > 0:
                logger.info(f"✅ Annonce {annonce['idSec']} mise à jour avec attributs manquants")
                return True
        logger.info(f"ℹ️ Annonce {annonce['idSec']} déjà complète dans la destination")
        return False
    else:
        await dest_collection.insert_one(annonce)
        logger.info(f"✅ Annonce {annonce['idSec']} transférée vers la base destination")
        return True
