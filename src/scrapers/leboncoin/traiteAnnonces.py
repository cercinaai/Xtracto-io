import asyncio
from src.database.database import init_db, close_db, get_db
from src.database.realState import RealState, annonce_exists, save_annonce_to_db
from src.database.agence import transfer_agence
from loguru import logger

async def traite_annonces():
    await init_db()
    source_db = get_db()
    
    # Récupérer les annonces brutes avec storeId et agenceName
    query = {
        "storeId": {"$exists": True},
        "agenceName": {"$exists": True}
    }
    annonces = await source_db["realState"].find(query).to_list(length=None)
    total_annonces = len(annonces)
    logger.info(f"📤 {total_annonces} annonces avec storeId et agenceName à traiter")

    if total_annonces == 0:
        logger.info("✅ Aucune annonce à traiter.")
        await close_db()
        return {"processed": 0}

    processed_count = 0

    # Collections cibles
    agences_collection = source_db["agencesBrute"]
    annonces_with_agence_collection = source_db["realStateWithAgence"]

    for annonce in annonces:
        try:
            annonce_id = annonce["idSec"]
            store_id = annonce.get("storeId")
            agence_name = annonce.get("agenceName")
            store_logo = annonce.get("store_logo", None)

            # Créer ou vérifier l'agence dans agencesBrute
            agence_data = {
                "storeId": store_id,
                "name": agence_name,
                "lien": f"https://www.leboncoin.fr/boutique/{store_id}",
                "logo": store_logo
            }
            existing_agence = await agences_collection.find_one({"storeId": store_id})
            if not existing_agence:
                result = await agences_collection.insert_one(agence_data)
                agence_id = str(result.inserted_id)
                logger.info(f"✅ Agence {store_id} ajoutée à agencesBrute avec _id: {agence_id}")
            else:
                agence_id = str(existing_agence["_id"])
                logger.info(f"ℹ️ Agence {store_id} déjà présente dans agencesBrute")

            # Ajouter l'idAgence à l'annonce et la transférer
            annonce["idAgence"] = agence_id
            if not await annonces_with_agence_collection.find_one({"idSec": annonce_id}):
                await annonces_with_agence_collection.insert_one(annonce)
                logger.info(f"✅ Annonce {annonce_id} transférée à realStateWithAgence")
                processed_count += 1
            else:
                logger.info(f"ℹ️ Annonce {annonce_id} déjà présente dans realStateWithAgence")

        except Exception as e:
            logger.error(f"⚠️ Erreur pour annonce {annonce_id} : {e}")

    await close_db()
    logger.info(f"✅ Traitement terminé : {processed_count} annonces transférées")
    return {"processed": processed_count}

if __name__ == "__main__":
    asyncio.run(traite_annonces())