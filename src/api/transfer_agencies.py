from src.database.database import get_source_db, get_destination_db
from src.database.agence import AgenceModel
from datetime import datetime
from loguru import logger
import asyncio

async def rename_idAgence_to_storeId(dest_collection):
    """Renomme idAgence en storeId dans agencesFinale si storeId n'existe pas."""
    agencies_to_rename = dest_collection.find({
        "idAgence": {"$exists": True},
        "storeId": {"$exists": False}
    })
    renamed_count = 0
    async for agency in agencies_to_rename:
        store_id = agency["idAgence"]
        await dest_collection.update_one(
            {"_id": agency["_id"]},
            {
                "$set": {"storeId": store_id},
                "$unset": {"idAgence": ""}
            }
        )
        renamed_count += 1
        logger.info(f"Renommé idAgence en storeId pour l'agence {agency.get('name')} (storeId: {store_id})")
    return renamed_count

def has_filled_fields(agency):
    """Vérifie si l'agence a au moins un champ rempli parmi ceux spécifiés."""
    fields = ["description", "number", "email", "adresse"]
    return any(agency.get(field) and agency.get(field) != "Non trouvé" for field in fields)

def has_required_fields(agency):
    """Vérifie si l'agence a au moins un champ requis rempli pour être conservée dans agencesFinale."""
    fields = ["CodeSiren", "adresse", "zone_intervention", "siteWeb", "horaires", "number", "description"]
    return any(agency.get(field) and agency.get(field) not in [None, "Non trouvé"] for field in fields)

async def transfer_from_agences_to_finale(source_collection, dest_collection):
    """Transfère ou met à jour les agences de agences vers agencesFinale en conservant l'_id."""
    inserted_count = 0
    updated_count = 0
    
    async for agency in source_collection.find():
        try:
            agence_model = AgenceModel(**agency)  # Valider avec le modèle
            store_id = agence_model.storeId
            name = agence_model.name
            original_id = agency["_id"]
        except Exception as e:
            logger.warning(f"Agence invalide {agency.get('name', 'inconnu')} : {e}")
            continue
        
        # Vérifier si l'agence a des champs remplis
        if not has_filled_fields(agency):
            logger.info(f"Agence {name} (storeId: {store_id}) n'a pas de champs remplis, ignorée.")
            continue
        
        existing_in_finales = await dest_collection.find_one({"storeId": store_id})
        
        if existing_in_finales:
            # Comparer et mettre à jour les champs manquants dans agencesFinale
            update_data = {}
            fields_to_check = ["description", "number", "email", "adresse"]
            for field in fields_to_check:
                final_value = existing_in_finales.get(field)
                source_value = agency.get(field)
                if (not final_value or final_value == "Non trouvé") and source_value and source_value != "Non trouvé":
                    update_data[field] = source_value
            
            if update_data:
                update_data["scraped"] = True
                update_data["scraped_at"] = datetime.utcnow().isoformat()
                await dest_collection.update_one(
                    {"storeId": store_id},
                    {"$set": update_data}
                )
                updated_count += 1
                logger.info(f"Agence {name} (storeId: {store_id}) mise à jour avec : {update_data}")
            else:
                logger.info(f"Agence {name} (storeId: {store_id}) déjà à jour dans agencesFinale.")
        else:
            # Insérer une nouvelle agence dans agencesFinale avec l'_id original
            new_agency = agence_model.dict(exclude_unset=True)
            new_agency["_id"] = original_id  # Conserver l'_id original
            new_agency["scraped"] = True
            new_agency["scraped_at"] = datetime.utcnow().isoformat()
            try:
                await dest_collection.insert_one(new_agency)
                inserted_count += 1
                logger.info(f"Agence {name} (storeId: {store_id}) insérée dans agencesFinale avec _id: {original_id}.")
            except Exception as e:
                logger.error(f"Erreur lors de l'insertion de {name} (storeId: {store_id}) : {e}")
    
    return inserted_count, updated_count

async def cleanup_agences_finale(dest_collection, source_collection, brute_collection):
    """Compare agencesFinale avec agencesBrute et nettoie les agences non valides."""
    updated_count = 0
    moved_to_brute_count = 0
    
    async for agency in dest_collection.find():
        store_id = agency.get("storeId")
        name = agency.get("name", "Inconnu")
        
        # Vérifier si l'agence a des champs requis
        if not has_required_fields(agency):
            # Vérifier dans agences et agencesBrute pour des données supplémentaires
            agency_in_agences = await source_collection.find_one({"storeId": store_id})
            agency_in_brute = await brute_collection.find_one({"storeId": store_id})
            
            update_data = {}
            fields_to_check = ["CodeSiren", "adresse", "zone_intervention", "siteWeb", "horaires", "number", "description"]
            
            for field in fields_to_check:
                if agency_in_agences and agency_in_agences.get(field) and agency_in_agences.get(field) != "Non trouvé":
                    update_data[field] = agency_in_agences[field]
                elif agency_in_brute and agency_in_brute.get(field) and agency_in_brute.get(field) != "Non trouvé":
                    update_data[field] = agency_in_brute[field]
            
            if update_data:
                update_data["scraped"] = True
                update_data["scraped_at"] = datetime.utcnow().isoformat()
                await dest_collection.update_one(
                    {"storeId": store_id},
                    {"$set": update_data}
                )
                updated_count += 1
                logger.info(f"Agence {name} (storeId: {store_id}) mise à jour dans agencesFinale avec : {update_data}")
            else:
                # Supprimer de agencesFinale et déplacer vers agencesBrute
                await dest_collection.delete_one({"storeId": store_id})
                brute_agency = {
                    "storeId": store_id,
                    "name": name,
                    "scraped": False,
                    "scraped_at": None
                }
                await brute_collection.update_one(
                    {"storeId": store_id},
                    {"$set": brute_agency},
                    upsert=True
                )
                moved_to_brute_count += 1
                logger.info(f"Agence {name} (storeId: {store_id}) déplacée vers agencesBrute (non scrapée).")
        else:
            logger.info(f"Agence {name} (storeId: {store_id}) conserve dans agencesFinale (champs suffisants).")
    
    return updated_count, moved_to_brute_count

async def transfer_agencies(queue=None):
    """Exécute le transfert et le nettoyage des agences."""
    try:
        # Récupérer les collections depuis database.py
        source_db = get_source_db()
        dest_db = get_destination_db()
        agences_collection = source_db["agences"]
        agences_finales_collection = dest_db["agencesFinale"]
        agences_brute_collection = source_db["agencesBrute"]

        # Étape 1 : Renommer idAgence en storeId
        renamed_count = await rename_idAgence_to_storeId(agences_finales_collection)
        
        # Étape 2 : Transférer de agences vers agencesFinale
        inserted_count, updated_from_agences_count = await transfer_from_agences_to_finale(
            agences_collection, agences_finales_collection
        )
        
        # Étape 3 : Nettoyer agencesFinale et comparer avec agencesBrute
        updated_from_cleanup_count, moved_to_brute_count = await cleanup_agences_finale(
            agences_finales_collection, agences_collection, agences_brute_collection
        )
        
        result = {
            "status": "success",
            "renamed": renamed_count,
            "inserted": inserted_count,
            "updated_from_agences": updated_from_agences_count,
            "updated_from_cleanup": updated_from_cleanup_count,
            "moved_to_brute": moved_to_brute_count,
            "message": "Transfert et nettoyage terminés avec succès."
        }
        if queue:
            queue.put(result)
        logger.info(f"✅ Transfert terminé : {result}")
        return result
    except Exception as e:
        result = {
            "status": "error",
            "message": f"Erreur lors du transfert : {str(e)}"
        }
        if queue:
            queue.put(result)
        logger.error(f"⚠️ Erreur lors du transfert : {e}")
        return result

if __name__ == "__main__":
    asyncio.run(transfer_agencies())