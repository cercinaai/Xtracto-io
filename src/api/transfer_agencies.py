from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

# Connexion à MongoDB sur le serveur distant
uri = "mongodb://admin:SuperSecureP%40ssw0rd!@localhost:27017/xtracto-io-prod?authSource=admin"
client = MongoClient(uri)
db = client["xtracto-io-prod"]
agences_collection = db["agences"]
agences_finales_collection = db["agencesFinale"]
agences_brute_collection = db["agencesBrute"]

def rename_idAgence_to_storeId():
    """Renomme idAgence en storeId dans agencesFinale si storeId n'existe pas."""
    agencies_to_rename = agences_finales_collection.find({
        "idAgence": {"$exists": True},
        "storeId": {"$exists": False}
    })
    renamed_count = 0
    for agency in agencies_to_rename:
        store_id = agency["idAgence"]
        agences_finales_collection.update_one(
            {"_id": agency["_id"]},
            {
                "$set": {"storeId": store_id},
                "$unset": {"idAgence": ""}
            }
        )
        renamed_count += 1
        print(f"Renommé idAgence en storeId pour l'agence {agency.get('name')} (storeId: {store_id})")
    return renamed_count

def has_filled_fields(agency):
    """Vérifie si l'agence a au moins un champ rempli parmi ceux spécifiés."""
    fields = ["description", "number", "email", "adresse"]
    return any(agency.get(field) and agency.get(field) != "Non trouvé" for field in fields)

def has_required_fields(agency):
    """Vérifie si l'agence a au moins un champ requis rempli pour être conservée dans agencesFinale."""
    fields = ["CodeSiren", "adresse", "zone_intervention", "siteWeb", "horaires", "number", "description"]
    return any(agency.get(field) and agency.get(field) not in [None, "Non trouvé"] for field in fields)

def transfer_from_agences_to_finale():
    """Transfère ou met à jour les agences de agences vers agencesFinale en conservant l'_id."""
    inserted_count = 0
    updated_count = 0
    
    for agency in agences_collection.find():
        store_id = agency.get("storeId")
        name = agency.get("name")
        original_id = agency.get("_id")
        
        # Vérifier si l'agence a des champs remplis
        if not has_filled_fields(agency):
            print(f"Agence {name} (storeId: {store_id}) n'a pas de champs remplis, ignorée.")
            continue
        
        existing_in_finales = agences_finales_collection.find_one({"storeId": store_id})
        
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
                agences_finales_collection.update_one(
                    {"storeId": store_id},
                    {"$set": update_data}
                )
                updated_count += 1
                print(f"Agence {name} (storeId: {store_id}) mise à jour avec : {update_data}")
            else:
                print(f"Agence {name} (storeId: {store_id}) déjà à jour dans agencesFinale.")
        else:
            # Insérer une nouvelle agence dans agencesFinale avec l'_id original
            new_agency = {
                "_id": original_id,  # Conserver l'_id original
                "storeId": store_id,
                "name": name,
                "lien": agency.get("lien", None),
                "CodeSiren": agency.get("CodeSiren", None),
                "logo": agency.get("logo", None),
                "adresse": agency.get("adresse", None),
                "zone_intervention": agency.get("zone_intervention", None),
                "siteWeb": agency.get("siteWeb", None),
                "horaires": agency.get("horaires", None),
                "number": agency.get("number", None),
                "description": agency.get("description", None),
                "email": agency.get("email", None),
                "scraped": True,
                "scraped_at": datetime.utcnow().isoformat()
            }
            try:
                agences_finales_collection.insert_one(new_agency)
                inserted_count += 1
                print(f"Agence {name} (storeId: {store_id}) insérée dans agencesFinale avec _id: {original_id}.")
            except Exception as e:
                print(f"Erreur lors de l'insertion de {name} (storeId: {store_id}) : {e}")
    
    return inserted_count, updated_count

def cleanup_agences_finale():
    """Compare agencesFinale avec agencesBrute et nettoie les agences non valides."""
    updated_count = 0
    moved_to_brute_count = 0
    
    for agency in agences_finales_collection.find():
        store_id = agency.get("storeId")
        name = agency.get("name", "Inconnu")
        
        # Vérifier si l'agence a des champs requis
        if not has_required_fields(agency):
            # Vérifier dans agences et agencesBrute pour des données supplémentaires
            agency_in_agences = agences_collection.find_one({"storeId": store_id})
            agency_in_brute = agences_brute_collection.find_one({"storeId": store_id})
            
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
                agences_finales_collection.update_one(
                    {"storeId": store_id},
                    {"$set": update_data}
                )
                updated_count += 1
                print(f"Agence {name} (storeId: {store_id}) mise à jour dans agencesFinale avec : {update_data}")
            else:
                # Supprimer de agencesFinale et déplacer vers agencesBrute avec un nouvel _id
                agences_finales_collection.delete_one({"storeId": store_id})
                brute_agency = {
                    "storeId": store_id,
                    "name": name,
                    "scraped": False,
                    "scraped_at": None
                }
                agences_brute_collection.update_one(
                    {"storeId": store_id},
                    {"$set": brute_agency},
                    upsert=True
                )
                moved_to_brute_count += 1
                print(f"Agence {name} (storeId: {store_id}) déplacée vers agencesBrute (non scrapée).")
        else:
            print(f"Agence {name} (storeId: {store_id}) conserve dans agencesFinale (champs suffisants).")
    
    return updated_count, moved_to_brute_count

def transfer_agencies():
    """Exécute le transfert et le nettoyage des agences."""
    try:
        # Étape 1 : Renommer idAgence en storeId
        renamed_count = rename_idAgence_to_storeId()
        
        # Étape 2 : Transférer de agences vers agencesFinale
        inserted_count, updated_from_agences_count = transfer_from_agences_to_finale()
        
        # Étape 3 : Nettoyer agencesFinale et comparer avec agencesBrute
        updated_from_cleanup_count, moved_to_brute_count = cleanup_agences_finale()
        
        return {
            "status": "success",
            "renamed": renamed_count,
            "inserted": inserted_count,
            "updated_from_agences": updated_from_agences_count,
            "updated_from_cleanup": updated_from_cleanup_count,
            "moved_to_brute": moved_to_brute_count,
            "message": "Transfert et nettoyage terminés avec succès."
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Erreur lors du transfert : {str(e)}"
        }
    finally:
        client.close()

if __name__ == "__main__":
    result = transfer_agencies()
    print(result)