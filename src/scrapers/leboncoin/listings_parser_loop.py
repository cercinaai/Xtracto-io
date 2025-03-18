import logging
from src.captcha.captchaSolver import solve_audio_captcha
from playwright.async_api import Page, TimeoutError, Response
from src.scrapers.leboncoin.utils.human_behavorScrapperLbc import (
    human_like_click_search, human_like_scroll_to_element, human_like_delay,
    human_like_exploration, simulate_reading
)
from src.database.realState import RealState, annonce_exists_by_unique_key, save_annonce_to_db
from src.database.agence import get_or_create_agence
from src.database.database import get_source_db
from datetime import datetime
import random
import asyncio
from loguru import logger

TARGET_API_URL = "https://api.leboncoin.fr/finder/search"
total_scraped = 0

async def check_and_solve_captcha(page: Page, action: str) -> bool:
    captcha_selector = 'iframe[title="DataDome CAPTCHA"]'
    if await page.locator(captcha_selector).is_visible(timeout=3000):
        logger.warning(f"⚠️ CAPTCHA détecté avant {action}.")
        if not await solve_audio_captcha(page):
            logger.error(f"❌ Échec de la résolution du CAPTCHA avant {action}.")
            return False
        logger.info(f"✅ CAPTCHA résolu avant {action}.")
    return True

async def get_attr_by_label(ad: dict, label: str, default=None, get_values: bool = False) -> str | None:
    for attr in ad.get("attributes", []):
        attr_label = attr.get("key_label") or attr.get("key", "")
        if attr_label.strip() == label:
            return attr.get("values_label" if get_values else "value_label", default) or default
    return default

async def process_ad(ad: dict) -> bool:
    global total_scraped
    annonce_id = str(ad.get("list_id"))
    title = ad.get("subject")
    price = ad.get("price", [None])[0] if isinstance(ad.get("price"), list) else ad.get("price")

    # Vérifier l'existence avec idSec, title et price
    if await annonce_exists_by_unique_key(annonce_id, title, price):
        logger.info(f"⏭ Annonce {annonce_id} déjà existante (idSec, title, price).")
        return True  # Retourne True si l'annonce existe déjà

    logger.debug(f"📋 Traitement de l'annonce {annonce_id}...")
    store_name = await get_attr_by_label(ad, "store_name")
    storeId = await get_attr_by_label(ad, "online_store_id")
    store_logo = await get_attr_by_label(ad, "store_logo")
    idAgence = await get_or_create_agence(storeId, store_name, store_logo) if storeId and store_name else None
    if idAgence:
        idAgence = str(idAgence)

    annonce_data = RealState(
        idSec=annonce_id,
        publication_date=ad.get("first_publication_date"),
        index_date=ad.get("index_date"),
        expiration_date=ad.get("expiration_date"),
        status=ad.get("status"),
        ad_type=ad.get("ad_type"),
        title=title,
        description=ad.get("body"),
        url=ad.get("url"),
        category_id=ad.get("category_id"),
        category_name=ad.get("category_name"),
        price=price,
        nbrImages=ad.get("images", {}).get("nb_images"),
        images=ad.get("images", {}).get("urls", []),
        typeBien=await get_attr_by_label(ad, "Type de bien"),
        meuble=await get_attr_by_label(ad, "Ce bien est :"),
        surface=await get_attr_by_label(ad, "Surface habitable"),
        nombreDepiece=await get_attr_by_label(ad, "Nombre de pièces"),
        nombreChambres=await get_attr_by_label(ad, "Nombre de chambres"),
        nombreSalleEau=await get_attr_by_label(ad, "Nombre de salle d'eau"),
        nb_salles_de_bain=await get_attr_by_label(ad, "Nombre de salle de bain"),
        nb_parkings=await get_attr_by_label(ad, "Places de parking"),
        nb_niveaux=await get_attr_by_label(ad, "Nombre de niveaux"),
        disponibilite=await get_attr_by_label(ad, "Disponible à partir de"),
        annee_construction=await get_attr_by_label(ad, "Année de construction"),
        classeEnergie=await get_attr_by_label(ad, "Classe énergie"),
        ges=await get_attr_by_label(ad, "GES"),
        ascenseur=await get_attr_by_label(ad, "Ascenseur"),
        etage=await get_attr_by_label(ad, "Étage de votre bien"),
        nombreEtages=await get_attr_by_label(ad, "Nombre d'étages dans l'immeuble"),
        exterieur=await get_attr_by_label(ad, "Extérieur", get_values=True),
        charges_incluses=await get_attr_by_label(ad, "Charges incluses"),
        depot_garantie=await get_attr_by_label(ad, "Dépôt de garantie"),
        loyer_mensuel_charges=await get_attr_by_label(ad, "Charges locatives"),
        caracteristiques=await get_attr_by_label(ad, "Caractéristiques", get_values=True),
        region=ad.get("location", {}).get("region_name"),
        city=ad.get("location", {}).get("city"),
        zipcode=ad.get("location", {}).get("zipcode"),
        departement=ad.get("location", {}).get("department_name"),
        latitude=ad.get("location", {}).get("lat"),
        longitude=ad.get("location", {}).get("lng"),
        region_id=ad.get("location", {}).get("region_id"),
        departement_id=ad.get("location", {}).get("department_id"),
        agenceName=ad.get("owner", {}).get("name"),
        store_name=store_name,
        storeId=storeId,
        idAgence=idAgence,
        scraped_at=datetime.utcnow()
    )

    try:
        await save_annonce_to_db(annonce_data)
        total_scraped += 1
        logger.info(f"✅ Annonce enregistrée dans realState : {annonce_id} - Total extrait : {total_scraped}")
        if idAgence:
            source_db = get_source_db()
            await source_db["realStateWithAgence"].insert_one(annonce_data.dict())
            logger.info(f"✅ Annonce {annonce_id} avec idAgence {idAgence} enregistrée dans realStateWithAgence")
        return False  # Nouvelle annonce
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'enregistrement de {annonce_id} : {e}")
        return False

# Le reste du code reste inchangé (get_latest_valid_api_response, handle_no_results, scrape_listings_via_api_loop)
async def get_latest_valid_api_response(api_responses: list) -> dict | None:
    for response in reversed(api_responses):
        if "ads" in response and response["ads"]:
            return response
    return None

async def handle_no_results(page: Page, current_page: int) -> bool:
    NO_RESULTS_SELECTOR = '[data-test-id="noErrorMainMessage"]'
    if await page.locator(NO_RESULTS_SELECTOR).is_visible(timeout=5000):
        logger.warning(f"⚠️ Message 'Désolés, nous n’avons pas ça sous la main !' détecté à la page {current_page + 1}.")
        logger.info("🔄 Rechargement de la page...")
        await page.reload(timeout=60000)
        if not await check_and_solve_captcha(page, "rechargement après message d'erreur"):
            logger.error("❌ Échec du CAPTCHA après rechargement.")
            return False
        previous_page = current_page
        previous_page_selector = f'[data-spark-component="pagination-item"][aria-label="Page {previous_page}"]'
        logger.info(f"⏪ Retour à la page précédente ({previous_page})...")
        await human_like_scroll_to_element(page, previous_page_selector, scroll_steps=3, jitter=True)
        await human_like_click_search(page, previous_page_selector, move_cursor=True, click_delay=random.uniform(0.5, 1.5))
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < 15:
            if f"page={previous_page}" in page.url.lower():
                logger.info(f"✅ Navigation confirmée vers la page {previous_page}")
                break
            await asyncio.sleep(0.5)
        else:
            logger.error(f"❌ Échec du retour à la page {previous_page}.")
            return False
        return True
    return False

async def scrape_listings_via_api_loop(page: Page, api_responses: list, response_handler, initial_response: dict | None) -> bool:
    global total_scraped
    current_page = 1
    MAX_PAGES = 100
    PAGINATION_CONTAINER = 'nav[data-spark-component="pagination"] > ul'
    consecutive_existing = 0

    logger.info("🌀 Début du scraping des annonces via API (mode boucle)...")
    if initial_response:
        logger.info(f"✅ Page 1: {len(initial_response['ads'])} annonces interceptées via API.")
        for ad in initial_response["ads"]:
            if await process_ad(ad):
                consecutive_existing += 1
                if consecutive_existing >= 2:
                    logger.info(f"🏁 Deux annonces consécutives existantes trouvées. Arrêt du cycle.")
                    return True
            else:
                consecutive_existing = 0
        await simulate_reading(page)
    else:
        logger.warning("⚠️ Page 1: Aucune réponse valide avec annonces.")
        return False

    logger.info("🎙️ Utilisation de l'écouteur API existant pour la pagination.")
    while current_page < MAX_PAGES:
        next_page_number = current_page + 1
        page_button_selector = f'[data-spark-component="pagination-item"][aria-label="Page {next_page_number}"]'
        page_button = page.locator(page_button_selector)

        await human_like_exploration(page)
        await simulate_reading(page)
        await asyncio.sleep(random.uniform(1, 3))

        logger.info(f"📜 Défilement vers la pagination pour la page {next_page_number}...")
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < 15:
            await human_like_scroll_to_element(page, PAGINATION_CONTAINER, scroll_steps=3, jitter=True)
            if await page_button.is_visible(timeout=1000):
                logger.info(f"✅ Bouton de la page {next_page_number} trouvé.")
                break
            await asyncio.sleep(0.5)
        else:
            logger.info(f"🏁 Pagination non trouvée pour la page {next_page_number}. Vérification du message d'erreur...")
            if await handle_no_results(page, current_page):
                api_event = []
                api_found = False

                async def on_api_response_previous(response: Response) -> None:
                    nonlocal api_found, consecutive_existing
                    if response.url.startswith(TARGET_API_URL) and response.status == 200 and not api_found:
                        json_response = await response.json()
                        if "ads" in json_response and json_response["ads"]:
                            api_event.append(json_response)
                            api_found = True
                            for ad in json_response.get("ads", []):
                                if await process_ad(ad):
                                    consecutive_existing += 1
                                    if consecutive_existing >= 2:
                                        logger.info(f"🏁 Deux annonces consécutives existantes trouvées. Arrêt du cycle.")
                                        return True
                                else:
                                    consecutive_existing = 0
                            logger.info(f"✅ Page {current_page}: {len(json_response['ads'])} annonces interceptées via API après retour.")

                def response_callback_previous(response):
                    asyncio.create_task(on_api_response_previous(response))

                page.on("response", response_callback_previous)
                start_time = asyncio.get_event_loop().time()
                while asyncio.get_event_loop().time() - start_time < 15 and not api_found:
                    await asyncio.sleep(0.5)
                page.remove_listener("response", response_callback_previous)
                if not api_found:
                    logger.warning(f"⚠️ Pas de réponse API pour la page {current_page} après retour.")
            else:
                logger.info(f"🏁 Fin de la pagination à la page {current_page}.")
                break

        logger.info(f"🌀 Passage à la page {next_page_number}...")
        api_event = []
        api_found = False

        async def on_api_response(response: Response) -> None:
            nonlocal api_found, consecutive_existing
            if response.url.startswith(TARGET_API_URL) and response.status == 200 and not api_found:
                try:
                    json_response = await response.json()
                    if "ads" in json_response and json_response["ads"]:
                        api_event.append(json_response)
                        api_found = True
                        for ad in json_response.get("ads", []):
                            if await process_ad(ad):
                                consecutive_existing += 1
                                if consecutive_existing >= 2:
                                    logger.info(f"🏁 Deux annonces consécutives existantes trouvées. Arrêt du cycle.")
                                    return True
                            else:
                                consecutive_existing = 0
                        logger.info(f"✅ Page {next_page_number}: {len(json_response['ads'])} annonces interceptées via API.")
                except Exception as e:
                    logger.error(f"⚠️ Erreur lors du traitement de la réponse API pour la page {next_page_number} : {e}")

        def response_callback(response):
            asyncio.create_task(on_api_response(response))

        page.on("response", response_callback)
        max_retries = 1
        for retry in range(max_retries):
            try:
                await human_like_exploration(page)
                await asyncio.sleep(random.uniform(0.5, 2))
                if not await check_and_solve_captcha(page, f"navigation vers page {next_page_number}"):
                    logger.error(f"❌ Échec CAPTCHA avant page {next_page_number}.")
                    break
                await human_like_scroll_to_element(page, page_button_selector, scroll_steps=3, jitter=True)
                await human_like_click_search(page, page_button_selector, move_cursor=True, click_delay=random.uniform(0.5, 1.5))
                start_time = asyncio.get_event_loop().time()
                while asyncio.get_event_loop().time() - start_time < 15:
                    if f"page={next_page_number}" in page.url.lower():
                        logger.info(f"✅ Navigation confirmée vers la page {next_page_number}")
                        break
                    await asyncio.sleep(0.5)
                else:
                    raise TimeoutError(f"Échec de la navigation vers la page {next_page_number}")
                start_time = asyncio.get_event_loop().time()
                while asyncio.get_event_loop().time() - start_time < 15 and not api_found:
                    await asyncio.sleep(0.5)
                if not api_found:
                    logger.warning(f"⚠️ Pas de réponse API pour la page {next_page_number}.")
                break
            except Exception as e:
                logger.error(f"⚠️ Erreur page {next_page_number} (tentative {retry+1}/{max_retries}): {e}")
                if retry < max_retries - 1:
                    await asyncio.sleep(random.uniform(2, 4))

        page.remove_listener("response", response_callback)
        current_page += 1
        await asyncio.sleep(random.uniform(5, 15))

    logger.info(f"🏁 Scraping terminé - Total annonces extraites : {total_scraped}")
    return False

if __name__ == "__main__":
    logger.info("Module listings_parser_loop chargé.")