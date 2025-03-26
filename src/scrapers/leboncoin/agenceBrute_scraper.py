import asyncio
from datetime import datetime
from src.captcha.captchaSolver import solve_audio_captcha
from src.config.browserConfig import setup_browser, cleanup_browser
from src.database.database import get_source_db, get_destination_db
from src.scrapers.leboncoin.utils.human_behavorScrapperLbc import (
    human_like_click_search,
    human_like_delay_search,
    human_like_scroll_to_element_search
)
from playwright.async_api import Page
from loguru import logger

async def scrape_agence_details(page: Page, store_id: str, lien: str) -> dict:
    """Scrape les détails d'une agence à partir de sa page."""
    update_data = {"scraped": True, "scraped_at": datetime.utcnow()}

    # CodeSiren
    code_siren = "Non trouvé"
    try:
        siren_locator = page.locator('h3[data-qa-id="siren_number"]')
        if await siren_locator.is_visible(timeout=5000):
            await human_like_scroll_to_element_search(page, siren_locator, scroll_steps=4, jitter=True)
            siren_text = await siren_locator.text_content()
            code_siren = siren_text.replace("SIREN : ", "").strip() if siren_text else "Non trouvé"
            logger.info(f"✅ CodeSiren trouvé pour l’agence {store_id} : {code_siren}")
    except Exception as e:
        logger.warning(f"⚠️ CodeSiren non trouvé pour l’agence {store_id} : {e}")
    update_data["CodeSiren"] = code_siren

    # Adresse
    await human_like_delay_search(0.5, 1.0)
    adresse = "Non trouvé"
    try:
        adresse_elements = await page.locator("div.flex.gap-sm p").all()
        if adresse_elements:
            text_contents = [await el.text_content() for el in adresse_elements if await el.is_visible(timeout=5000)]
            adresse = " ".join(text.strip() for text in text_contents if text.strip()) or "Non trouvé"
            logger.info(f"✅ Adresse trouvée pour l’agence {store_id} : {adresse}")
    except Exception as e:
        logger.warning(f"⚠️ Adresse non trouvée pour l’agence {store_id} : {e}")
    update_data["adresse"] = adresse

    # Zone d’intervention
    await human_like_delay_search(0.5, 1.0)
    zone_intervention = "Non trouvé"
    try:
        zone_intervention_locator = page.locator("section:has-text('Ce professionnel intervient dans la zone suivante') p")
        if await zone_intervention_locator.is_visible(timeout=5000):
            await human_like_scroll_to_element_search(page, zone_intervention_locator, scroll_steps=4, jitter=True)
            zone_intervention = await zone_intervention_locator.text_content() or "Non trouvé"
            logger.info(f"✅ Zone d’intervention trouvée pour l’agence {store_id} : {zone_intervention}")
    except Exception as e:
        logger.warning(f"⚠️ Zone d’intervention non trouvée pour l’agence {store_id} : {e}")
    update_data["zone_intervention"] = zone_intervention

    # Site Web
    await human_like_delay_search(0.5, 1.0)
    site_web = "Non trouvé"
    try:
        site_web_locator = page.locator("a[data-qa-id='link_pro_website']")
        if await site_web_locator.is_visible(timeout=5000):
            await human_like_scroll_to_element_search(page, site_web_locator, scroll_steps=4, jitter=True)
            site_web = await site_web_locator.get_attribute("href") or "Non trouvé"
            logger.info(f"✅ Site web trouvé pour l’agence {store_id} : {site_web}")
    except Exception as e:
        logger.warning(f"⚠️ Site web non trouvé pour l’agence {store_id} : {e}")
    update_data["siteWeb"] = site_web

    # Horaires
    await human_like_delay_search(0.5, 1.0)
    horaires = "Non trouvé"
    try:
        horaires_locator = page.locator("div[data-qa-id='company_timesheet']")
        if await horaires_locator.is_visible(timeout=5000):
            await human_like_scroll_to_element_search(page, horaires_locator, scroll_steps=4, jitter=True)
            horaires = await horaires_locator.text_content() or "Non trouvé"
            logger.info(f"✅ Horaires trouvés pour l’agence {store_id} : {horaires}")
    except Exception as e:
        logger.warning(f"⚠️ Horaires non trouvés pour l’agence {store_id} : {e}")
    update_data["horaires"] = horaires

    # Numéro de téléphone
    await human_like_delay_search(0.5, 1.0)
    numero = "Non trouvé"
    try:
        phone_tab_button = page.locator("button[role='tab'][id*='radix-'][id$='-trigger-phoneTab']")
        if await phone_tab_button.is_visible(timeout=5000):
            await human_like_scroll_to_element_search(page, phone_tab_button, scroll_steps=4, jitter=True)
            await human_like_click_search(page, phone_tab_button, move_cursor=True, click_delay=0.5)
            await human_like_delay_search(0.5, 2.0)

            display_number_button = page.locator("button[data-qa-id='button_display_phone']")
            if await display_number_button.is_visible(timeout=15000):
                await human_like_scroll_to_element_search(page, display_number_button, scroll_steps=4, jitter=True)
                await human_like_click_search(page, display_number_button, move_cursor=True, click_delay=0.5)
                await human_like_delay_search(0.5, 2.0)

                numero_locator = page.locator("div[data-qa-id='company_phone']")
                if await numero_locator.is_visible(timeout=15000):
                    await human_like_scroll_to_element_search(page, numero_locator, scroll_steps=4, jitter=True)
                    numero = await numero_locator.text_content() or "Non trouvé"
                    logger.info(f"✅ Numéro de téléphone trouvé pour l’agence {store_id} : {numero}")
    except Exception as e:
        logger.warning(f"⚠️ Impossible de scraper le numéro pour l’agence {store_id} : {e}")
    update_data["number"] = numero

    # Description
    await human_like_delay_search(0.5, 1.0)
    description = "Non trouvé"
    try:
        voir_plus_button = page.locator("div[data-qa-id='company_description'] button:has-text('Voir plus')")
        if await voir_plus_button.is_visible(timeout=2000):
            await human_like_click_search(page, voir_plus_button, move_cursor=True, click_delay=0.5)
            await human_like_delay_search(0.5, 1.5)

        description_locator = page.locator("div[data-qa-id='company_description'] p")
        if await description_locator.is_visible(timeout=10000):
            description = await description_locator.text_content() or "Non trouvé"
            logger.info(f"✅ Description trouvée pour l’agence {store_id} : {description[:50]}...")
    except Exception as e:
        logger.warning(f"⚠️ Impossible de scraper la description pour l’agence {store_id} : {e}")
    update_data["description"] = description

    return update_data

async def scrape_agences(queue):
    """Scrape les agences de la collection agencesBrute qui ne sont pas dans agencesFinale."""
    logger.info("🚀 Démarrage du scraping des agences dans agencesBrute...")
    source_db = get_source_db()
    dest_db = get_destination_db()
    agences_brute_collection = source_db["agencesBrute"]
    agences_finale_collection = dest_db["agencesFinale"]

    while True:
        current_hour = datetime.now().hour
        logger.info(f"⏰ Vérification horaire - Heure actuelle : {current_hour}h")
        
        # Exécuter uniquement entre 22h et 10h
        if not (current_hour < 10 or current_hour >= 22):
            logger.info("⏹️ Arrêt temporaire du scraper (horaire diurne). Reprise à 22h.")
            await asyncio.sleep(3600)  # Attendre 1 heure avant de vérifier à nouveau
            continue

        finale_ids = await agences_finale_collection.distinct("storeId")  # Utiliser storeId ici
        agences = await agences_brute_collection.find({
            "scraped": {"$ne": True},
            "$or": [
                {"storeId": {"$nin": finale_ids}}
            ]
        }).to_list(length=None)
        total_agences = len(agences)
        logger.info(f"📊 Nombre total d'agences à scraper : {total_agences}")

        if total_agences == 0:
            logger.info("ℹ️ Aucune agence à scraper dans agencesBrute ou toutes sont déjà dans agencesFinale.")
            await queue.put({"status": "success", "data": {"updated": [], "total": 0, "remaining": 0}})
            await asyncio.sleep(3600)  # Attendre 1 heure avant de recommencer
            continue

        updated_agences = []
        remaining_agences = total_agences
        browser = context = client = profile_id = playwright = None

        try:
            browser, context, client, profile_id, playwright = await setup_browser()
            page = await context.new_page()
            await page.goto("https://www.leboncoin.fr/", timeout=60000)
            await human_like_delay_search(1, 3)

            if await page.locator('iframe[title="DataDome CAPTCHA"]').is_visible(timeout=5000):
                if not await solve_audio_captcha(page):
                    logger.error("❌ Échec de la résolution du CAPTCHA initial.")
                    raise Exception("CAPTCHA failure")
                await human_like_delay_search(2, 5)

            cookie_button = page.locator("button", has_text="Accepter")
            if await cookie_button.is_visible(timeout=5000):
                await human_like_click_search(page, cookie_button, move_cursor=True, click_delay=0.2)
                await human_like_delay_search(0.2, 0.5)

            for index, agence in enumerate(agences, 1):
                current_hour = datetime.now().hour
                if current_hour >= 10 and current_hour < 22:
                    logger.info("⏹️ Arrêt forcé à 10h du matin. Fermeture du navigateur.")
                    await cleanup_browser(client, profile_id, playwright, browser)
                    browser = context = client = profile_id = playwright = None
                    break

                store_id = agence.get("storeId")
                original_id = agence.get("_id")
                if not store_id:
                    logger.error(f"❌ Aucune clé 'storeId' trouvée pour l'agence {original_id}")
                    remaining_agences -= 1
                    continue

                lien = agence.get("lien")
                logger.info(f"🔍 Scraping de l’agence {store_id} ({index}/{total_agences}) : {lien}")

                agence_page = await context.new_page()
                try:
                    await agence_page.goto(lien, timeout=90000)
                    await human_like_delay_search(1, 3)

                    if await agence_page.locator('iframe[title="DataDome CAPTCHA"]').is_visible(timeout=5000):
                        if not await solve_audio_captcha(agence_page):
                            logger.error(f"❌ Échec de la résolution du CAPTCHA pour l’agence {store_id}.")
                            raise Exception("CAPTCHA failure")

                    update_data = await scrape_agence_details(agence_page, store_id, lien)
                    # Mettre à jour agencesBrute
                    await agences_brute_collection.update_one(
                        {"_id": original_id},
                        {"$set": update_data}
                    )
                    # Transférer ou mettre à jour dans agencesFinale avec l'_id original
                    agence_data = await agences_brute_collection.find_one({"_id": original_id})
                    agence_data["_id"] = original_id  # Conserver l'_id original
                    await agences_finale_collection.delete_one({"storeId": store_id})  # Supprimer si existant
                    await agences_finale_collection.insert_one(agence_data)
                    updated_agences.append({"storeId": store_id, "name": agence.get("name"), "_id": str(original_id), **update_data})
                    logger.info(f"✅ Agence {store_id} scrapée et transférée avec _id: {original_id}")
                except Exception as e:
                    if "CAPTCHA failure" not in str(e):
                        logger.error(f"⚠️ Erreur lors du scraping de l’agence {store_id} : {e}")
                finally:
                    await agence_page.close()
                remaining_agences -= 1

            if browser:  # Si le navigateur est encore ouvert après la boucle
                logger.info(f"🏁 Scraping terminé - Total agences traitées : {total_agences}, mises à jour : {len(updated_agences)}")
                await queue.put({"status": "success", "data": {"updated": updated_agences, "total": total_agences, "remaining": remaining_agences}})
                await cleanup_browser(client, profile_id, playwright, browser)

        except Exception as e:
            logger.error(f"⚠️ Erreur dans la session : {e}")
            if browser:
                await cleanup_browser(client, profile_id, playwright, browser)
            await asyncio.sleep(10)  # Attendre avant de relancer
            continue