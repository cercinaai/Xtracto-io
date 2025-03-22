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

    # CodeSiren (ne pas lever d'exception si absent)
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
        phone_button = page.locator("button[id='radix-:Rl6mirl6:-trigger-phoneTab']")
        if await phone_button.is_visible(timeout=5000):
            await human_like_scroll_to_element_search(page, phone_button, scroll_steps=4, jitter=True)
            await human_like_click_search(page, phone_button, move_cursor=True, click_delay=0.5)
            await human_like_delay_search(0.5, 2.0)

            max_attempts = 3
            for attempt in range(max_attempts):
                display_number_button = page.locator("button[data-qa-id='button_display_phone']")
                if await display_number_button.is_visible(timeout=15000):
                    await human_like_scroll_to_element_search(page, display_number_button, scroll_steps=4, jitter=True)
                    await human_like_click_search(page, display_number_button, move_cursor=True, click_delay=0.5)
                    await human_like_delay_search(0.5, 2.0)
                    await page.wait_for_timeout(2000)

                    numero_locator = page.locator("div[data-qa-id='company_phone']")
                    if await numero_locator.is_visible(timeout=15000):
                        await human_like_scroll_to_element_search(page, numero_locator, scroll_steps=4, jitter=True)
                        numero = await numero_locator.text_content() or "Non trouvé"
                        logger.info(f"✅ Numéro de téléphone trouvé pour l’agence {store_id} : {numero}")
                        break
                else:
                    logger.warning(f"⚠️ Tentative {attempt + 1}/{max_attempts} : Bouton 'Afficher le numéro' non visible pour l’agence {store_id}")
                if attempt < max_attempts - 1:
                    await human_like_delay_search(2, 3)
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
            await page.wait_for_timeout(1000)

        description_locator = page.locator("div[data-qa-id='company_description'] p")
        if description_locator and await description_locator.is_visible(timeout=10000):
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

    while True:  # Boucle externe pour relancer en cas d'échec CAPTCHA
        # Récupérer les agences non scrapées et absentes de agencesFinale
        finale_ids = await agences_finale_collection.distinct("idAgence")
        agences = await agences_brute_collection.find({
            "scraped": {"$ne": True},
            "$or": [
                {"idAgence": {"$nin": finale_ids}},
                {"storeId": {"$nin": finale_ids}}
            ]
        }).to_list(length=None)
        total_agences = len(agences)
        logger.info(f"📊 Nombre total d'agences à scraper : {total_agences}")

        if total_agences == 0:
            logger.info("ℹ️ Aucune agence à scraper dans agencesBrute ou toutes sont déjà dans agencesFinale.")
            await queue.put({"status": "success", "data": {"updated": [], "total": 0, "remaining": 0}})
            break

        updated_agences = []
        remaining_agences = total_agences
        browser = context = client = profile_id = playwright = None

        try:
            browser, context, client, profile_id, playwright = await setup_browser()
            page = await context.new_page()
            await page.goto("https://www.leboncoin.fr/", timeout=60000)  # Navigation initiale
            await human_like_delay_search(1, 3)

            if await page.locator('iframe[title="DataDome CAPTCHA"]').is_visible(timeout=5000):
                if not await solve_audio_captcha(page):
                    logger.error("❌ Échec de la résolution du CAPTCHA initial, fermeture du navigateur...")
                    await cleanup_browser(client, profile_id, playwright, browser)
                    await asyncio.sleep(10)  # Attente avant nouvelle tentative
                    continue
                await human_like_delay_search(2, 5)

            cookie_button = page.locator("button", has_text="Accepter")
            if await cookie_button.is_visible(timeout=5000):
                await human_like_click_search(page, cookie_button, move_cursor=True, click_delay=0.2)
                await human_like_delay_search(0.2, 0.5)

            for index, agence in enumerate(agences, 1):
                store_id = agence.get("idAgence") or agence.get("storeId")
                if not store_id:
                    logger.error(f"❌ Aucune clé 'idAgence' ou 'storeId' trouvée pour l'agence {agence.get('_id')}")
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
                            logger.error(f"❌ Échec de la résolution du CAPTCHA pour l’agence {store_id}, fermeture du navigateur...")
                            await agence_page.close()
                            await cleanup_browser(client, profile_id, playwright, browser)
                            await asyncio.sleep(10)  # Attente avant nouvelle tentative
                            raise Exception("CAPTCHA failure, restarting session")  # Forcer la boucle externe
                        await human_like_delay_search(2, 5)

                    update_data = await scrape_agence_details(agence_page, store_id, lien)
                    await agences_brute_collection.update_one(
                        {"_id": agence["_id"]},
                        {"$set": {**update_data, "idAgence": store_id}, "$unset": {"storeId": ""}}
                    )
                    agence_data = await agences_brute_collection.find_one({"idAgence": store_id})
                    await agences_finale_collection.update_one(
                        {"idAgence": store_id},
                        {"$set": agence_data},
                        upsert=True
                    )
                    updated_agences.append({"idAgence": store_id, "name": agence.get("name"), **update_data})
                    logger.info(f"✅ Agence {store_id} scrapée et transférée")
                except Exception as e:
                    if "CAPTCHA failure" in str(e):
                        raise  # Relancer la boucle externe
                    logger.error(f"⚠️ Erreur lors du scraping de l’agence {store_id} : {e}")
                finally:
                    await agence_page.close()
                remaining_agences -= 1

            logger.info(f"🏁 Scraping terminé - Total agences traitées : {total_agences}, mises à jour : {len(updated_agences)}")
            await queue.put({"status": "success", "data": {"updated": updated_agences, "total": total_agences, "remaining": remaining_agences}})
            break  # Sortir de la boucle si tout est traité avec succès

        except Exception as e:
            logger.error(f"⚠️ Erreur dans la session : {e}")
            if browser:  # Nettoyer uniquement si le navigateur est encore actif
                await cleanup_browser(client, profile_id, playwright, browser)
            await asyncio.sleep(10)  # Attente avant de relancer
            continue  # Relancer la boucle externe
