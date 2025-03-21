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
from src.scrapers.leboncoin.utils.agence_scraper import scrape_agence_details  # Import factorisé
from playwright.async_api import Page, TimeoutError
from loguru import logger

async def scrape_annonce_agences(queue):
    """Scrape les annonces dans realState sans idAgence et associe leurs agences."""
    logger.info("🚀 Démarrage du scraping des annonces sans agences dans realState...")
    source_db = get_source_db()
    dest_db = get_destination_db()
    realstate_collection = source_db["realState"]
    realstate_withagence_collection = source_db["realStateWithAgence"]
    agences_brute_collection = source_db["agencesBrute"]
    agences_finale_collection = dest_db["agencesFinale"]

    annonces = await realstate_collection.find({"idAgence": {"$exists": False}}).to_list(length=None)
    total_annonces = len(annonces)
    logger.info(f"📊 Nombre total d'annonces sans idAgence : {total_annonces}")

    if total_annonces == 0:
        logger.info("ℹ️ Aucune annonce sans idAgence à traiter.")
        queue.put({"status": "success", "data": {"updated": [], "skipped": [], "total": 0, "remaining": 0}})
        return

    updated_annonces = []
    skipped_annonces = []
    remaining_annonces = total_annonces

    for index, annonce in enumerate(annonces, 1):
        annonce_id = annonce["idSec"]
        url = annonce["url"]
        logger.info(f"🔍 Traitement de l’annonce {annonce_id} ({index}/{total_annonces}) : {url}")

        browser = context = client = profile_id = playwright = page = None
        try:
            browser, context, client, profile_id, playwright = await setup_browser()
            page = await context.new_page()
            await page.goto(url, timeout=60000)
            await human_like_delay_search(1, 3)

            if await page.locator('iframe[title="DataDome CAPTCHA"]').is_visible(timeout=5000):
                if not await solve_audio_captcha(page):
                    logger.error(f"❌ Échec de la résolution du CAPTCHA pour l’annonce {annonce_id}")
                    remaining_annonces -= 1
                    continue
                await human_like_delay_search(2, 5)

            cookie_button = page.locator("button", has_text="Accepter")
            if await cookie_button.is_visible(timeout=5000):
                await human_like_scroll_to_element_search(page, cookie_button, scroll_steps=2, jitter=True)
                await human_like_click_search(page, cookie_button, move_cursor=True, click_delay=0.2)
                await human_like_delay_search(0.2, 0.5)

            if await page.locator("text='Page non trouvée'").is_visible(timeout=3000):
                logger.warning(f"⚠️ Page non trouvée pour l’annonce {annonce_id}, suppression en cours...")
                await realstate_collection.delete_one({"idSec": annonce_id})
                remaining_annonces -= 1
                continue

            agence_link_locator = page.locator('a.text-body-1.custom\\:text-headline-2.block.truncate.font-bold[href*="/boutique/"]')
            if await agence_link_locator.is_visible(timeout=5000):
                await human_like_scroll_to_element_search(page, agence_link_locator, scroll_steps=2, jitter=True)
                agence_link = await agence_link_locator.get_attribute("href")
                agence_name = await agence_link_locator.text_content()
                store_id = agence_link.split("/boutique/")[1].split("/")[0]

                agence = await agences_finale_collection.find_one({"idAgence": store_id})
                if agence:
                    annonce["idAgence"] = store_id
                    await realstate_withagence_collection.update_one({"idSec": annonce_id}, {"$set": annonce}, upsert=True)
                    await realstate_collection.delete_one({"idSec": annonce_id})
                    updated_annonces.append({"idSec": annonce_id, "idAgence": store_id})
                    continue

                agence_brute = await agences_brute_collection.find_one({"idAgence": store_id})
                if not agence_brute:
                    await agences_brute_collection.insert_one({
                        "idAgence": store_id,
                        "name": agence_name,
                        "lien": f"https://www.leboncoin.fr{agence_link}",
                        "scraped": False
                    })

                agence_page = await context.new_page()
                try:
                    await agence_page.goto(f"https://www.leboncoin.fr{agence_link}", timeout=60000)
                    update_data = await scrape_agence_details(agence_page, store_id, agence_link)
                    await agences_brute_collection.update_one({"idAgence": store_id}, {"$set": update_data})
                    agence_data = await agences_brute_collection.find_one({"idAgence": store_id})
                    await agences_finale_collection.update_one({"idAgence": store_id}, {"$set": agence_data}, upsert=True)
                    annonce["idAgence"] = store_id
                    await realstate_withagence_collection.update_one({"idSec": annonce_id}, {"$set": annonce}, upsert=True)
                    await realstate_collection.delete_one({"idSec": annonce_id})
                    updated_annonces.append({"idSec": annonce_id, "idAgence": store_id})
                finally:
                    await agence_page.close()
            else:
                skipped_annonces.append(annonce_id)

        except Exception as e:
            logger.error(f"⚠️ Erreur pour l’annonce {annonce_id} : {e}")
            skipped_annonces.append(annonce_id)
        finally:
            if 'page' in locals():
                await page.close()
            await cleanup_browser(client, profile_id, playwright, browser)
            remaining_annonces -= 1

    logger.info(f"🏁 Scraping terminé - Total : {total_annonces}, mises à jour : {len(updated_annonces)}, skippées : {len(skipped_annonces)}")
    queue.put({"status": "success", "data": {"updated": updated_annonces, "skipped": skipped_annonces, "total": total_annonces, "remaining": remaining_annonces}})