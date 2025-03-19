import logging
import random
import asyncio
from multiprocessing import Process, Queue
from src.captcha.captchaSolver import solve_audio_captcha
from src.config.browserConfig import setup_browser, cleanup_browser
from src.scrapers.leboncoin.searchParser import close_cookies_popup, wait_for_page_load, apply_filters, navigate_to_locations, close_gimii_popup
from src.scrapers.leboncoin.listings_parser import scrape_listings_via_api
from playwright.async_api import Page
from src.scrapers.leboncoin.utils.human_behavorScrapperLbc import human_like_exploration, simulate_reading
from loguru import logger

logger = logging.getLogger(__name__)
TARGET_API_URL = "https://api.leboncoin.fr/finder/search"

async def check_and_solve_captcha(page: Page, action: str) -> bool:
    """Vérifie et résout le CAPTCHA si présent avant une action."""
    captcha_selector = 'iframe[title="DataDome CAPTCHA"]'
    if await page.locator(captcha_selector).is_visible(timeout=3000):
        logger.warning(f"⚠️ CAPTCHA détecté avant {action}.")
        if not await solve_audio_captcha(page):
            logger.error(f"❌ Échec de la résolution du CAPTCHA avant {action}.")
            return False
        logger.info(f"✅ CAPTCHA résolu avant {action}.")
    return True

def run_open_leboncoin(queue):
    asyncio.run(open_leboncoin(queue))

async def open_leboncoin(queue: Queue):
    logger.info("🚀 Démarrage du scraping...")
    max_retries = 1
    browser = context = client = profile_id = playwright = None

    try:
        browser, context, client, profile_id, playwright = await setup_browser()
        if not browser or not context:
            logger.error("⚠️ ERREUR: Impossible d'ouvrir le navigateur.")
            queue.put({"status": "error", "message": "Impossible d'ouvrir le navigateur"})
            return
    except Exception as e:
        logger.error(f"⚠️ Erreur lors de l'initialisation du navigateur : {e}")
        queue.put({"status": "error", "message": "Échec de l'initialisation du navigateur"})
        return

    page = None
    try:
        page = await context.new_page()
        api_responses = []

        async def process_response(response):
            if response.url.startswith(TARGET_API_URL) and response.status == 200:
                try:
                    json_response = await response.json()
                    api_responses.append(json_response)
                    if "ads" in json_response and json_response["ads"]:
                        logger.info(f"📡 {len(json_response['ads'])} annonces : {response.url}")
                    else:
                        logger.debug(f"📡 Sans annonces : {response.url}")
                except Exception as e:
                    logger.debug(f"⚠️ Erreur dans la réponse {response.url} : {e}")

        def on_response(response):
            asyncio.create_task(process_response(response))

        page.on("response", on_response)
        logger.info("🌍 Accès à Leboncoin...")
        await page.goto("https://www.leboncoin.fr/", timeout=90000)

        await human_like_exploration(page)
        await simulate_reading(page)
        await asyncio.sleep(random.uniform(1, 3))

        if not await check_and_solve_captcha(page, "premières interactions"):
            queue.put({"status": "error", "message": "Échec de la résolution du CAPTCHA initial"})
            return

        await wait_for_page_load(page)
        await close_cookies_popup(page)

        gimii_closed = await close_gimii_popup(page)
        if gimii_closed:
            logger.info("🔄 Popup Gimii détectée et fermée, nouvelle tentative de chargement.")
            await wait_for_page_load(page)

        if await page.locator("#didomi-popup > div > div > div > span").is_visible(timeout=2000):
            logger.error("⚠️ La pop-up des cookies n’a pas été fermée correctement.")
            queue.put({"status": "error", "message": "Échec fermeture cookies"})
            return

        logger.info("🔍 Seconde vérification de la popup Gimii avant navigation...")
        gimii_closed_again = await close_gimii_popup(page)
        if gimii_closed_again:
            logger.info("🔄 Popup Gimii détectée une seconde fois et fermée.")
            await wait_for_page_load(page)

        if not await check_and_solve_captcha(page, "navigation vers Locations"):
            queue.put({"status": "error", "message": "Échec de la résolution du CAPTCHA avant navigation Locations"})
            return

        await navigate_to_locations(page)
        initial_response, response_handler = await apply_filters(page, api_responses)

        if not await check_and_solve_captcha(page, "scraping des listings"):
            queue.put({"status": "error", "message": "Échec de la résolution du CAPTCHA avant scraping"})
            return

        # Scraper 100 pages avant de fermer le navigateur
        await scrape_listings_via_api(page, api_responses, response_handler, initial_response)
        title = await page.title()
        logger.info(f"✅ Page ouverte - Titre : {title}")
        queue.put({"status": "success", "title": title})

    except Exception as e:
        logger.error(f"⚠️ Erreur lors du scraping : {e}")
        queue.put({"status": "error", "message": str(e)})
    finally:
        if page:
            try:
                page.remove_listener("response", on_response)
                logger.info("🎙️ Écouteur API principal supprimé.")
            except Exception as e:
                logger.error(f"❌ Erreur lors de la suppression de l'écouteur principal : {e}")
            # Ne pas fermer la page ici, elle reste ouverte jusqu'à la fin du scraping
        # Ne pas appeler cleanup_browser ici, on le fera après le scraping des 100 pages

    # Attendre que le scraping soit terminé (géré dans scrape_listings_via_api)
    # Le navigateur sera fermé dans access_leboncoin après 100 pages

    await cleanup_browser(client, profile_id, playwright, browser)

async def access_leboncoin():
    queue = Queue()
    process = Process(target=run_open_leboncoin, args=(queue,))
    process.start()
    process.join()
    result = queue.get()

    # Si le scraping est terminé (100 pages scrappées ou erreur), le navigateur est fermé dans open_leboncoin
    if result.get("status") == "success":
        logger.info("✅ Scraping des 100 pages terminé avec succès.")
    else:
        logger.error("❌ Scraping échoué : le navigateur a été fermé, mais tu peux vérifier les logs pour plus de détails.")

    return {"status": "success", "message": "Scraping terminé", "data": result}

if __name__ == "__main__":
    asyncio.run(access_leboncoin())