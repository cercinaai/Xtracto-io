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

TARGET_API_URL = "https://api.leboncoin.fr/finder/search"

async def check_and_solve_captcha(page: Page, action: str) -> bool:
    captcha_selector = 'iframe[title="DataDome CAPTCHA"]'
    if await page.locator(captcha_selector).is_visible(timeout=3000):
        logger.warning(f"⚠️ CAPTCHA détecté avant {action}.")
        if not await solve_audio_captcha(page):
            logger.error(f"❌ Échec de la résolution du CAPTCHA avant {action}.")
            return False
        logger.info(f"✅ CAPTCHA résolu avant {action}.")
    return True

async def open_leboncoin(queue: Queue):
    logger.info("🚀 Démarrage du scraping...")
    max_retries = 3  # Augmenté à 3
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

    for attempt in range(max_retries):
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
                continue

            await wait_for_page_load(page)
            await close_cookies_popup(page)

            gimii_closed = await close_gimii_popup(page)
            if gimii_closed:
                logger.info("🔄 Popup Gimii détectée et fermée.")
                await wait_for_page_load(page)

            if not await check_and_solve_captcha(page, "navigation vers Locations"):
                queue.put({"status": "error", "message": "Échec de la résolution du CAPTCHA avant navigation Locations"})
                continue

            await navigate_to_locations(page)
            initial_response, response_handler = await apply_filters(page, api_responses)

            if not await check_and_solve_captcha(page, "scraping des listings"):
                queue.put({"status": "error", "message": "Échec de la résolution du CAPTCHA avant scraping"})
                continue

            await scrape_listings_via_api(page, api_responses, response_handler, initial_response)
            title = await page.title()
            logger.info(f"✅ Page ouverte - Titre : {title}")
            queue.put({"status": "success", "title": title})
            break

        except Exception as e:
            logger.error(f"⚠️ Erreur (Tentative {attempt+1}/{max_retries}) : {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(random.uniform(10, 20))
            else:
                queue.put({"status": "error", "message": str(e)})
        finally:
            if 'page' in locals():
                page.remove_listener("response", on_response)
                await page.close()

    await cleanup_browser(client, profile_id, playwright, browser)