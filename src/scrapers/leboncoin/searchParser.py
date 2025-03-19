from playwright.async_api import expect, TimeoutError as PlaywrightTimeoutError
from src.scrapers.leboncoin.utils.human_behavorScrapperLbc import (
    human_like_click_search,
    human_like_delay_search,
    human_like_scroll_to_element_search,
    human_like_scroll_to_element
)
from loguru import logger
import asyncio
import random
from src.scrapers.leboncoin.listings_parser import get_latest_valid_api_response, process_ad
from src.captcha.captchaSolver import solve_audio_captcha
from src.database.database import init_db, close_db

TARGET_API_URL = "https://api.leboncoin.fr/finder/search"
EXPECTED_LOCATIONS_URL = "https://www.leboncoin.fr/c/locations"

async def check_and_solve_captcha(page, action: str) -> bool:
    """Vérifie et résout le CAPTCHA si présent avant une action."""
    captcha_selector = 'iframe[title="DataDome CAPTCHA"]'
    try:
        if await page.locator(captcha_selector).is_visible(timeout=3000):
            logger.warning(f"⚠️ CAPTCHA détecté avant {action}.")
            if not await solve_audio_captcha(page):
                logger.error(f"❌ Échec de la résolution du CAPTCHA avant {action}.")
                return False
            logger.info(f"✅ CAPTCHA résolu avant {action}.")
    except Exception as e:
        logger.warning(f"⚠️ Erreur lors de la vérification CAPTCHA avant {action} : {e}")
    return True

async def close_cookies_popup(page):
    ACCEPT_BUTTON = '#didomi-popup > div > div > div > span'
    logger.info("🔍 Vérification de la popup des cookies...")
    accept_button = page.locator(ACCEPT_BUTTON)
    if await accept_button.is_visible(timeout=5000):
        await human_like_scroll_to_element_search(page, ACCEPT_BUTTON, scroll_steps=2, jitter=True)
        await human_like_delay_search(0.5, 1.5)
        if not await check_and_solve_captcha(page, "fermeture cookies"):
            raise Exception("Échec CAPTCHA avant fermeture cookies")
        await human_like_click_search(page, ACCEPT_BUTTON, move_cursor=True, click_delay=0.7)
        logger.info("✅ Popup des cookies fermée.")
    else:
        logger.info("✅ Aucune popup de cookies détectée.")

async def close_gimii_popup(page) -> bool:
    CLOSE_BUTTON = '#gimii-root > div > div.gimii_root__NNJEc.gimii_modal__wnVRr.gimii-modal-opening-animation > div.gimii_root__NNJEc > div:nth-child(6) > div > button.gimii_root__CDCDX.gimii_secondary__gXJIP.gimii_action__y3k2A'
    logger.info("🔍 Vérification de la popup Gimii...")
    close_button = page.locator(CLOSE_BUTTON)
    if await close_button.is_visible(timeout=5000):
        await human_like_scroll_to_element_search(page, CLOSE_BUTTON, scroll_steps=2, jitter=True)
        await human_like_delay_search(0.5, 1.5)
        if not await check_and_solve_captcha(page, "fermeture Gimii"):
            raise Exception("Échec CAPTCHA avant fermeture Gimii")
        await human_like_click_search(page, CLOSE_BUTTON, move_cursor=True, click_delay=0.7)
        logger.info("✅ Popup Gimii fermée.")
        return True
    else:
        logger.info("✅ Aucune popup Gimii détectée.")
        return False

async def wait_for_page_load(page):
    LOCATIONS_LINK = 'a[href="/c/locations"][title="Locations"]'
    logger.info("⏳ Attente du chargement initial de la page...")
    locator = page.locator(LOCATIONS_LINK)
    max_wait = 60
    start_time = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start_time < max_wait:
        if await locator.is_visible(timeout=1000):
            await human_like_delay_search(1, 3)
            logger.info("✅ Page chargée avec le lien 'Locations' visible.")
            return
        await asyncio.sleep(0.5)
    logger.error("⚠️ Timeout lors de l'attente du chargement de la page : lien 'Locations' non trouvé.")
    raise PlaywrightTimeoutError("Page non chargée dans le délai imparti")

async def navigate_to_locations(page):
    LOCATIONS_LINK = 'a[href="/c/locations"][title="Locations"]'
    logger.info("🌀 Navigation vers 'Locations'...")

    # Étape 1 : Fermer la popup des cookies et attendre
    await close_cookies_popup(page)
    logger.info("⏳ Attente après fermeture des cookies...")
    await human_like_delay_search(1, 2)

    # Étape 2 : Vérifier Gimii ; si non trouvé, scroller vers "Locations"
    gimii_closed = await close_gimii_popup(page)
    if not gimii_closed:
        logger.info("📜 Défilement vers le lien 'Locations' car aucune popup Gimii détectée...")
        await human_like_scroll_to_element_search(page, LOCATIONS_LINK, scroll_steps=random.randint(6, 10), jitter=True)
    else:
        logger.info("📜 Défilement supplémentaire après fermeture de Gimii pour atteindre 'Locations'...")
        await human_like_scroll_to_element_search(page, LOCATIONS_LINK, scroll_steps=2, jitter=True)

    # Étape 3 : Vérifier à nouveau Gimii avant de cliquer
    locations_link = page.locator(LOCATIONS_LINK)
    try:
        await locations_link.wait_for(state="visible", timeout=30000)
        logger.info("✅ Lien 'Locations' visible après défilement.")
    except PlaywrightTimeoutError:
        logger.error("❌ Lien 'Locations' non trouvé dans le délai imparti après défilement.")
        await page.screenshot(path="locations_link_error.png")
        raise Exception("Lien 'Locations' non visible sur la page.")

    gimii_before_click = await close_gimii_popup(page)
    if gimii_before_click:
        logger.info("📜 Défilement supplémentaire après fermeture de Gimii avant clic...")
        await human_like_scroll_to_element_search(page, LOCATIONS_LINK, scroll_steps=2, jitter=True)
        await locations_link.wait_for(state="visible", timeout=10000)

    # Étape 4 : Cliquer sur le lien "Locations"
    await human_like_delay_search(0.5, 1.5)
    if not await check_and_solve_captcha(page, "clic sur Locations"):
        raise Exception("Échec CAPTCHA avant clic sur Locations")
    await human_like_click_search(page, LOCATIONS_LINK, move_cursor=True, click_variance=30)

    # Attendre un peu avant de vérifier l'URL
    await asyncio.sleep(random.uniform(2, 5))
    current_url = page.url
    logger.info(f"🌐 URL actuelle après clic : {current_url}")

    # Vérifier si on est sur la bonne page
    if EXPECTED_LOCATIONS_URL not in current_url:
        logger.warning(f"⚠️ URL incorrecte après clic ({current_url}), attente de redirection naturelle...")
        await page.wait_for_load_state("domcontentloaded", timeout=30000)
        current_url = page.url
        logger.info(f"🌐 URL après attente : {current_url}")
        if EXPECTED_LOCATIONS_URL not in current_url:
            logger.error(f"❌ Échec de la navigation vers {EXPECTED_LOCATIONS_URL}")
            await page.screenshot(path="navigation_error.png")
            raise Exception(f"Navigation vers 'Locations' échouée, URL actuelle : {current_url}")

    # Étape 5 : Vérifier Gimii après navigation
    gimii_reappeared = await close_gimii_popup(page)
    if gimii_reappeared:
        logger.warning("⚠️ Popup Gimii réapparue sur la page 'Locations', fermée à nouveau.")

    # Attente du chargement avec une condition moins stricte
    logger.info("⏳ Attente du chargement complet de la page 'Locations'...")
    await page.wait_for_load_state("domcontentloaded", timeout=30000)
    
    # Vérification supplémentaire : attendre un élément spécifique de la page "Locations"
    FILTERS_BUTTON = 'button[title="Afficher tous les filtres"]'
    try:
        await page.locator(FILTERS_BUTTON).wait_for(state="visible", timeout=10000)
        logger.info("✅ Page 'Locations' chargée avec le bouton 'Afficher tous les filtres' visible.")
    except PlaywrightTimeoutError:
        logger.error("❌ Bouton 'Afficher tous les filtres' non trouvé après chargement.")
        await page.screenshot(path="locations_page_error.png")
        raise Exception("Échec de la vérification du chargement de la page 'Locations'.")

    logger.info("✅ Navigation vers 'Locations' réussie.")
    return True

async def apply_filters(page, api_responses: list):
    FILTRES_BTN = 'button[title="Afficher tous les filtres"]'
    MAISON_CHECKBOX = 'button[role="checkbox"][value="1"]'
    APPARTEMENT_CHECKBOX = 'button[role="checkbox"][value="2"]'
    PRO_CHECKBOX = 'button[role="checkbox"][value="pro"]'
    SEARCH_BTN_SELECTOR = 'footer button[aria-label="Rechercher"]'  # Sélecteur mis à jour
    SEARCH_BTN = page.locator(SEARCH_BTN_SELECTOR)
    LOGIN_PAGE_INDICATOR = 'input[name="email"]'

    await init_db()
    logger.info("✅ Base de données initialisée pour cette session.")

    logger.info("🖱️ Clic sur 'Afficher tous les filtres'...")
    filter_button = page.locator(FILTRES_BTN)
    await expect(filter_button).to_be_visible(timeout=60000)
    if not await check_and_solve_captcha(page, "clic sur Filtres"):
        raise Exception("Échec CAPTCHA avant clic sur Filtres")
    
    await human_like_click_search(page, FILTRES_BTN, click_delay=0.7, move_cursor=True)
    await human_like_delay_search(2, 4)

    if await page.locator(LOGIN_PAGE_INDICATOR).is_visible(timeout=5000):
        logger.error("⚠️ Redirection vers la page de login détectée après clic sur 'Afficher tous les filtres'.")
        logger.info("🔄 Tentative de retour à la page des locations...")
        await page.go_back(timeout=60000)
        await page.wait_for_load_state("networkidle", timeout=30000)
        if not await page.locator(FILTRES_BTN).is_visible(timeout=10000):
            logger.error("❌ Impossible de revenir à la page des filtres.")
            raise Exception("Redirection vers login non résolue")
        logger.info("✅ Retour à la page des filtres réussi, nouvelle tentative de clic...")
        await human_like_click_search(page, FILTRES_BTN, click_delay=0.7, move_cursor=True)
        await human_like_delay_search(2, 4)

    logger.info("📜 Application du filtre 'Maison'...")
    await page.wait_for_selector(MAISON_CHECKBOX, state="visible")
    await human_like_scroll_to_element_search(page, MAISON_CHECKBOX, scroll_steps=4, jitter=True)
    if not await check_and_solve_captcha(page, "clic sur Maison"):
        raise Exception("Échec CAPTCHA avant clic sur Maison")
    await human_like_click_search(page, MAISON_CHECKBOX, click_delay=0.5, move_cursor=True)
    await human_like_delay_search(1, 2)

    logger.info("📜 Application du filtre 'Appartement'...")
    await page.wait_for_selector(APPARTEMENT_CHECKBOX, state="visible", timeout=40000)
    await human_like_scroll_to_element_search(page, APPARTEMENT_CHECKBOX, scroll_steps=4, jitter=True)
    if not await check_and_solve_captcha(page, "clic sur Appartement"):
        raise Exception("Échec CAPTCHA avant clic sur Appartement")
    await human_like_click_search(page, APPARTEMENT_CHECKBOX, click_delay=0.5, move_cursor=True)
    await human_like_delay_search(1, 2)

    api_events = []
    async def on_api_response(response):
        try:
            if response.url.startswith(TARGET_API_URL) and response.status == 200:
                json_response = await response.json()
                if "ads" in json_response and json_response["ads"]:
                    api_events.append(json_response)
                    logger.info(f"📡 Interception API : {len(json_response['ads'])} annonces trouvées")
                    for ad in json_response["ads"]:
                        await process_ad(ad)
        except Exception as e:
            logger.error(f"⚠️ Erreur lors du traitement de la réponse API : {e}")

    page.on("response", lambda response: asyncio.create_task(on_api_response(response)))
    logger.info("🎙️ Écouteur API activé à partir de 'Professionnel'.")

    logger.info("📜 Application du filtre 'Professionnel'...")
    await human_like_delay_search(1, 2)
    api_responses.clear()
    await human_like_scroll_to_element_search(page, PRO_CHECKBOX, scroll_steps=4, jitter=True)
    if not await check_and_solve_captcha(page, "clic sur Professionnel"):
        raise Exception("Échec CAPTCHA avant clic sur Professionnel")
    await human_like_click_search(page, PRO_CHECKBOX, click_delay=0.5, move_cursor=True)
    await asyncio.sleep(random.uniform(2, 5))

    initial_response = await get_latest_valid_api_response(api_events)
    if not initial_response:
        logger.warning("⚠️ Aucune réponse API valide interceptée après 'Professionnel'.")
    else:
        logger.info(f"✅ Réponse API initiale capturée : {len(initial_response['ads'])} annonces.")

    logger.info("🔄 Clic sur 'Rechercher' pour charger la page 1 et préparer la pagination...")
    try:
        # Attendre que le bouton "Rechercher" soit visible
        try:
            await SEARCH_BTN.wait_for(state="visible", timeout=30000)
            logger.info("✅ Bouton 'Rechercher' visible avec le sélecteur.")
            await human_like_scroll_to_element(page, SEARCH_BTN_SELECTOR, scroll_steps=2, jitter=True)
        except PlaywrightTimeoutError:
            logger.warning("⚠️ Bouton 'Rechercher' non trouvé avec le sélecteur. Tentative avec navigation par Tab...")
            # Simuler 3 appuis sur Tab pour naviguer vers le bouton
            for _ in range(3):
                await page.keyboard.press("Tab")
                await human_like_delay_search(0.5, 1)
            # Appuyer sur Enter pour cliquer
            await page.keyboard.press("Enter")
            logger.info("✅ Clic sur 'Rechercher' effectué via Tab + Enter.")
            await human_like_delay_search(2, 5)
            # Vérifier si la navigation a réussi
            if not await page.locator('button[title="Afficher tous les filtres"]').is_visible(timeout=10000):
                raise Exception("Échec de la navigation après Tab + Enter.")

        # Vérifier CAPTCHA avant clic (si le bouton est trouvé)
        if await SEARCH_BTN.is_visible():
            if not await check_and_solve_captcha(page, "clic sur Rechercher"):
                raise Exception("Échec CAPTCHA avant clic sur Rechercher")
            await human_like_click_search(page, SEARCH_BTN_SELECTOR, move_cursor=True, click_delay=0.7, click_variance=30)
            await human_like_delay_search(2, 5)

    except Exception as e:
        logger.error(f"❌ Erreur lors du clic sur 'Rechercher' : {str(e)}")
        await page.screenshot(path="search_button_error.png")
        raise

    final_response = await get_latest_valid_api_response(api_events)
    if not final_response:
        logger.warning("⚠️ Aucune réponse API valide interceptée après 'Rechercher'.")
    else:
        logger.info(f"✅ Réponse API finale capturée : {len(final_response['ads'])} annonces.")

    logger.info("✅ Filtres appliqués, prêt pour la pagination.")
    return final_response or initial_response, lambda response: asyncio.create_task(on_api_response(response))

if __name__ == "__main__":
    logger.info("Module searchParser chargé.")