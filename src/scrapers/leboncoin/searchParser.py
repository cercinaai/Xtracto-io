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
    # Sélecteur corrigé pour la popup Gimii
    GIMII_POPUP = 'div[class*="gimii_root__"]'  # Conteneur de la popup
    CLOSE_BUTTON = 'button[class*="gimii_root__"][class*="gimii_secondary__"][class*="gimii_action__"]'  # Bouton "Je ne souhaite pas participer"
    logger.info("🔍 Vérification de la popup Gimii...")

    # Attendre que la popup soit visible avec un timeout plus long
    popup = page.locator(GIMII_POPUP)
    close_button = page.locator(CLOSE_BUTTON)
    
    # Boucle d'attente dynamique pour détecter la popup
    max_wait = 10  # Attendre jusqu'à 10 secondes
    start_time = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start_time < max_wait:
        try:
            if await popup.is_visible(timeout=1000):
                logger.info("✅ Popup Gimii détectée.")
                # S'assurer que le bouton de fermeture est visible
                await close_button.wait_for(state="visible", timeout=5000)
                await human_like_scroll_to_element_search(page, CLOSE_BUTTON, scroll_steps=2, jitter=True)
                await human_like_delay_search(0.5, 1.5)
                if not await check_and_solve_captcha(page, "fermeture Gimii"):
                    raise Exception("Échec CAPTCHA avant fermeture Gimii")
                await human_like_click_search(page, CLOSE_BUTTON, move_cursor=True, click_delay=0.7)
                logger.info("✅ Popup Gimii fermée.")
                # Attendre un peu pour s'assurer que la popup est bien fermée
                await human_like_delay_search(1, 2)
                # Vérifier que la popup a bien disparu
                if await popup.is_visible(timeout=2000):
                    logger.warning("⚠️ Popup Gimii toujours visible après tentative de fermeture.")
                    return False
                return True
        except Exception as e:
            logger.warning(f"⚠️ Erreur lors de la détection de la popup Gimii : {e}")
        await asyncio.sleep(0.5)

    # Si la popup n'est pas détectée après le délai, capturer une capture d'écran pour débogage
    logger.warning("⚠️ Aucune popup Gimii détectée après attente prolongée.")
    await page.screenshot(path="gimii_detection_error.png")
    page_content = await page.content()
    with open("gimii_detection_error.html", "w", encoding="utf-8") as f:
        f.write(page_content)
    logger.info("📸 Capture d'écran et contenu HTML sauvegardés pour débogage (gimii_detection_error).")
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

async def navigate_to_locations(page, max_attempts=3):
    LOCATIONS_LINK = 'a[href="/c/locations"][title="Locations"]'
    FILTERS_BUTTON = 'button[title="Afficher tous les filtres"]'
    RESULTS_CONTAINER = '[data-test-id="listing-card"]'
    logger.info("🌀 Navigation vers 'Locations'...")

    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"🌀 Tentative {attempt}/{max_attempts} de navigation vers 'Locations'...")

            # Étape 1 : Fermer la popup des cookies et attendre
            await close_cookies_popup(page)
            logger.info("⏳ Attente après fermeture des cookies...")
            await human_like_delay_search(1, 2)

            # Étape 2 : Vérifier Gimii une seule fois avant de scroller
            gimii_closed = await close_gimii_popup(page)
            if gimii_closed:
                logger.info("✅ Popup Gimii détectée et fermée avant défilement.")
            else:
                logger.info("✅ Aucune popup Gimii détectée avant défilement.")

            # Ajouter un délai avant de scroller pour simuler un comportement humain
            logger.info("⏳ Attente prolongée avant de scroller vers 'Locations'...")
            await human_like_delay_search(4, 6)  # Délai de 4 à 6 secondes

            # Étape 3 : Scroller vers le lien "Locations"
            logger.info("📜 Défilement vers le lien 'Locations'...")
            await human_like_scroll_to_element_search(page, LOCATIONS_LINK, scroll_steps=random.randint(6, 10), jitter=True)

            # Étape 4 : Vérifier que le lien "Locations" est visible et cliquable
            locations_link = page.locator(LOCATIONS_LINK)
            try:
                await locations_link.wait_for(state="visible", timeout=30000)
                logger.info("✅ Lien 'Locations' visible après défilement.")
                await expect(locations_link).to_be_enabled(timeout=5000)
                logger.info("✅ Lien 'Locations' est cliquable.")
            except PlaywrightTimeoutError as e:
                logger.error(f"❌ Lien 'Locations' non trouvé ou non cliquable dans le délai imparti : {e}")
                await page.screenshot(path=f"locations_link_error_attempt_{attempt}.png")
                raise Exception("Lien 'Locations' non visible ou non cliquable sur la page.")

            # Étape 5 : Vérifier Gimii après le défilement et avant le clic, avec plusieurs tentatives
            max_gimii_attempts = 3
            for gimii_attempt in range(max_gimii_attempts):
                gimii_before_click = await close_gimii_popup(page)
                if gimii_before_click:
                    logger.info(f"✅ Popup Gimii détectée et fermée après défilement (tentative {gimii_attempt + 1}/{max_gimii_attempts}).")
                    # Attendre un peu après la fermeture pour s'assurer que la popup est bien fermée
                    await human_like_delay_search(1, 2)
                    # S'assurer que le lien "Locations" est toujours visible après la fermeture de Gimii
                    await human_like_scroll_to_element_search(page, LOCATIONS_LINK, scroll_steps=2, jitter=True)
                    await locations_link.wait_for(state="visible", timeout=10000)
                    await expect(locations_link).to_be_enabled(timeout=5000)
                else:
                    logger.info("✅ Aucune popup Gimii détectée après défilement.")
                    break
            else:
                logger.error(f"❌ Popup Gimii persiste après {max_gimii_attempts} tentatives de fermeture.")
                raise Exception("Échec de la fermeture de la popup Gimii avant clic sur Locations")

            # Étape 6 : Vérifier les blocages anti-bot avant le clic
            captcha_iframe = page.locator('iframe[title="DataDome CAPTCHA"]')
            if await captcha_iframe.is_visible(timeout=3000):
                logger.warning("⚠️ CAPTCHA détecté avant clic sur 'Locations', tentative de résolution...")
                if not await solve_audio_captcha(page):
                    logger.error("❌ Échec de la résolution du CAPTCHA avant clic sur 'Locations'.")
                    raise Exception("Échec CAPTCHA avant clic sur Locations")
                logger.info("✅ CAPTCHA résolu, reprise de la navigation...")

            error_message = page.locator('text="Vous avez été bloqué"')
            if await error_message.is_visible(timeout=3000):
                logger.error("❌ Blocage anti-bot détecté par Leboncoin.")
                await page.screenshot(path=f"anti_bot_error_attempt_{attempt}.png")
                raise Exception("Blocage anti-bot détecté avant clic sur Locations")

            # Étape 7 : Cliquer sur le lien "Locations" avec une attente explicite
            # Capturer une capture d'écran et le contenu HTML pour débogage
            await page.screenshot(path=f"before_locations_click_attempt_{attempt}.png")
            page_content = await page.content()
            with open(f"before_locations_click_attempt_{attempt}.html", "w", encoding="utf-8") as f:
                f.write(page_content)
            logger.info(f"📸 Capture d'écran et contenu HTML sauvegardés avant clic (attempt {attempt}).")

            await human_like_delay_search(2, 5)
            await locations_link.hover()
            logger.info("🖱️ Survol du lien 'Locations' effectué.")
            await human_like_delay_search(0.5, 1.5)

            # Tentative de clic avec plusieurs méthodes
            try:
                # Méthode 1 : Clic via human_like_click_search
                async with page.expect_navigation(timeout=60000) as navigation_info:
                    await human_like_click_search(page, LOCATIONS_LINK, move_cursor=True, click_variance=30)
                    logger.info("🖱️ Clic sur le lien 'Locations' effectué via human_like_click_search.")
                await navigation_info.value
            except Exception as e:
                logger.warning(f"⚠️ Échec du clic via human_like_click_search : {e}")
                try:
                    # Méthode 2 : Clic via JavaScript
                    logger.info("🔄 Tentative de clic via JavaScript...")
                    async with page.expect_navigation(timeout=60000) as navigation_info:
                        await page.evaluate("""
                            (selector) => {
                                const link = document.querySelector(selector);
                                if (link) {
                                    const event = new Event('click', { bubbles: true, cancelable: true });
                                    link.dispatchEvent(event);
                                    link.click();
                                } else {
                                    throw new Error("Lien non trouvé pour clic JavaScript");
                                }
                            }
                        """, LOCATIONS_LINK)
                        logger.info("🖱️ Clic sur le lien 'Locations' effectué via JavaScript.")
                    await navigation_info.value
                except Exception as e:
                    logger.warning(f"⚠️ Échec du clic via JavaScript : {e}")
                    # Méthode 3 : Navigation directe
                    logger.info("🔄 Tentative de navigation directe...")
                    await page.goto(EXPECTED_LOCATIONS_URL, timeout=60000)
                    logger.info("🌐 Navigation directe vers 'Locations' effectuée.")

            # Étape 8 : Attendre la redirection et vérifier la page "Locations"
            logger.info("⏳ Attente de la redirection vers la page 'Locations'...")
            await page.wait_for_load_state("domcontentloaded", timeout=60000)

            # Vérifier l'URL après la navigation
            current_url = page.url
            logger.info(f"🌐 URL actuelle après navigation : {current_url}")
            if EXPECTED_LOCATIONS_URL not in current_url:
                logger.warning(f"⚠️ URL incorrecte après navigation ({current_url}), tentative de navigation JavaScript...")
                await page.evaluate(f"window.location.href = '{EXPECTED_LOCATIONS_URL}'")
                await page.wait_for_load_state("domcontentloaded", timeout=60000)
                current_url = page.url
                logger.info(f"🌐 URL après navigation JavaScript : {current_url}")
                if EXPECTED_LOCATIONS_URL not in current_url:
                    logger.error(f"❌ Échec de la navigation vers {EXPECTED_LOCATIONS_URL}")
                    await page.screenshot(path=f"navigation_error_attempt_{attempt}.png")
                    raise Exception(f"Navigation vers 'Locations' échouée, URL actuelle : {current_url}")

            # Vérifier la présence du bouton "Afficher tous les filtres" ou d'un conteneur de résultats
            navigation_confirmed = False
            if EXPECTED_LOCATIONS_URL in current_url:
                logger.info("✅ URL correcte détectée.")
                try:
                    await page.locator(RESULTS_CONTAINER).first.wait_for(state="visible", timeout=15000)
                    logger.info("✅ Conteneur de résultats visible, navigation confirmée.")
                    navigation_confirmed = True
                except PlaywrightTimeoutError:
                    logger.warning("⚠️ Conteneur de résultats non trouvé, vérification du bouton 'Afficher tous les filtres'...")

            if not navigation_confirmed:
                try:
                    await page.locator(FILTERS_BUTTON).wait_for(state="visible", timeout=15000)
                    logger.info("✅ Page 'Locations' chargée avec le bouton 'Afficher tous les filtres' visible.")
                    navigation_confirmed = True
                except PlaywrightTimeoutError:
                    logger.warning(f"⚠️ Bouton 'Afficher tous les filtres' non trouvé après navigation. URL actuelle : {current_url}")
                    if await error_message.is_visible(timeout=3000):
                        logger.error("❌ Blocage anti-bot détecté après navigation.")
                        await page.screenshot(path=f"anti_bot_error_after_navigation_attempt_{attempt}.png")
                        raise Exception("Blocage anti-bot détecté après navigation")
                    page_content = await page.content()
                    with open(f"page_content_attempt_{attempt}.html", "w", encoding="utf-8") as f:
                        f.write(page_content)
                    logger.error(f"❌ Échec de la navigation vers {EXPECTED_LOCATIONS_URL}, contenu de la page sauvegardé dans page_content_attempt_{attempt}.html")
                    await page.screenshot(path=f"navigation_error_attempt_{attempt}.png")
                    raise Exception(f"Navigation vers 'Locations' échouée, URL actuelle : {current_url}")

            # Étape 9 : Vérifier Gimii après navigation
            gimii_reappeared = await close_gimii_popup(page)
            if gimii_reappeared:
                logger.warning("⚠️ Popup Gimii réapparue sur la page 'Locations', fermée à nouveau.")

            logger.info("✅ Navigation vers 'Locations' réussie.")
            return True

        except Exception as e:
            logger.error(f"⚠️ Erreur lors de la navigation (Tentative {attempt}/{max_attempts}) : {e}")
            if attempt == max_attempts:
                logger.error("❌ Échec après toutes les tentatives.")
                raise
            try:
                await page.reload(timeout=60000)
                await human_like_delay_search(5, 10)
                await wait_for_page_load(page)
            except Exception as reload_error:
                logger.error(f"❌ Échec du rechargement de la page : {reload_error}")
                raise

async def apply_filters(page, api_responses: list):
    FILTRES_BTN = 'button[title="Afficher tous les filtres"]'
    MAISON_CHECKBOX = 'button[role="checkbox"][value="1"]'
    APPARTEMENT_CHECKBOX = 'button[role="checkbox"][value="2"]'
    PRO_CHECKBOX = 'button[role="checkbox"][value="pro"]'
    SEARCH_BTN = page.locator('#radix-\\:Rbbj6qcrl6\\: > footer button[aria-label="Rechercher"][data-spark-component="button"]')
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
        await SEARCH_BTN.wait_for(state="visible", timeout=20000)
        await human_like_scroll_to_element(page, '#radix-\\:Rbbj6qcrl6\\: > footer button[aria-label="Rechercher"][data-spark-component="button"]', scroll_steps=2, jitter=True)
        if not await check_and_solve_captcha(page, "clic sur Rechercher"):
            raise Exception("Échec CAPTCHA avant clic sur Rechercher")
        await human_like_click_search(page, '#radix-\\:Rbbj6qcrl6\\: > footer button[aria-label="Rechercher"][data-spark-component="button"]', move_cursor=True, click_delay=0.7, click_variance=30)
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