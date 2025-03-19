import os
from dotenv import load_dotenv
from kameleo.local_api_client import KameleoLocalApiClient
from kameleo.local_api_client.builder_for_create_profile import BuilderForCreateProfile
from kameleo.local_api_client.models import WebDriverSettings, WebglMetaSpoofingOptions, Server
from playwright.async_api import async_playwright
import logging
import asyncio
import random

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SHIFTER_PROXIES = [
    {"host": "hermes.p.shifter.io", "port": 10445},
    {"host": "hermes.p.shifter.io", "port": 10446},
    {"host": "hermes.p.shifter.io", "port": 10447},
    {"host": "hermes.p.shifter.io", "port": 10448},
    {"host": "hermes.p.shifter.io", "port": 10449}
]

async def setup_browser():
    kameleo_host = os.getenv("KAMELEO_HOST", "192.168.122.91")  # Valeur par défaut
    kameleo_port = int(os.getenv("KAMELEO_PORT", 5001))         # Valeur par défaut
    client = KameleoLocalApiClient(endpoint=f'http://{kameleo_host}:{kameleo_port}', retry_total=0)

    base_profiles = client.search_base_profiles(device_type='desktop', browser_product='chrome', language='en-us')
    if not base_profiles:
        raise Exception("Aucun profil de base Chrome trouvé")

    selected_proxy = random.choice(SHIFTER_PROXIES)
    proxy_host = selected_proxy["host"]
    proxy_port = selected_proxy["port"]

    logger.info(f"🌐 Proxy Shifter sélectionné : {proxy_host}:{proxy_port}")

    create_profile_request = (
        BuilderForCreateProfile
        .for_base_profile(base_profiles[0].id)
        .set_name(f'profile_{random.randint(1000, 9999)}')
        .set_recommended_defaults()
        .set_proxy('socks5', Server(
            host=proxy_host,
            port=proxy_port
        ))
        .set_webgl_meta('manual', WebglMetaSpoofingOptions(
            vendor='Google Inc.',
            renderer='ANGLE (Intel(R) HD Graphics 630 Direct3D11 vs_5_0 ps_5_0)'
        ))
        .set_start_page("https://kameleo.io")
        .set_password_manager("enabled")
        .build()
    )

    profile = client.create_profile(body=create_profile_request)
    logger.info(f"Profil Kameleo créé avec ID : {profile.id}")

    client.start_profile_with_options(
        profile.id,
        WebDriverSettings(arguments=["headless", "--disable-gpu", "--no-sandbox", "--disable-images", "--disable-media-session-api"])
    )

    playwright = await async_playwright().start()
    browser_ws_endpoint = f'ws://{kameleo_host}:{kameleo_port}/playwright/{profile.id}'
    browser = await playwright.chromium.connect_over_cdp(endpoint_url=browser_ws_endpoint)
    context = browser.contexts[0]
    return browser, context, client, profile.id, playwright

async def cleanup_browser(client, profile_id, playwright, browser):
    try:
        if client and profile_id:
            client.stop_profile(profile_id)
            logger.info("Profil Kameleo arrêté")
        if browser:
            await browser.close()
        if playwright:
            await playwright.stop()
            logger.info("Playwright arrêté")
    except Exception as e:
        logger.error(f"⚠️ Erreur lors du nettoyage : {e}")

async def main():
    browser, context, client, profile_id, playwright = await setup_browser()
    try:
        page = context.pages[0]
        await page.goto("https://example.com")
        logger.info("Navigateur démarré avec succès avec le proxy Shifter en mode headless")
    finally:
        await cleanup_browser(client, profile_id, playwright, browser)

if __name__ == "__main__":
    asyncio.run(main())