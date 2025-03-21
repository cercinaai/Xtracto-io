import asyncio
from datetime import datetime
from typing import Optional

from fastapi import BackgroundTasks

from src.database.database import init_db
from src.scrapers.leboncoin.firstScrapper import open_leboncoin
from src.scrapers.leboncoin.leboncoinLoopScrapper import open_leboncoin_loop
from src.scrapers.leboncoin.agenceBrute_scraper import scrape_agences
from src.scrapers.leboncoin.agence_notexisting import scrape_annonce_agences
from src.scrapers.leboncoin.image_processor import process_and_transfer_images
import logging

logger = logging.getLogger(__name__)

running_tasks = {
    "first_scraper": False,
    "loop_scraper": False,
    "agence_brute": False,
    "agence_notexisting": False,
    "process_and_transfer": False
}

async def first_scraper_task():
    logger.info("🚀 Lancement de firstScraper...")
    while True:
        current_hour = datetime.now().hour
        if 10 <= current_hour < 22:  # Entre 10h00 et 22h00
            await firstScraper()
        await asyncio.sleep(60 * 60)  # Vérifie toutes les heures

async def loop_scraper_task():
    logger.info("⏳ Début de l'attente initiale de 5 minutes pour loopScraper...")
    await asyncio.sleep(5 * 60)  # Attendre 5 minutes au démarrage
    logger.info("✅ Fin de l'attente initiale, boucle de loopScraper démarrée.")
    while True:
        try:
            current_hour = datetime.now().hour
            if 10 <= current_hour < 22:  # Entre 10h05 et 22h00
                logger.info("🔄 Lancement de loopScraper...")
                await loopScraper()
            else:
                logger.debug("⏰ Hors plage horaire pour loopScraper (10h-22h).")
            await asyncio.sleep(5 * 60)  # Attendre 5 minutes entre chaque tentative
        except Exception as e:
            logger.error(f"⚠️ Erreur dans loop_scraper_task: {e}")
            await asyncio.sleep(5 * 60)  # Continuer après une erreur

async def agence_brute_task():
    while True:
        current_hour = datetime.now().hour
        if current_hour < 10 or current_hour >= 22:  # Entre 22h00 et 10h00
            logger.info("🔧 Lancement de agenceBrute_scraper...")
            await agenceBrute_scraper()
        await asyncio.sleep(60 * 60)

async def agence_notexisting_task():
    while True:
        current_hour = datetime.now().hour
        if current_hour < 10 or current_hour >= 22:  # Entre 22h00 et 10h00
            logger.info("🕵️ Lancement de agence_notexisting...")
            await agence_notexisting()
        await asyncio.sleep(60 * 60)

async def start_cron():
    logger.info("📸 Lancement continu du traitement des images...")
    asyncio.create_task(process_and_transfer_images())
    
    logger.info("⏰ Planification de firstScraper pour 10h00-22h00...")
    asyncio.create_task(first_scraper_task())
    
    logger.info("⏰ Planification de loopScraper pour 10h05-22h00...")
    asyncio.create_task(loop_scraper_task())
    
    logger.info("⏰ Planification de agenceBrute_scraper pour 22h00-10h00...")
    asyncio.create_task(agence_brute_task())
    
    logger.info("⏰ Planification de agence_notexisting pour 22h00-10h00...")
    asyncio.create_task(agence_notexisting_task())

    # Boucle de surveillance pour vérifier que les tâches sont actives
    while True:
        await asyncio.sleep(60)
        logger.debug(f"🟢 Cron actif. Tâches planifiées: {list(running_tasks.keys())}")

def start_cron_in_background(background_tasks: BackgroundTasks):
    background_tasks.add_task(start_cron)