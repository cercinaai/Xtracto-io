import asyncio
import random
from datetime import datetime, time
from src.scrapers.leboncoin.image_processor import process_and_transfer_images
from src.scrapers.leboncoin.firstScrapper import open_leboncoin
from src.scrapers.leboncoin.leboncoinLoopScrapper import open_leboncoin_loop
from multiprocessing import Process, Queue
from loguru import logger

# Define the time window for scraping (10:00 AM to 10:00 PM)
SCRAPING_START_TIME = time(10, 0)  # 10:00 AM
SCRAPING_END_TIME = time(22, 0)   # 10:00 PM

def is_within_scraping_window() -> bool:
    """Check if the current time is within the scraping window (10:00 AM to 10:00 PM)."""
    now = datetime.now().time()
    return SCRAPING_START_TIME <= now <= SCRAPING_END_TIME

async def run_scraper_in_process(func, task_name: str) -> dict:
    """Run a scraper function in a separate process and return the result."""
    queue = Queue()
    process = Process(target=func, args=(queue,))
    process.start()
    process.join()
    if not queue.empty():
        return queue.get()
    return {"status": "error", "message": f"{task_name} did not return a result"}

async def first_scraper_task():
    """Run the firstScraper (open_leboncoin) with retries and scheduling."""
    while True:
        # Wait until 10:00 AM to start
        now = datetime.now()
        start_time_today = datetime.combine(now.date(), SCRAPING_START_TIME)
        if now.time() < SCRAPING_START_TIME:
            seconds_until_start = (start_time_today - now).total_seconds()
            logger.info(f"⏳ Waiting until 10:00 AM to start firstScraper (in {seconds_until_start:.0f} seconds)...")
            await asyncio.sleep(seconds_until_start)

        # Run the scraper if within the time window
        while is_within_scraping_window():
            logger.info("🚀 Launching firstScraper (open_leboncoin)...")
            max_retries = 3
            for attempt in range(max_retries):
                result = await run_scraper_in_process(open_leboncoin, "firstScraper")
                if result["status"] == "success":
                    logger.info("✅ firstScraper completed successfully.")
                    break
                else:
                    logger.error(f"⚠️ firstScraper failed (attempt {attempt + 1}/{max_retries}): {result['message']}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(5)  # Wait 5 seconds before retrying
                    else:
                        logger.error("❌ firstScraper failed after all retries. Will retry after 30 minutes.")

            # Wait 30 minutes before the next run, but only if still within the time window
            logger.info("⏳ Waiting 30 minutes before relaunching firstScraper...")
            await asyncio.sleep(30 * 60)  # 30 minutes

        # If outside the time window, wait until the next day at 10:00 AM
        now = datetime.now()
        next_day = now.replace(hour=SCRAPING_START_TIME.hour, minute=SCRAPING_START_TIME.minute, second=0, microsecond=0)
        if now.time() >= SCRAPING_START_TIME:
            next_day = next_day.replace(day=next_day.day + 1)
        seconds_until_next = (next_day - now).total_seconds()
        logger.info(f"⏳ Outside scraping window. Waiting until tomorrow 10:00 AM (in {seconds_until_next:.0f} seconds)...")
        await asyncio.sleep(seconds_until_next)

async def loop_scraper_task():
    """Run the loopScraper (open_leboncoin_loop) between 10:00 AM and 10:00 PM with a 2-5 minute delay between runs."""
    # Wait 5 minutes after 10:00 AM to start (i.e., start at 10:05 AM)
    now = datetime.now()
    start_time_today = datetime.combine(now.date(), SCRAPING_START_TIME)
    first_run_time = start_time_today.replace(minute=5)  # 10:05 AM
    if now < first_run_time:
        seconds_until_start = (first_run_time - now).total_seconds()
        logger.info(f"⏳ Waiting until 10:05 AM to start loopScraper (in {seconds_until_start:.0f} seconds)...")
        await asyncio.sleep(seconds_until_start)

    while True:
        # Run the scraper if within the time window
        while is_within_scraping_window():
            logger.info("🔄 Launching loopScraper (open_leboncoin_loop)...")
            result = await run_scraper_in_process(open_leboncoin_loop, "loopScraper")
            if result["status"] == "success":
                logger.info("✅ loopScraper completed successfully.")
            else:
                logger.error(f"⚠️ loopScraper failed: {result['message']}")

            # Wait 2-5 minutes before the next run
            wait_time = random.uniform(2 * 60, 5 * 60)  # Random delay between 2 and 5 minutes
            logger.info(f"⏳ Waiting {wait_time/60:.1f} minutes before relaunching loopScraper...")
            await asyncio.sleep(wait_time)

        # If outside the time window, wait until the next day at 10:05 AM
        now = datetime.now()
        next_day = now.replace(hour=SCRAPING_START_TIME.hour, minute=5, second=0, microsecond=0)
        if now.time() >= SCRAPING_START_TIME:
            next_day = next_day.replace(day=next_day.day + 1)
        seconds_until_next = (next_day - now).total_seconds()
        logger.info(f"⏳ Outside scraping window. Waiting until tomorrow 10:05 AM (in {seconds_until_next:.0f} seconds)...")
        await asyncio.sleep(seconds_until_next)

async def start_cron():
    """Start the cron jobs for image processing, firstScraper, and loopScraper."""
    # Start the image processing task (runs continuously)
    asyncio.create_task(process_and_transfer_images())
    logger.info("📸 Started continuous image processing task.")

    # Start the firstScraper task (runs at 10:00 AM, relaunches every 30 minutes)
    asyncio.create_task(first_scraper_task())
    logger.info("⏰ Scheduled firstScraper to start at 10:00 AM.")

    # Start the loopScraper task (runs at 10:05 AM, loops every 2-5 minutes until 10:00 PM)
    asyncio.create_task(loop_scraper_task())
    logger.info("⏰ Scheduled loopScraper to start at 10:05 AM and run until 10:00 PM.")

if __name__ == "__main__":
    asyncio.run(start_cron())