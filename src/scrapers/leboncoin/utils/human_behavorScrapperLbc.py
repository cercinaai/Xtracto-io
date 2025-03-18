import random
import asyncio
import logging
import numpy as np
from playwright.async_api import Page, Locator
from loguru import logger

async def human_like_exploration(page: Page):
    """Simule une exploration aléatoire humaine plus lente."""
    if random.random() < 0.6:
        logger.info("🔍 Exploration humaine simulée...")
        elements = await page.locator("a, button, div").all()
        if elements:
            target = random.choice(elements)
            if await target.is_visible():
                box = await target.bounding_box()
                if box:
                    await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=30)  # Plus lent
                    await human_like_delay(1, 2.5, context="hover")

async def simulate_reading(page: Page):
    """Simule une pause de lecture réaliste plus longue."""
    if random.random() < 0.7:
        pause = random.uniform(3, 7)  # Plus long
        logger.info(f"📖 Pause lecture : {pause:.2f}s")
        await human_like_delay(pause, pause + 1, context="reading")

async def human_like_delay(min_time=1, max_time=3, context="generic"):
    """Simule un délai réaliste plus naturel."""
    base_delay = random.uniform(min_time, max_time)
    if context == "click_search":
        delay = np.random.normal(loc=base_delay * 1.5, scale=0.4)  # Plus lent
    elif context == "scroll":
        delay = np.random.normal(loc=base_delay * 1.0, scale=0.3)
    elif context == "hover":
        delay = np.random.normal(loc=base_delay * 1.2, scale=0.3)
    elif context == "reading":
        delay = np.random.normal(loc=base_delay, scale=0.4)
    else:
        delay = np.random.normal(loc=base_delay, scale=0.3)
    
    if random.random() < 0.2:  # 20% de chance d’hésitation
        delay += random.uniform(2, 4)  # Plus long
    delay = max(min_time, min(delay, max_time * 1.5))  # Augmentation max
    logger.info(f"⏳ Attente ({context}) : {delay:.2f}s")
    await asyncio.sleep(delay)

async def human_like_scroll_to_element(page: Page, element: str | Locator, scroll_steps=8, jitter=True, reverse=False):
    """Défilement progressif plus lent."""
    if isinstance(element, str):
        locator = page.locator(element).first
    else:
        locator = element

    if not await locator.is_visible(timeout=5000):
        logger.warning(f"⚠️ Élément {element} introuvable ou non visible.")
        return

    logger.info(f"🌀 Défilement humain vers {element} ({scroll_steps} étapes)...")
    viewport_height = await page.evaluate("window.innerHeight")
    current_scroll = await page.evaluate("window.scrollY")
    box = await locator.bounding_box()
    if not box:
        return
    target_y = box["y"] + box["height"] / 2 - viewport_height / 2

    for step in range(scroll_steps):
        step_size = (target_y - current_scroll) / (scroll_steps - step)
        if jitter:
            step_size += random.uniform(-viewport_height * 0.2, viewport_height * 0.2)
        if reverse:
            step_size = -abs(step_size)
        await page.mouse.wheel(0, int(step_size))
        await human_like_delay(0.3, 0.8, context="scroll")  # Plus lent
        current_scroll += step_size

    await locator.scroll_into_view_if_needed(timeout=2000)
    if random.random() < 0.4:
        overscroll = random.uniform(-150, -50) if not reverse else random.uniform(50, 150)
        await page.mouse.wheel(0, int(overscroll))
        await human_like_delay(0.5, 1.2, context="scroll")

async def human_like_click(page: Page, element: str | Locator, move_cursor=False, click_delay=0.5, click_variance=30, precision=0.9, retries=1):
    """Clic réaliste plus lent."""
    for attempt in range(retries + 1):
        try:
            if isinstance(element, str):
                locator = page.locator(element).first
            else:
                locator = element

            if not await locator.is_visible(timeout=5000):
                logger.warning(f"⚠️ Élément {element} introuvable ou non visible.")
                return

            box = await locator.bounding_box()
            if not box:
                return

            x = box['x'] + box['width'] * random.uniform(0.25, 0.75)
            y = box['y'] + box['height'] * random.uniform(0.25, 0.75)

            if move_cursor and random.random() < precision:
                await page.mouse.move(
                    x + random.randint(-click_variance, click_variance),
                    y + random.randint(-click_variance, click_variance),
                    steps=random.randint(20, 40)  # Plus lent
                )
                await human_like_delay(0.2, click_delay, context="hover")

            if random.random() < 0.3:
                await human_like_delay(0.6, 1.5, context="click_search")
            await page.mouse.click(
                x + random.randint(-5, 5),
                y + random.randint(-5, 5),
                delay=random.randint(100, 300)  # Plus lent
            )
            if random.random() < 0.7:
                await page.mouse.move(
                    x + random.randint(-20, 20),
                    y + random.randint(-20, 20),
                    steps=random.randint(5, 10)
                )
                await human_like_delay(0.1, 0.4, context="click_search")
            break
        except Exception as e:
            if attempt < retries:
                await human_like_delay(0.8, 1.5, context="click_search")
                continue
            logger.error(f"⚠️ Erreur clic : {e}")
            raise

async def human_like_delay_search(min_time=1, max_time=3):
    await human_like_delay(min_time, max_time)

async def human_like_scroll_to_element_search(page: Page, selector: str, scroll_steps=8, jitter=True, reverse=False):
    await human_like_scroll_to_element(page, selector, scroll_steps, jitter, reverse)

async def human_like_click_search(page: Page, selector: str, move_cursor=False, click_delay=0.5, click_variance=30, precision=0.9):
    await human_like_click(page, selector, move_cursor, click_delay, click_variance, precision, retries=1)

async def human_like_mouse_pattern(page: Page):
    """Simule des mouvements aléatoires plus lents."""
    width, height = (await page.viewport_size())["width"], (await page.viewport_size())["height"]
    logger.info("🖱️ Simulation de mouvements humains aléatoires...")
    for _ in range(random.randint(3, 7)):
        target_x = random.randint(int(width * 0.15), int(width * 0.85))
        target_y = random.randint(int(height * 0.15), int(height * 0.85))
        await page.mouse.move(
            target_x + random.randint(-30, 30),
            target_y + random.randint(-30, 30),
            steps=random.randint(30, 60)  # Plus lent
        )
        if random.random() < 0.5:
            await human_like_delay(0.6, 1.5)
        else:
            await human_like_delay(0.2, 0.7)
    if random.random() < 0.6:
        await page.mouse.wheel(0, random.randint(-100, 100))
        await human_like_delay(0.5, 1.2)
        
async def human_like_scroll_to_element(page: Page, element: str | Locator, scroll_steps=6, jitter=True, reverse=False):
    """Défilement progressif avec variabilité humaine, adapté au navigateur."""
    try:
        if isinstance(element, str):
            locator = page.locator(element).first
        elif isinstance(element, Locator):
            locator = element
        else:
            raise ValueError("L'élément doit être un sélecteur string ou un Locator")

        if not await locator.is_visible(timeout=5000):
            logger.warning(f"⚠️ Élément {element} introuvable ou non visible.")
            return

        logger.info(f"🌀 Défilement humain vers {element} ({scroll_steps} étapes)...")
        viewport_height = await page.evaluate("window.innerHeight")
        current_scroll = await page.evaluate("window.scrollY")

        # Calculer la position cible
        box = await locator.bounding_box()
        if not box:
            logger.warning(f"⚠️ Impossible de calculer la position de {element}.")
            return
        target_y = box["y"] + box["height"] / 2 - viewport_height / 2

        # Simuler un défilement progressif avec des variations
        for step in range(scroll_steps):
            step_size = (target_y - current_scroll) / (scroll_steps - step)
            if jitter:
                step_size += random.uniform(-viewport_height * 0.15, viewport_height * 0.15)
            if reverse:
                step_size = -abs(step_size)

            await page.mouse.wheel(0, int(step_size))  # Utilisation de wheel pour simuler le scroll
            await human_like_delay(0.2, 0.6, context="scroll")
            current_scroll += step_size

        # Ajustement final
        await locator.scroll_into_view_if_needed(timeout=2000)

        # Simuler un léger overscroll ou ajustement
        if random.random() < 0.35:
            overscroll = random.uniform(-120, -40) if not reverse else random.uniform(40, 120)
            await page.mouse.wheel(0, int(overscroll))
            await human_like_delay(0.3, 0.8, context="scroll")

        await human_like_delay(0.5, 1.5, context="scroll")  # Pause naturelle après arrivée

    except Exception as e:
        logger.error(f"⚠️ Erreur défilement : {e}")
        