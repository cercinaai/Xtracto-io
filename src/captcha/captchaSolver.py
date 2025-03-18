# src/scrapers/captcha/captcha_solver.py
import logging
import random
import requests
import asyncio
from pydub import AudioSegment
import io
import speech_recognition as sr
from bs4 import BeautifulSoup
from word2number import w2n
from playwright.async_api import Page, TimeoutError  # Updated to async API
logger = logging.getLogger(__name__)



def correct_digit_sequence(digits, expected_length=6):
    if len(digits) <= expected_length:
        return digits
    corrected_digits = ""
    i = 0
    while i < len(digits):
        if i + 2 < len(digits) and digits[i] == digits[i+1] == digits[i+2]:
            corrected_digits += digits[i:i+2]
            i += 3
            logger.warning(f":danger: Détection de répétition suspecte ({digits[i-3:i]}), corrigé en {corrected_digits[-2:]}")
        else:
            corrected_digits += digits[i]
            i += 1
    if len(corrected_digits) > expected_length:
        logger.warning(f":danger: Trop de chiffres ({len(corrected_digits)}), troncature à {expected_length} chiffres : {corrected_digits[:expected_length]}")
        return corrected_digits[:expected_length]
    return corrected_digits

async def solve_audio_captcha(page: Page) -> bool:
    """Résout un CAPTCHA audio DataDome en mode asynchrone et retourne True si réussi."""
    try:
        captcha_frame = page.locator('iframe[title="DataDome CAPTCHA"]')
        if not await captcha_frame.is_visible(timeout=5000):
            logger.info(":coche_blanche: Aucun CAPTCHA détecté.")
            return True
        logger.info(":danger: CAPTCHA DataDome détecté, passage au mode audio...")
        frame = page.frame_locator('iframe[title="DataDome CAPTCHA"]').first
        audio_button = frame.locator('#captcha__audio__button')
        await audio_button.wait_for(state="visible", timeout=10000)
        button_box = await audio_button.bounding_box()
        if button_box:
            x = button_box['x'] + button_box['width'] / 2
            y = button_box['y'] + button_box['height'] / 2
            logger.info(f":épingle2: Coordonnées du bouton audio : x={x}, y={y}")
            await page.mouse.move(x, y, steps=random.randint(10, 20))
            await asyncio.sleep(random.uniform(0.5, 1))
            await page.mouse.click(x, y)
            await asyncio.sleep(random.uniform(1, 2))
            logger.info(":coche_blanche: Passé au CAPTCHA audio avec la souris.")
        else:
            logger.error(":x: Impossible de récupérer les coordonnées du bouton audio.")
            return False

        play_button = frame.locator('button.audio-captcha-play-button')
        await play_button.wait_for(state="visible", timeout=10000)
        play_box = await play_button.bounding_box()
        if play_box:
            x = play_box['x'] + play_box['width'] / 2
            y = play_box['y'] + play_box['height'] / 2
            logger.info(f":épingle2: Coordonnées du bouton play : x={x}, y={y}")
            await page.mouse.move(x, y, steps=random.randint(10, 20))
            await asyncio.sleep(random.uniform(0.5, 1))
            await page.mouse.click(x, y)
            start_time = asyncio.get_event_loop().time()
            logger.info(":coche_blanche: Bouton play cliqué, démarrage de l’audio...")
            inputs = frame.locator('.audio-captcha-inputs')
            input_count = await inputs.count()
            first_input = inputs.nth(0)
            await first_input.click()
            logger.info(":coche_blanche: Premier champ d’entrée focalisé.")
        else:
            logger.error(":x: Impossible de récupérer les coordonnées du bouton play.")
            return False

        captcha_frame_div = frame.locator('#captcha__frame')
        html_content = await captcha_frame_div.inner_html()
        soup = BeautifulSoup(html_content, 'html.parser')
        audio_tag = soup.find('audio', class_='audio-captcha-track')
        if audio_tag and audio_tag.get('src'):
            audio_url = audio_tag['src']
            logger.info(f":antenne_satellite: URL audio trouvée via BeautifulSoup : {audio_url}")
        else:
            logger.error(":x: Aucun élément <audio> trouvé dans le HTML.")
            logger.debug(f"Contenu brut de #captcha__frame : {html_content}")
            return False

        audio_file = "captcha_audio.wav"
        response = await asyncio.to_thread(requests.get, audio_url, timeout=10)
        with open(audio_file, 'wb') as f:
            f.write(response.content)
        logger.info(f":coche_blanche: Audio téléchargé dans : {audio_file}")
        audio = AudioSegment.from_wav(audio_file)
        audio.export(audio_file, format="wav")
        recognizer = sr.Recognizer()
        with sr.AudioFile(audio_file) as source:
            audio_data = recognizer.record(source)
            try:
                text = recognizer.recognize_google(audio_data, language="en-US")
                logger.info(f":note: Contenu brut de l’audio : {text}")
                words = text.split()
                digits = ""
                for word in words:
                    try:
                        num = w2n.word_to_num(word)
                        digits += str(num)
                    except ValueError:
                        continue
                if not digits:
                    logger.error(":x: Aucun chiffre valide extrait du texte.")
                    return False
                digits = correct_digit_sequence(digits, expected_length=input_count)
                logger.info(f":micro_de_studio: Chiffres corrigés pour {input_count} champs : {digits}")
            except sr.UnknownValueError:
                logger.error(":x: Impossible de comprendre l’audio du CAPTCHA.")
                return False
            except sr.RequestError as e:
                logger.error(f":x: Erreur avec le service de reconnaissance vocale : {e}")
                return False

        timing = [5, 8, 10, 12, 14, 17]
        if len(digits) != len(timing):
            logger.error(f":x: Nombre de chiffres ({len(digits)}) ne correspond pas aux délais ({len(timing)}).")
            return False
        for i, digit in enumerate(digits):
            elapsed = asyncio.get_event_loop().time() - start_time
            delay = timing[i] - elapsed
            if delay > 0:
                await asyncio.sleep(delay)
            else:
                logger.warning(f":danger: Délai négatif pour chiffre {i} ({digit}), saisie immédiate.")
            frame = page.frame_locator('iframe[title="DataDome CAPTCHA"]').first
            inputs = frame.locator('.audio-captcha-inputs')
            input_field = inputs.nth(i)
            box = await input_field.bounding_box()
            x_pos = box['x'] + random.randint(5, 15)
            y_pos = box['y'] + random.randint(5, 15)
            await page.mouse.move(x_pos, y_pos, steps=10)
            await input_field.click()
            await page.keyboard.type(digit, delay=random.uniform(0.1, 0.3))
            logger.info(f":écriture: Remplissage de l’entrée {i} avec le chiffre : {digit} à t={timing[i]}s")
            await asyncio.sleep(0.5)
        logger.info(":sablier_avec_écoulement: Attente de la validation automatique...")
        await asyncio.sleep(5)
        current_url = page.url
        page_content = await page.content()
        if "blocked" in page_content.lower() or "captcha" in current_url.lower():
            logger.error(":x: Toujours bloqué après saisie correcte.")
            logger.debug(f":page_imprimée: Contenu de la page : {page_content[:500]}...")
            return False
        else:
            logger.info(":coche_blanche: Accès à la page cible détecté.")
            return True
    except Exception as e:
        logger.error(f":x: Erreur lors de la résolution du CAPTCHA : {e}")
        return False