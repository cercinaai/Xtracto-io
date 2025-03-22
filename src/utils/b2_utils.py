"""
Module b2_utils.py
Fournit des utilitaires pour télécharger, recadrer et uploader des images sur Backblaze B2, avec gestion asynchrone et retries.
"""

import asyncio
import aiohttp
import cv2
import numpy as np
from typing import Optional
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from b2sdk.v2.exception import B2Error
from src.config.settings import B2_BUCKET_NAME, B2_ACCESS_KEY, B2_SECRET_KEY
from loguru import logger
from concurrent.futures import ThreadPoolExecutor

# Désactiver les logs inutiles de b2sdk
import logging
logging.getLogger('b2sdk').setLevel(logging.ERROR)

# Semaphore pour limiter les uploads concurrents
UPLOAD_SEMAPHORE = asyncio.Semaphore(5)

# Pool de threads pour les opérations OpenCV
EXECUTOR = ThreadPoolExecutor(max_workers=10)

async def get_b2_api() -> B2Api:
    """Obtient une instance asynchrone de l'API B2 avec gestion des retries."""
    b2_api = B2Api(InMemoryAccountInfo())
    for attempt in range(3):
        try:
            await asyncio.to_thread(b2_api.authorize_account, "production", B2_ACCESS_KEY, B2_SECRET_KEY)
            return b2_api
        except B2Error as e:
            if attempt < 2:
                await asyncio.sleep(0.5 * (2 ** attempt))
                continue
            logger.error(f"Échec définitif de l'authentification B2 après 3 tentatives : {e}")
            return None
    return None

def detect_watermark_in_corner(gray_image: np.ndarray, corner: str, threshold_range: tuple = (100, 250), 
                              min_area: int = 20, max_area: int = 3000) -> Optional[tuple]:
    """Détecte un filigrane dans un coin spécifique de l'image."""
    if gray_image is None or gray_image.size == 0:
        return None

    blurred = cv2.GaussianBlur(gray_image, (3, 3), 0)
    best_contours = []
    for threshold in range(threshold_range[0], threshold_range[1] + 1, 10):
        _, binary = cv2.threshold(blurred, threshold, 255, cv2.THRESH_BINARY_INV)
        contours, _ = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        for contour in contours:
            area = cv2.contourArea(contour)
            if min_area <= area <= max_area:
                best_contours.append(contour)

    if not best_contours:
        adaptive = cv2.adaptiveThreshold(blurred, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                                        cv2.THRESH_BINARY_INV, 11, 2)
        contours, _ = cv2.findContours(adaptive, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        for contour in contours:
            area = cv2.contourArea(contour)
            if min_area <= area <= max_area:
                best_contours.append(contour)

    best_contours = sorted(best_contours, key=cv2.contourArea, reverse=True)
    height, width = gray_image.shape
    margin = min(height, width) // 4

    for contour in best_contours:
        x, y, w, h = cv2.boundingRect(contour)
        if corner == 'top_left' and x < margin and y < margin:
            return (x, y, w, min(h + 20, height - y))
        elif corner == 'top_right' and x > width - margin - w and y < margin:
            return (x, y, w, min(h + 20, height - y))
        elif corner == 'bottom_left' and x < margin and y > height - margin - h:
            return (x, y, w, min(h + 20, height - y))
        elif corner == 'bottom_right' and x > width - margin - w and y > height - margin - h:
            return (x, y, w, min(h + 20, height - y))
    return None

async def crop_watermark_from_image(image_buffer: bytes) -> bytes:
    """Recadre une image pour supprimer les filigranes en haut ou en bas, retourne None si échec."""
    if not image_buffer or len(image_buffer) == 0:
        logger.debug("Buffer d'image vide ou invalide")
        return None

    try:
        # Décoder l'image dans un thread
        original_image = await asyncio.to_thread(
            cv2.imdecode, np.frombuffer(image_buffer, np.uint8), cv2.IMREAD_COLOR
        )
        if original_image is None or original_image.size == 0:
            logger.debug("Impossible de charger l'image")
            return None

        height, width = original_image.shape[:2]
        gray_image = await asyncio.to_thread(cv2.cvtColor, original_image, cv2.COLOR_BGR2GRAY)

        # Détection des filigranes
        top_detection = None
        bottom_detection = None
        for corner in ['top_left', 'top_right']:
            coords = detect_watermark_in_corner(gray_image, corner)
            if coords:
                top_detection = coords
                break
        for corner in ['bottom_left', 'bottom_right']:
            coords = detect_watermark_in_corner(gray_image, corner)
            if coords:
                bottom_detection = coords
                break

        # Calcul des zones à couper
        top_cut = max(top_detection[3], 20) if top_detection else 20
        bottom_cut = max(bottom_detection[3], 20) if bottom_detection else 20
        new_top = min(top_cut, height // 2)
        new_bottom = height - min(bottom_cut, height // 2)

        if new_top >= new_bottom:
            cropped = original_image
        else:
            cropped = original_image[new_top:new_bottom, :]

        # Encoder dans un thread
        _, cropped_buffer = await asyncio.to_thread(cv2.imencode, '.jpg', cropped)
        if cropped_buffer is None or len(cropped_buffer) == 0:
            logger.debug("Échec de l'encodage JPEG")
            return None
        return cropped_buffer.tobytes()

    except Exception as e:
        logger.debug(f"Erreur non bloquante lors du recadrage : {e}")
        return None

async def upload_image_to_b2(image_url: str, filename: str, target: str = "real_estate") -> str:
    """Télécharge, recadre et uploade une image sur B2, retourne 'N/A' si échec."""
    async with UPLOAD_SEMAPHORE:
        try:
            if not image_url.startswith('http'):
                return "N/A"

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    image_url,
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
                    timeout=aiohttp.ClientTimeout(total=5)  # Réduit à 5s pour accélérer
                ) as response:
                    if response.status == 404:
                        return "N/A"
                    response.raise_for_status()
                    image_data = await response.read()
                    if not image_data:
                        return "N/A"

            cropped_buffer = await crop_watermark_from_image(image_data)
            if cropped_buffer is None:
                return "N/A"

            b2_api = await get_b2_api()
            if b2_api is None:
                return "N/A"
            bucket = await asyncio.to_thread(b2_api.get_bucket_by_name, B2_BUCKET_NAME)
            target_name = f"{target}/{filename}"
            await asyncio.to_thread(bucket.upload_bytes, cropped_buffer, target_name, content_type='image/jpeg')
            return f"https://f003.backblazeb2.com/file/{B2_BUCKET_NAME}/{target_name}"

        except Exception as e:
            logger.debug(f"Échec non bloquant upload image {image_url} : {e}")
            return "N/A"

def sanitize_filename(filename: str) -> str:
    """Nettoie le nom du fichier."""
    if not filename:
        return "default_image.jpg"
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in filename)