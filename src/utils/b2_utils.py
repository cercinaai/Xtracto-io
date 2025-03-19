import asyncio
import requests
import cv2
import numpy as np
from typing import Optional
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from src.config.settings import B2_BUCKET_NAME, B2_ACCESS_KEY, B2_SECRET_KEY
from loguru import logger

async def get_b2_api() -> B2Api:
    b2_api = B2Api(InMemoryAccountInfo())
    await asyncio.to_thread(b2_api.authorize_account, "production", B2_ACCESS_KEY, B2_SECRET_KEY)
    return b2_api

def crop_watermark_from_image(image_buffer: bytes, max_cut: int = 50) -> bytes:
    if not image_buffer or len(image_buffer) == 0:
        raise ValueError("Buffer d'image vide ou invalide")
    original_image = cv2.imdecode(np.frombuffer(image_buffer, np.uint8), cv2.IMREAD_COLOR)
    if original_image is None or original_image.size == 0:
        raise ValueError("Impossible de charger l'image")
    height, width = original_image.shape[:2]
    gray_image = cv2.cvtColor(original_image, cv2.COLOR_BGR2GRAY)
    _, bw_image = cv2.threshold(gray_image, 128, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)
    top_detection = None
    bottom_detection = None
    for corner in ['top_left', 'top_right']:
        coords = detect_watermark_in_corner(bw_image, corner)
        if coords:
            top_detection = coords
            break
    for corner in ['bottom_left', 'bottom_right']:
        coords = detect_watermark_in_corner(bw_image, corner)
        if coords:
            bottom_detection = coords
            break
    removal = min(top_detection[3] if top_detection else 0, max_cut) or min(bottom_detection[3] if bottom_detection else 0, max_cut)
    new_top = removal
    new_bottom = height - removal
    cropped = original_image[new_top:new_bottom, :] if new_top < new_bottom else original_image
    _, cropped_buffer = cv2.imencode('.jpg', cropped)
    if cropped_buffer is None or len(cropped_buffer) == 0:
        raise ValueError("Échec de l'encodage JPEG")
    return cropped_buffer.tobytes()

def detect_watermark_in_corner(gray_image: np.ndarray, corner: str, threshold_range: tuple = (100, 250), min_area: int = 20, max_area: int = 3000) -> Optional[tuple]:
    if gray_image is None or gray_image.size == 0:
        return None
    blurred = cv2.GaussianBlur(gray_image, (3, 3), 0)
    best_contours = []
    for threshold in range(threshold_range[0], threshold_range[1] + 1, 5):
        _, binary = cv2.threshold(blurred, threshold, 255, cv2.THRESH_BINARY_INV)
        kernel = np.ones((5, 5), np.uint8)
        binary = cv2.dilate(binary, kernel, iterations=3)
        binary = cv2.erode(binary, kernel, iterations=1)
        contours, _ = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        for contour in contours:
            area = cv2.contourArea(contour)
            if min_area <= area <= max_area:
                best_contours.append(contour)
    if not best_contours:
        adaptive = cv2.adaptiveThreshold(blurred, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 11, 2)
        kernel_adapt = np.ones((3, 3), np.uint8)
        adaptive = cv2.morphologyEx(adaptive, cv2.MORPH_CLOSE, kernel_adapt, iterations=2)
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
            return (max(0, x - 10), max(0, y - 10), min(w + 20, width - x), min(h + 20, height - y))
        elif corner == 'top_right' and x > width - margin - w and y < margin:
            return (max(0, x - 10), max(0, y - 10), min(w + 20, width - x), min(h + 20, height - y))
        elif corner == 'bottom_left' and x < margin and y > height - margin - h:
            return (max(0, x - 10), max(0, y - 10), min(w + 20, width - x), min(h + 20, height - y))
        elif corner == 'bottom_right' and x > width - margin - w and y > height - margin - h:
            return (max(0, x - 10), max(0, y - 10), min(w + 20, width - x), min(h + 20, height - y))
    return None

   
async def upload_image_to_b2(image_url: str, filename: str, target: str = "real_estate") -> str:
    max_retries = 3
    backoff_factor = 0.5
    for attempt in range(max_retries):
        try:
            if not image_url.startswith('http'):
                raise ValueError("URL invalide")
            logger.debug(f"📥 Téléchargement de l'image {image_url}")
            response = await asyncio.to_thread(
                requests.get,
                image_url,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
                timeout=10
            )
            response.raise_for_status()
            if not response.content:
                logger.warning(f"⚠️ Contenu vide pour l'image {image_url}")
                return "N/A"
            logger.debug(f"✂️ Suppression du filigrane pour {image_url}")
            cropped_buffer = crop_watermark_from_image(response.content)
            b2_api = await get_b2_api()
            bucket = await asyncio.to_thread(b2_api.get_bucket_by_name, B2_BUCKET_NAME)
            target_name = f"{target}/{filename}"
            logger.debug(f"📤 Upload vers Backblaze: {target_name}")
            await asyncio.to_thread(bucket.upload_bytes, cropped_buffer, target_name, content_type='image/jpeg')
            uploaded_url = f"https://f003.backblazeb2.com/file/{B2_BUCKET_NAME}/{target_name}"
            logger.debug(f"✅ Image uploadée avec succès: {uploaded_url}")
            return uploaded_url
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"⚠️ Tentative {attempt + 1}/{max_retries} échouée pour {image_url}: {str(e)}")
                await asyncio.sleep(backoff_factor * (2 ** attempt))
                continue
            logger.error(f"❌ Échec définitif pour {image_url} après {max_retries} tentatives: {str(e)}")
            return "N/A"