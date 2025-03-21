import asyncio
import platform
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.api.apis import api_router
from src.database.database import init_db, close_db
from src.api.cron import start_cron
from loguru import logger
import uvicorn
from contextlib import asynccontextmanager
import os
import sys

if not os.path.exists("logs/leboncoin"):
    os.makedirs("logs/leboncoin")
if not os.path.exists("logs/capture/leboncoin"):
    os.makedirs("logs/capture/leboncoin")

logger.remove()
logger.add(sys.stdout, level="DEBUG")
logger.add(
    "logs/leboncoin/leboncoin_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="7 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await init_db()
        logger.success("✅ Connexion aux bases de données établie avec succès")
        asyncio.create_task(start_cron())
        logger.info("🚀 Serveur démarré sur http://0.0.0.0:8002")
    except Exception as e:
        logger.critical(f"🚨 Erreur critique au démarrage : {e}")
        raise SystemExit(1)
    
    yield
    
    try:
        await close_db()
        logger.info("🔌 Connexion aux bases de données fermée proprement")
    except Exception as e:
        logger.error(f"⚠️ Erreur lors de la fermeture : {e}")

app = FastAPI(
    title="Xtracto-io Async Scraper API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api/v1")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8002,
        log_level="info",
        workers=2,
        timeout_keep_alive=120,
        reload=True
    )