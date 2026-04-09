"""FastAPI application entry point."""

import logging
import os
import sys

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .config import settings
from .routers import box_room, cages, calendar, experiments, scheduling

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Experiment Queue API",
    description="REST API for experiment queue management",
    version="0.1.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "https://experiment-queue.rodentparty.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(experiments.router, prefix="/api/experiments", tags=["experiments"])
app.include_router(scheduling.router, prefix="/api/scheduling", tags=["scheduling"])
app.include_router(calendar.router, prefix="/api/calendar", tags=["calendar"])
app.include_router(cages.router, prefix="/api/cages", tags=["cages"])
app.include_router(box_room.router, prefix="/api/box-room", tags=["box-room"])


@app.get("/api/health")
def health_check() -> dict:
    """Health check endpoint."""
    return {"status": "ok"}


# Mount static files for production (built React app)
ui_dist = os.path.join(os.path.dirname(__file__), "..", "..", "ui", "dist")
if os.path.isdir(ui_dist):
    app.mount("/", StaticFiles(directory=ui_dist, html=True), name="ui")
