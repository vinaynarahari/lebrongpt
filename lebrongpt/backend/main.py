from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict
import logging
from kaggle_data import KaggleDataFetcher

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Kaggle data fetcher
kaggle_fetcher = KaggleDataFetcher()

class PlayerStats(BaseModel):
    player_name: str
    stats: Dict

class ComparisonRequest(BaseModel):
    player1: str
    player2: str

@app.get("/")
async def root():
    return {"message": "Welcome to LeBronGPT API"}

@app.get("/player/{player_name}", response_model=PlayerStats)
async def get_player_stats(player_name: str):
    try:
        stats = kaggle_fetcher.get_player_stats(player_name)
        if stats is None:
            raise HTTPException(status_code=404, detail="Player not found")
        return PlayerStats(player_name=player_name, stats=stats)
    except Exception as e:
        logger.error(f"Error getting player stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/compare", response_model=Dict)
async def compare_players(request: ComparisonRequest):
    try:
        comparison = kaggle_fetcher.get_comparison_stats(request.player1, request.player2)
        if comparison is None:
            raise HTTPException(status_code=404, detail="One or both players not found")
        return comparison
    except Exception as e:
        logger.error(f"Error comparing players: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 