from fastapi import FastAPI, HTTPException, Query
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

def normalize_player_name(name: str) -> str:
    """Normalize player name by removing spaces and converting to lowercase"""
    return name.replace(" ", "").lower()

@app.get("/")
async def root():
    return {"message": "Welcome to LeBronGPT API"}

@app.get("/player/{player_name}", response_model=PlayerStats)
async def get_player_stats(player_name: str):
    try:
        # Normalize the player name
        normalized_name = normalize_player_name(player_name)
        
        # Get all player names from the dataset
        all_players = kaggle_fetcher.get_all_players()
        
        # Find the matching player name (case-insensitive)
        matching_player = None
        for player in all_players:
            if normalize_player_name(player) == normalized_name:
                matching_player = player
                break
        
        if not matching_player:
            raise HTTPException(status_code=404, detail="Player not found")
        
        stats = kaggle_fetcher.get_player_stats(matching_player)
        if stats is None:
            raise HTTPException(status_code=404, detail="Player not found")
        return PlayerStats(player_name=matching_player, stats=stats)
    except Exception as e:
        logger.error(f"Error getting player stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/compare", response_model=Dict)
async def compare_players_get(
    player1: str = Query(..., description="First player name"),
    player2: str = Query(..., description="Second player name")
):
    try:
        # Normalize player names
        normalized_player1 = normalize_player_name(player1)
        normalized_player2 = normalize_player_name(player2)
        
        # Get all player names from the dataset
        all_players = kaggle_fetcher.get_all_players()
        
        # Find matching player names
        matching_player1 = None
        matching_player2 = None
        for player in all_players:
            if normalize_player_name(player) == normalized_player1:
                matching_player1 = player
            if normalize_player_name(player) == normalized_player2:
                matching_player2 = player
        
        if not matching_player1 or not matching_player2:
            raise HTTPException(status_code=404, detail="One or both players not found")
        
        comparison = kaggle_fetcher.get_comparison_stats(matching_player1, matching_player2)
        if comparison is None:
            raise HTTPException(status_code=404, detail="One or both players not found")
        return comparison
    except Exception as e:
        logger.error(f"Error comparing players: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/compare", response_model=Dict)
async def compare_players_post(request: ComparisonRequest):
    try:
        # Normalize player names
        normalized_player1 = normalize_player_name(request.player1)
        normalized_player2 = normalize_player_name(request.player2)
        
        # Get all player names from the dataset
        all_players = kaggle_fetcher.get_all_players()
        
        # Find matching player names
        matching_player1 = None
        matching_player2 = None
        for player in all_players:
            if normalize_player_name(player) == normalized_player1:
                matching_player1 = player
            if normalize_player_name(player) == normalized_player2:
                matching_player2 = player
        
        if not matching_player1 or not matching_player2:
            raise HTTPException(status_code=404, detail="One or both players not found")
        
        comparison = kaggle_fetcher.get_comparison_stats(matching_player1, matching_player2)
        if comparison is None:
            raise HTTPException(status_code=404, detail="One or both players not found")
        return comparison
    except Exception as e:
        logger.error(f"Error comparing players: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 