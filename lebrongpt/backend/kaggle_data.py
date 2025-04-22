import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KaggleDataFetcher:
    def __init__(self):
        self.api = KaggleApi()
        self.api.authenticate()
        self.dataset = "eoinamoore/historical-nba-data-and-player-box-scores"
        self.file_name = "PlayerStatistics.csv"
        self._cache = None
        self._last_fetch = None

    def _fetch_data(self) -> pd.DataFrame:
        """Fetch the latest data from Kaggle"""
        try:
            # Download the dataset
            self.api.dataset_download_file(
                self.dataset,
                self.file_name,
                path="/tmp",
                force=True
            )
            
            # Read the CSV file
            df = pd.read_csv(f"/tmp/{self.file_name}")
            
            # Clean up the downloaded file
            os.remove(f"/tmp/{self.file_name}")
            
            return df
        except Exception as e:
            logger.error(f"Error fetching data from Kaggle: {str(e)}")
            raise

    def get_player_stats(self, player_name: str) -> Optional[Dict]:
        """Get statistics for a specific player"""
        try:
            # Fetch fresh data if cache is empty or older than 1 hour
            if self._cache is None or self._last_fetch is None:
                self._cache = self._fetch_data()
                self._last_fetch = pd.Timestamp.now()
            
            # Search for player (case insensitive)
            player_data = self._cache[
                self._cache['Player'].str.lower() == player_name.lower()
            ]
            
            if player_data.empty:
                return None
                
            # Get the most recent season's data
            latest_season = player_data['Season'].max()
            latest_data = player_data[player_data['Season'] == latest_season]
            
            # Convert to dictionary
            stats = latest_data.iloc[0].to_dict()
            
            return stats
        except Exception as e:
            logger.error(f"Error getting player stats: {str(e)}")
            return None

    def get_comparison_stats(self, player1: str, player2: str) -> Optional[Dict]:
        """Get statistics for both players for comparison"""
        try:
            player1_stats = self.get_player_stats(player1)
            player2_stats = self.get_player_stats(player2)
            
            if not player1_stats or not player2_stats:
                return None
                
            return {
                'player1': player1_stats,
                'player2': player2_stats
            }
        except Exception as e:
            logger.error(f"Error getting comparison stats: {str(e)}")
            return None 