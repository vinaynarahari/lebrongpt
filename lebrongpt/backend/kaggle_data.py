import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from typing import Dict, List, Optional
import logging
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class KaggleDataFetcher:
    def __init__(self):
        self.api = KaggleApi()
        self._authenticate()
        self.dataset = "eoinamoore/historical-nba-data-and-player-box-scores"
        self.stats_file = "PlayerStatistics.csv"
        self.players_file = "Players.csv"
        self._cache = None
        self._last_fetch = None
        self._cache_duration = timedelta(hours=1)  # Cache for 1 hour

    def _authenticate(self):
        """Authenticate with Kaggle API using environment variables"""
        try:
            username = os.getenv('KAGGLE_USERNAME')
            key = os.getenv('KAGGLE_KEY')
            
            if not username or not key:
                raise ValueError("Kaggle credentials not found in environment variables")
            
            # Create kaggle.json file in the home directory
            kaggle_dir = os.path.expanduser('~/.kaggle')
            os.makedirs(kaggle_dir, exist_ok=True)
            
            kaggle_json = {
                'username': username,
                'key': key
            }
            
            with open(os.path.join(kaggle_dir, 'kaggle.json'), 'w') as f:
                json.dump(kaggle_json, f)
            
            # Set proper permissions
            os.chmod(os.path.join(kaggle_dir, 'kaggle.json'), 0o600)
                
            self.api.authenticate()
            logger.info("Successfully authenticated with Kaggle API")
        except Exception as e:
            logger.error(f"Failed to authenticate with Kaggle API: {str(e)}")
            raise

    def _fetch_data(self) -> pd.DataFrame:
        """Fetch and process the latest data from Kaggle"""
        try:
            # Download both datasets
            self.api.dataset_download_file(
                self.dataset,
                self.stats_file,
                path="/tmp",
                force=True
            )
            
            self.api.dataset_download_file(
                self.dataset,
                self.players_file,
                path="/tmp",
                force=True
            )
            
            # Read the CSV files
            stats_df = pd.read_csv(f"/tmp/{self.stats_file}")
            players_df = pd.read_csv(f"/tmp/{self.players_file}")
            
            # Clean up the downloaded files
            os.remove(f"/tmp/{self.stats_file}")
            os.remove(f"/tmp/{self.players_file}")
            
            # Process the data
            return self._process_data(stats_df, players_df)
            
        except Exception as e:
            logger.error(f"Error fetching data from Kaggle: {str(e)}")
            raise

    def _process_data(self, stats_df: pd.DataFrame, players_df: pd.DataFrame) -> pd.DataFrame:
        """Process and aggregate the player statistics"""
        try:
            # Convert date columns to datetime
            stats_df['Date'] = pd.to_datetime(stats_df['Date'])
            
            # Group by player and calculate career totals
            career_stats = stats_df.groupby('Player').agg({
                'PTS': 'sum',
                'AST': 'sum',
                'REB': 'sum',
                'STL': 'sum',
                'BLK': 'sum',
                'TOV': 'sum',
                'FGM': 'sum',
                'FGA': 'sum',
                '3PM': 'sum',
                '3PA': 'sum',
                'FTM': 'sum',
                'FTA': 'sum',
                'Games': 'count'
            }).reset_index()
            
            # Calculate averages
            career_stats['PPG'] = career_stats['PTS'] / career_stats['Games']
            career_stats['APG'] = career_stats['AST'] / career_stats['Games']
            career_stats['RPG'] = career_stats['REB'] / career_stats['Games']
            career_stats['SPG'] = career_stats['STL'] / career_stats['Games']
            career_stats['BPG'] = career_stats['BLK'] / career_stats['Games']
            career_stats['TOPG'] = career_stats['TOV'] / career_stats['Games']
            career_stats['FG%'] = (career_stats['FGM'] / career_stats['FGA'] * 100).round(1)
            career_stats['3P%'] = (career_stats['3PM'] / career_stats['3PA'] * 100).round(1)
            career_stats['FT%'] = (career_stats['FTM'] / career_stats['FTA'] * 100).round(1)
            
            # Get latest season stats
            latest_season = stats_df['Season'].max()
            latest_stats = stats_df[stats_df['Season'] == latest_season].groupby('Player').agg({
                'PTS': 'mean',
                'AST': 'mean',
                'REB': 'mean',
                'STL': 'mean',
                'BLK': 'mean',
                'TOV': 'mean',
                'FGM': 'mean',
                'FGA': 'mean',
                '3PM': 'mean',
                '3PA': 'mean',
                'FTM': 'mean',
                'FTA': 'mean',
                'Games': 'count'
            }).reset_index()
            
            # Add season prefix to latest stats columns
            latest_stats.columns = [f'Current_{col}' if col != 'Player' else col for col in latest_stats.columns]
            
            # Merge career and latest stats
            final_stats = pd.merge(career_stats, latest_stats, on='Player', how='left')
            
            # Merge with player information
            final_stats = pd.merge(final_stats, players_df[['Player', 'Position', 'Height', 'Weight']], 
                                 on='Player', how='left')
            
            return final_stats
            
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            raise

    def get_player_stats(self, player_name: str) -> Optional[Dict]:
        """Get statistics for a specific player"""
        try:
            # Fetch fresh data if cache is empty or older than cache_duration
            if self._cache is None or self._last_fetch is None or \
               datetime.now() - self._last_fetch > self._cache_duration:
                self._cache = self._fetch_data()
                self._last_fetch = datetime.now()
            
            # Search for player (case insensitive)
            player_data = self._cache[
                self._cache['Player'].str.lower() == player_name.lower()
            ]
            
            if player_data.empty:
                return None
            
            # Convert to dictionary and format numbers
            stats = player_data.iloc[0].to_dict()
            
            # Format numeric values
            for key, value in stats.items():
                if isinstance(value, float):
                    stats[key] = round(value, 1)
            
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