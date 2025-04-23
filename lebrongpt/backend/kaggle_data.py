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
        self._player_names = None

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
            
            # Read the CSV files with low_memory=False to avoid mixed type warnings
            stats_df = pd.read_csv(f"/tmp/{self.stats_file}", low_memory=False)
            players_df = pd.read_csv(f"/tmp/{self.players_file}", low_memory=False)
            
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
            # Create full player name
            stats_df['Player'] = stats_df['firstName'] + ' ' + stats_df['lastName']
            
            # Store unique player names
            self._player_names = sorted(stats_df['Player'].unique().tolist())
            
            # Convert date column to datetime
            stats_df['gameDate'] = pd.to_datetime(stats_df['gameDate'])
            
            # Filter out preseason games
            stats_df = stats_df[~stats_df['gameType'].str.contains('Preseason', case=False, na=False)]
            
            # Create separate dataframes for regular season and playoffs
            regular_season_df = stats_df[
                stats_df['gameType'].str.contains('Regular Season|NBA Emirates Cup', case=False, na=False)
            ]
            playoffs_df = stats_df[
                stats_df['gameType'].str.contains('Playoffs|Play-in Tournament', case=False, na=False)
            ]
            
            # Function to calculate stats for a given dataframe
            def calculate_stats(df, prefix=''):
                if df.empty:
                    return pd.DataFrame()
                
                stats = df.groupby('Player').agg({
                    'points': 'sum',
                    'assists': 'sum',
                    'reboundsTotal': 'sum',
                    'steals': 'sum',
                    'blocks': 'sum',
                    'turnovers': 'sum',
                    'fieldGoalsMade': 'sum',
                    'fieldGoalsAttempted': 'sum',
                    'threePointersMade': 'sum',
                    'threePointersAttempted': 'sum',
                    'freeThrowsMade': 'sum',
                    'freeThrowsAttempted': 'sum',
                    'gameDate': 'count'  # Use gameDate count as Games played
                }).reset_index()
                
                # Rename gameDate count to Games
                stats.rename(columns={'gameDate': 'Games'}, inplace=True)
                
                # Calculate averages
                stats['PPG'] = stats['points'] / stats['Games']
                stats['APG'] = stats['assists'] / stats['Games']
                stats['RPG'] = stats['reboundsTotal'] / stats['Games']
                stats['SPG'] = stats['steals'] / stats['Games']
                stats['BPG'] = stats['blocks'] / stats['Games']
                stats['TOPG'] = stats['turnovers'] / stats['Games']
                stats['FG%'] = (stats['fieldGoalsMade'] / stats['fieldGoalsAttempted'] * 100).round(1)
                stats['3P%'] = (stats['threePointersMade'] / stats['threePointersAttempted'] * 100).round(1)
                stats['FT%'] = (stats['freeThrowsMade'] / stats['freeThrowsAttempted'] * 100).round(1)
                
                # Add prefix to all columns except Player
                if prefix:
                    stats.columns = [f'{prefix}_{col}' if col != 'Player' else col for col in stats.columns]
                
                return stats
            
            # Calculate stats for each category
            regular_season_stats = calculate_stats(regular_season_df, 'Regular')
            playoffs_stats = calculate_stats(playoffs_df, 'Playoffs')
            combined_stats = calculate_stats(stats_df, 'Career')
            
            # Merge all stats
            final_stats = pd.merge(regular_season_stats, playoffs_stats, on='Player', how='outer')
            final_stats = pd.merge(final_stats, combined_stats, on='Player', how='outer')
            
            # Fill NaN values with 0
            final_stats = final_stats.fillna(0)
            
            # Get latest season stats (using gameDate to determine season)
            latest_date = stats_df['gameDate'].max()
            latest_season_start = latest_date.replace(month=10, day=1)  # NBA season typically starts in October
            if latest_date.month < 10:
                latest_season_start = latest_season_start.replace(year=latest_season_start.year - 1)
            
            latest_stats = stats_df[stats_df['gameDate'] >= latest_season_start].groupby('Player').agg({
                'points': 'mean',
                'assists': 'mean',
                'reboundsTotal': 'mean',
                'steals': 'mean',
                'blocks': 'mean',
                'turnovers': 'mean',
                'fieldGoalsMade': 'mean',
                'fieldGoalsAttempted': 'mean',
                'threePointersMade': 'mean',
                'threePointersAttempted': 'mean',
                'freeThrowsMade': 'mean',
                'freeThrowsAttempted': 'mean',
                'gameDate': 'count'
            }).reset_index()
            
            # Rename gameDate count to Games in latest stats
            latest_stats.rename(columns={'gameDate': 'Games'}, inplace=True)
            
            # Add season prefix to latest stats columns
            latest_stats.columns = [f'Current_{col}' if col != 'Player' else col for col in latest_stats.columns]
            
            # Merge with latest stats
            final_stats = pd.merge(final_stats, latest_stats, on='Player', how='left')
            
            # Add player information if available
            if 'firstName' in players_df.columns and 'lastName' in players_df.columns:
                players_df['Player'] = players_df['firstName'] + ' ' + players_df['lastName']
                
                # Determine position based on guard, forward, center columns
                players_df['Position'] = players_df.apply(
                    lambda row: 'G' if row['guard'] == 1 else 'F' if row['forward'] == 1 else 'C' if row['center'] == 1 else 'Unknown',
                    axis=1
                )
                
                final_stats = pd.merge(final_stats, 
                                     players_df[['Player', 'Position', 'height', 'bodyWeight']], 
                                     on='Player', how='left')
                
                # Rename columns to match expected format
                final_stats.rename(columns={
                    'height': 'Height',
                    'bodyWeight': 'Weight'
                }, inplace=True)
            
            return final_stats
            
        except Exception as e:
            logger.error(f"Error processing data: {str(e)}")
            raise

    def get_all_players(self) -> List[str]:
        """Get a list of all player names"""
        try:
            # Fetch fresh data if cache is empty or older than cache_duration
            if self._cache is None or self._last_fetch is None or \
               datetime.now() - self._last_fetch > self._cache_duration:
                self._cache = self._fetch_data()
                self._last_fetch = datetime.now()
            
            return self._player_names
            
        except Exception as e:
            logger.error(f"Error getting player names: {str(e)}")
            return []

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
            
            # Format numeric values and handle invalid floats
            for key, value in stats.items():
                if isinstance(value, float):
                    if pd.isna(value) or value == float('inf') or value == float('-inf'):
                        stats[key] = 0
                    else:
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