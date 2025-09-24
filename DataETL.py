import sqlite3
from pathlib import Path
import pandas as pd
import logging

# Set up logging for better visibility
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataETL:
    """Handles loading, cleaning, and setting up the data in a SQLite database."""
    def __init__(self, directory: str):
        self.directory = Path(directory)
        self.dataframes = []
        self.errors = []
        self.combined_df = None

    def load_files(self):
        """Load CSV and Parquet files into DataFrames."""
        logger.info(f"Scanning for data files in '{self.directory}'...")
        for file in self.directory.glob("*"):
            try:
                if file.suffix.lower() == ".csv":
                    df = pd.read_csv(file, low_memory=False)
                    self.dataframes.append((file.name, df))
                    logger.info(f"Loaded {file.name}")
                elif file.suffix.lower() in [".parquet", ".pq"]:
                    df = pd.read_parquet(file)
                    self.dataframes.append((file.name, df))
                    logger.info(f"Loaded {file.name}")
            except Exception as e:
                self.errors.append((file.name, e))
                logger.error(f"Failed to load {file.name}: {e}")

    def preprocess_data(self):
        """Combine, clean, and standardize data from all loaded files."""
        if not self.dataframes:
            logger.warning("No dataframes loaded, skipping preprocessing.")
            return

        logger.info("Preprocessing data...")
        all_dfs = [df for _, df in self.dataframes]
        self.combined_df = pd.concat(all_dfs, ignore_index=True)

        # Standardize timestamp column
        if 'DATETIME' in self.combined_df.columns:
            self.combined_df.rename(columns={'DATETIME': 'datetime'}, inplace=True)
        
        # **FIX**: Convert all timestamps to UTC to handle mixed timezone information.
        # This prevents the 'Cannot compare tz-naive and tz-aware timestamps' error.
        self.combined_df['datetime'] = pd.to_datetime(self.combined_df['datetime'], errors='coerce', utc=True)
        self.combined_df.dropna(subset=['datetime'], inplace=True)
        self.combined_df.sort_values('datetime', inplace=True)
        
        logger.info(f"Preprocessing complete. Total events: {len(self.combined_df)}")

    def create_database(self, db_path: str, table_name: str):
        """Create a SQLite database and populate it with the processed data."""
        if self.combined_df is None or self.combined_df.empty:
            logger.error("No data to write to the database.")
            return

        logger.info(f"Writing data to SQLite database: {db_path}")
        try:
            with sqlite3.connect(db_path) as conn:
                self.combined_df.to_sql(table_name, conn, if_exists='replace', index=False)
                logger.info(f"Successfully created table '{table_name}' with {len(self.combined_df)} records.")
        except Exception as e:
            logger.error(f"Database creation failed: {e}")

    def run(self, db_path: str, table_name: str):
        """Execute the full ETL pipeline."""
        self.load_files()
        self.preprocess_data()
        self.create_database(db_path, table_name)
        return len(self.combined_df) if self.combined_df is not None else 0