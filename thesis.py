import sqlite3
from pathlib import Path
import pandas as pd
import logging
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse


class RecursiveDownloader:
    """
    A class to recursively find and download files with specific extensions
    from a list of starting URLs, mirroring the web directory structure locally.
    """
    def __init__(self, extensions=['.csv', '.parquet'], local_root='data'):
        """
        Initializes the downloader.

        Args:
            extensions (list): A list of file extensions to download (e.g., ['.csv', '.parquet']).
            local_root (str): The root directory for all local downloads.
        """
        self.target_extensions = tuple(ext.lower() for ext in extensions)
        self.local_root = local_root
        self.session = requests.Session()
        self.visited_urls = set()

    def _infer_local_path(self, url):
        """Infers the local file path from its URL."""
        path = urlparse(url).path
        # Create a relative path and join it with the local root
        relative_path = path.lstrip('/')
        return os.path.join(self.local_root, relative_path)

    def _download_file(self, file_url):
        """Downloads a single file to its inferred local path."""
        local_path = self._infer_local_path(file_url)
        local_dir = os.path.dirname(local_path)

        # Ensure the directory for the file exists
        os.makedirs(local_dir, exist_ok=True)

        print(f"⬇️  Downloading {os.path.basename(local_path)} to {local_dir}/")
        try:
            with self.session.get(file_url, stream=True) as r:
                r.raise_for_status()
                with open(local_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        except requests.exceptions.RequestException as e:
            print(f"  -> ❌ Failed to download {file_url}. Error: {e}")

    def _process_url(self, url):
        """Recursively process a URL, downloading files or exploring subdirectories."""
        if url in self.visited_urls:
            return
        
        print(f"🔎 Searching in: {url}")
        self.visited_urls.add(url)

        try:
            response = self.session.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"  -> ❌ Could not access {url}. Error: {e}")
            return

        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a', href=True)

        for link in links:
            href = link['href']
            # Ignore parent directory links and query parameters
            if href.startswith('?') or href.startswith('../'):
                continue

            absolute_url = urljoin(url, href)
            
            # Case 1: The link is a file we want to download
            if absolute_url.lower().endswith(self.target_extensions):
                self._download_file(absolute_url)
            
            # Case 2: The link is a subdirectory to explore further
            elif href.endswith('/'):
                self._process_url(absolute_url)

    def download_all(self, start_urls):
        """
        Starts the download process from a list of initial URLs.

        Args:
            start_urls (list): A list of URLs to begin searching from.
        """
        print(f"Starting download process for extensions: {self.target_extensions}")
        for url in start_urls:
            self._process_url(url)
        print("\n✅ All tasks complete!")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataETL:
    """Handles recursive loading, cleaning, and setting up data in a SQLite database."""
    def __init__(self, directory: str):
        self.base_directory = Path(directory)
        # Use a dictionary to group dataframes by their inferred table name
        self.grouped_data = {}
        self.errors = []

    def load_files_recursively(self):
        """Recursively scan for CSV and Parquet files and group them by parent directory."""
        logger.info(f"Recursively scanning for data files in '{self.base_directory}'...")
        
        # Use glob with '**/*' to search recursively
        for file in self.base_directory.glob('**/*'):
            if not file.is_file():
                continue

            try:
                # Infer table name from the immediate parent directory's name
                table_name = file.parent.name
                df = None

                if file.suffix.lower() == ".csv":
                    df = pd.read_csv(file, low_memory=False)
                elif file.suffix.lower() in [".parquet", ".pq"]:
                    df = pd.read_parquet(file)

                if df is not None:
                    if table_name not in self.grouped_data:
                        self.grouped_data[table_name] = []
                    self.grouped_data[table_name].append(df)
                    logger.info(f"Loaded '{file.relative_to(self.base_directory)}' for table '{table_name}'")

            except Exception as e:
                self.errors.append((file.name, e))
                logger.error(f"Failed to load {file.name}: {e}")

    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize a single combined dataframe for a table."""
        if 'DATETIME' in df.columns:
            df.rename(columns={'DATETIME': 'datetime'}, inplace=True)

        if 'datetime' in df.columns:
            # Convert timestamps to UTC to handle mixed timezone information
            df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce', utc=True)
            df.dropna(subset=['datetime'], inplace=True)
            df.sort_values('datetime', inplace=True)
        
        return df

    def _write_to_db(self, df: pd.DataFrame, db_path: str, table_name: str):
        """Write a processed DataFrame to a specific table in the SQLite database."""
        if df is None or df.empty:
            logger.warning(f"No data for table '{table_name}', skipping database write.")
            return
        
        logger.info(f"Writing data to table '{table_name}' in {db_path}...")
        try:
            with sqlite3.connect(db_path) as conn:
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                logger.info(f"✅ Successfully wrote {len(df)} records to table '{table_name}'.")
        except Exception as e:
            logger.error(f"❌ Database write failed for table '{table_name}': {e}")

    def run(self, db_path: str):
        """Execute the full ETL pipeline for all found tables."""
        total_records = 0
        self.load_files_recursively()

        if not self.grouped_data:
            logger.warning("No data files were found. ETL process is stopping.")
            return 0

        for table_name, dfs_list in self.grouped_data.items():
            logger.info(f"--- Processing data for table: {table_name} ---")
            
            combined_df = pd.concat(dfs_list, ignore_index=True)
            
            processed_df = self._preprocess_dataframe(combined_df)
            
            self._write_to_db(processed_df, db_path, table_name)
            total_records += len(processed_df)
        
        logger.info(f"\nETL pipeline complete. Total records processed: {total_records}")
        return total_records
    

import sqlite3
import pandas as pd
from pathlib import Path

class SQLiteInspector:
    """A utility class to inspect a SQLite database."""

    def __init__(self, db_path: str):
        """Initializes the inspector with the path to the database."""
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database file not found at: {self.db_path}")
        self.conn = None

    def __enter__(self):
        """Enter the context manager, creating a database connection."""
        self.conn = sqlite3.connect(self.db_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager, closing the database connection."""
        if self.conn:
            self.conn.close()

    def _validate_table_name(self, table_name: str):
        """Private helper to check if a table exists, raising an error if not."""
        tables = self.get_tables()
        if table_name not in tables:
            raise ValueError(f"Table '{table_name}' not found. Please choose from: {tables}")

    def get_tables(self) -> list[str]:
        """Returns a list of all table names in the database."""
        query = "SELECT name FROM sqlite_master WHERE type='table';"
        cursor = self.conn.cursor()
        cursor.execute(query)
        return [table[0] for table in cursor.fetchall()]

    def get_column_names(self, table_name: str) -> list[str]:
        """Gets the column names for a specific table."""
        self._validate_table_name(table_name) # Error handling
        query = f"PRAGMA table_info('{table_name}');"
        cursor = self.conn.cursor()
        cursor.execute(query)
        return [col[1] for col in cursor.fetchall()]

    def get_top_unique_values(self, table_name: str, column_name: str, n: int = 10) -> pd.DataFrame:
        """Gets the top N most frequent unique values for a given column."""
        self._validate_table_name(table_name) # Error handling
        query = f"""
            SELECT "{column_name}", COUNT(*) as count
            FROM "{table_name}"
            GROUP BY "{column_name}"
            ORDER BY count DESC
            LIMIT {n};
        """
        return pd.read_sql_query(query, self.conn)

    def get_table_as_df(self, table_name: str) -> pd.DataFrame:
        """
        Retrieves an entire table as a pandas DataFrame.

        Args:
            table_name (str): The name of the table to retrieve.

        Returns:
            A pandas DataFrame containing all data from the table.
        """
        self._validate_table_name(table_name) # Error handling
        print(f"Reading full table '{table_name}' into DataFrame...")
        query = f'SELECT * FROM "{table_name}";'
        return pd.read_sql_query(query, self.conn)

