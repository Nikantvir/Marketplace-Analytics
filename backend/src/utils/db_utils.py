import psycopg2
import pandas as pd
import os
import logging
from datetime import datetime
import csv

logger = logging.getLogger(__name__)

class PostgresConnector:
    def __init__(self, host=None, port=None, dbname=None, user=None, password=None):
        """Initialize connection parameters from environment or arguments"""
        self.host = host or os.environ.get('POSTGRES_HOST', 'postgres')
        self.port = port or os.environ.get('POSTGRES_PORT', '5432')
        self.dbname = dbname or os.environ.get('POSTGRES_DB', 'airflow')
        self.user = user or os.environ.get('POSTGRES_USER', 'airflow')
        self.password = password or os.environ.get('POSTGRES_PASSWORD', 'airflow')
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish connection to the PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {str(e)}")
            return False
    
    def disconnect(self):
        """Close the connection to the PostgreSQL database"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from PostgreSQL")
    
    def execute(self, query, params=None, commit=True):
        """Execute a SQL query with optional parameters"""
        try:
            if not self.conn or self.conn.closed:
                self.connect()
            
            self.cursor.execute(query, params)
            
            if commit:
                self.conn.commit()
            
            return True
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            if commit:
                self.conn.rollback()
            return False
    
    def fetch_all(self, query, params=None):
        """Execute a query and fetch all results"""
        try:
            if not self.conn or self.conn.closed:
                self.connect()
            
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching data: {str(e)}")
            return []
    
    def insert_dataframe(self, df, table_name, schema='stg'):
        """Insert DataFrame into a PostgreSQL table"""
        if df.empty:
            logger.warning(f"DataFrame is empty, skipping insert to {schema}.{table_name}")
            return False
        
        try:
            # Create columns string and placeholders for the INSERT statement
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            
            # Prepare SQL query
            query = f"""
            INSERT INTO {schema}.{table_name} ({columns})
            VALUES ({placeholders})
            """
            
            # Convert DataFrame to list of tuples
            data = [tuple(row) for row in df.to_numpy()]
            
            # Execute multiple insert
            if not self.conn or self.conn.closed:
                self.connect()
            
            self.cursor.executemany(query, data)
            self.conn.commit()
            
            logger.info(f"Successfully inserted {len(df)} rows into {schema}.{table_name}")
            return True
        except Exception as e:
            logger.error(f"Error inserting data into {schema}.{table_name}: {str(e)}")
            if self.conn:
                self.conn.rollback()
            return False

def save_to_csv(df, filename, directory=None):
    """Save DataFrame to CSV file"""
    if df.empty:
        logger.warning(f"DataFrame is empty, skipping CSV export for {filename}")
        return False
    
    try:
        # Get directory from environment if not provided
        if not directory:
            directory = os.environ.get('DATA_CSV_PATH', '/opt/airflow/data/csv')
        
        # Ensure directory exists
        os.makedirs(directory, exist_ok=True)
        
        # Create timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(directory, f"{filename}_{timestamp}.csv")
        
        # Save DataFrame to CSV
        df.to_csv(filepath, index=False, quoting=csv.QUOTE_NONNUMERIC)
        
        logger.info(f"Successfully saved {len(df)} rows to {filepath}")
        return True
    except Exception as e:
        logger.error(f"Error saving data to CSV {filename}: {str(e)}")
        return False