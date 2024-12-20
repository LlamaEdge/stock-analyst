import mysql.connector
from mysql.connector import Error
from typing import Dict, Any, Optional
from sec_downloader import Downloader
from sec_edgar_downloader._orchestrator import (
    aggregate_filings_to_download,
    get_ticker_to_cik_mapping
)
from sec_edgar_downloader._types import DownloadMetadata
from sec_edgar_downloader._sec_gateway import download_filing
from dotenv import load_dotenv
import os

load_dotenv('./.env', override=False)

DB_HOST = os.environ.get('DB_HOST') or os.getenv('DB_HOST')
DB_PORT = os.environ.get('DB_PORT') or os.getenv('DB_PORT', '3306') 
DB_USER = os.environ.get('DB_USER') or os.getenv('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD') or os.getenv('DB_PASSWORD')
DB_NAME = os.environ.get('DB_NAME') or os.getenv('DB_NAME')

def create_database_if_not_exists():
    try:
        connection = mysql.connector.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD)
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    except Error as e:
        print(f"Error creating database: {e}")
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if connection.is_connected():
            connection.close()

def create_database_connection():
    try:
        connection = mysql.connector.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        if connection.is_connected():
            
         return connection
    except Error as e:
        print(f"Error connecting to MySQL database '{DB_NAME}': {e}")
        return None

def execute_query(query: str, data: tuple = (), connection=None) -> None:
    try:
        if connection is None or not connection.is_connected():
            raise ValueError("Database connection is not established or closed.")
        with connection.cursor() as cursor:
            cursor.execute(query, data)
            connection.commit()
    except Error as e:
        print(f"Error executing query: {e}")

def initialize_database():
    create_database_if_not_exists()
    connection = create_database_connection()
    if connection:
        connection.close()
        print("Database initialization complete.")
    else:
        print("Failed to initialize database.")

def create_column(table_name: str, column_name: str, column_type: str, connection) -> None:
    execute_query(f"ALTER TABLE {table_name} ADD {column_name} {column_type}", connection=connection)

def delete_column(table_name: str, column_name: str, connection) -> None:
    execute_query(f"ALTER TABLE {table_name} DROP COLUMN {column_name}", connection=connection)

def insert_record(table_name: str, data: Dict[str, Any], connection) -> None:
    if not data:
        raise ValueError("No data provided for insertion.")
    columns = ', '.join(data.keys())
    placeholders = ', '.join(['%s'] * len(data))
    execute_query(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", tuple(data.values()), connection)

def save_filing_to_db(filing_contents: bytes, save_info: Dict[str, Any], connection) -> None:
    save_info['content'] = filing_contents  
    insert_record('sec_filings', save_info, connection)

def fetch_ticker_to_cik_mapping(ticker: str) -> Optional[str]:
    try:
        cik_mapping = get_ticker_to_cik_mapping("Your Company Name/1.0 (your.email@example.com)")
        return cik_mapping.get(ticker.upper()) if cik_mapping else None
    except Exception as e:
        print(f"Error fetching CIK for ticker {ticker}: {e}")
        return None

def get_filing_text_by_accession_number(accession_number: str, connection) -> Optional[str]:
    query = "SELECT content FROM sec_filings WHERE accession_number = %s"
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, (accession_number,))
                result = cursor.fetchone()
                return decode_blob(result[0]) if result else None
        except Error as e:
            print(f"Error retrieving filing content: {e}")
    return None

def save_filing_text_as_blob(accession_number: str, text_content: str, connection) -> None:
    execute_query("UPDATE sec_filings SET content = %s WHERE accession_number = %s", (encode_blob(text_content), accession_number), connection)

def encode_blob(text_content: str) -> bytes:
    return text_content.encode('utf-8')

def decode_blob(blob_data: bytes) -> str:
    return blob_data.decode('utf-8')

def save_blob_to_file(accession_number: str, output_path: str, connection) -> None:
    content = get_filing_text_by_accession_number(accession_number, connection)
    if content:
        with open(output_path, 'w', encoding='utf-8') as file:
            file.write(content)

def store_blob_from_text(accession_number: str, text_content: str, connection) -> None:
    save_filing_text_as_blob(accession_number, text_content, connection)

def custom_fetch_and_save_filings(download_metadata: DownloadMetadata, user_agent: str, connection) -> int:
    successfully_downloaded = 0
    to_download = aggregate_filings_to_download(download_metadata, user_agent)
    
    for td in to_download:
        try:
            raw_filing = download_filing(td.raw_filing_uri, user_agent)
            print(f"Downloaded filing: {td.accession_number}")

            
            dl = Downloader("MyCompanyName", "email@example.com")
            metadatas = dl.get_filing_metadatas(f"{download_metadata.ticker}/{td.accession_number}")

            if metadatas:
                filing_metadata = metadatas[0]
                save_info = {
                    'company_identifier': download_metadata.ticker or download_metadata.cik,
                    'form': filing_metadata.form_type,
                    'accession_number': filing_metadata.accession_number,
                    'filing_date': filing_metadata.filing_date,
                    'report_date': filing_metadata.report_date,
                    'file_url': filing_metadata.primary_doc_url
                }
                save_filing_to_db(raw_filing, save_info, connection)

                successfully_downloaded += 1
        except Exception as e:
            print(f"Error occurred while downloading filing {td.accession_number}: {e}")

    return successfully_downloaded

def create_table(table_name: str, columns: Dict[str, str], connection) -> None:
    columns_with_types = ', '.join([f"{name} {col_type}" for name, col_type in columns.items()])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types})"
    execute_query(create_table_query, connection=connection)


def check_column_exists(connection, column_name: str, table_name: str) -> bool:
    try:
        cursor = connection.cursor()
        query = f"SHOW COLUMNS FROM {table_name} LIKE %s"
        cursor.execute(query, (column_name,))
        result = cursor.fetchone()
        return result is not None
    except Exception as e:
        print(f"Error checking column existence: {e}")
        return False
    finally:
        cursor.close()

def update_parsed_text(connection, accession_number: str, parsed_blob: bytes) -> None:
    try:
        cursor = connection.cursor()
        query = "UPDATE sec_filings SET parsed_text = %s WHERE accession_number = %s"
        cursor.execute(query, (parsed_blob, accession_number))
        connection.commit()
        print(f"Updated 'parsed_text' for accession number: {accession_number}")
    except Error as e:
        print(f"Error updating 'parsed_text' for accession number {accession_number}: {e}")
    finally:
        cursor.close()
