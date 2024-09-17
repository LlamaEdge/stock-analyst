import mysql.connector
from mysql.connector import Error
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
from sec_downloader import Downloader
from sec_edgar_downloader._orchestrator import (
    aggregate_filings_to_download,
    get_ticker_to_cik_mapping
)
from sec_edgar_downloader._types import DownloadMetadata
from sec_edgar_downloader._sec_gateway import download_filing


# SQL query to create the database and table
def create_database_and_table():
    try:
        connection = mysql.connector.connect(
            host='',
            user='',
            password=''
        )
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS sec;")
        cursor.execute("USE sec;")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sec_filings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company_identifier VARCHAR(255),
            form VARCHAR(50),
            accession_number VARCHAR(50),
            filing_date DATE,
            report_date DATE,
            file_url VARCHAR(255),
            content LONGBLOB
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        connection.close()
        print("Database and table created successfully.")
    except Error as e:
        print(f"Error creating database and table: {e}")


# Database connection details
def create_database_connection():
    try:
        connection = mysql.connector.connect(
            host='',
            database='sec',
            user='',
            password=''
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    return None


# Function to save SEC filing contents to the database
def save_filing_to_db(filing_contents: bytes, save_info: Dict[str, Any]) -> None:
    connection = create_database_connection()
    if connection is None:
        print("Failed to connect to the database.")
        return

    try:
        cursor = connection.cursor()
        query = """INSERT INTO sec_filings 
                   (company_identifier, form, accession_number, filing_date, report_date, file_url, content) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(query, (
            save_info['company_identifier'],
            save_info['form'],
            save_info['accession_number'],
            save_info['filing_date'],
            save_info['report_date'],
            save_info['file_url'],
            filing_contents
        ))
        connection.commit()
        print(f"Filing saved to database: {save_info['accession_number']}")
    except Error as e:
        print(f"Error saving filing to database: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


# Custom function to download and save SEC filings to the database
def custom_fetch_and_save_filings(download_metadata: DownloadMetadata, user_agent: str) -> int:
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
                save_filing_to_db(raw_filing, save_info)
        except Exception as e:
            print(f"Error occurred while downloading filing {td.accession_number}: {e}")
            continue
        successfully_downloaded += 1
    return successfully_downloaded


# Function to download and save filings with a date range
def download_and_save_filings(ticker: str, form: str, before_date: str, after_date: str, limit: int = 1):
    user_agent = "Your Company Name/1.0 (your.email@example.com)"
    ticker_to_cik_mapping = get_ticker_to_cik_mapping(user_agent)    
    if ticker not in ticker_to_cik_mapping:
        print(f"CIK for ticker {ticker} not found.")
        return  
    cik = ticker_to_cik_mapping[ticker]    
    metadata = DownloadMetadata(
        download_folder=Path("/tmp"),  # Temp folder for any intermediate operations
        form=form,
        cik=cik,  
        ticker=ticker,
        limit=limit,
        before=datetime.strptime(before_date, "%Y-%m-%d").date(),  
        after=datetime.strptime(after_date, "%Y-%m-%d").date()     
    )
    custom_fetch_and_save_filings(metadata, user_agent)


# Main execution
if __name__ == "__main__":
    create_database_and_table()

    # Example: Download filings for AAPL within date range 2023-01-01 to 2024-01-01
    download_and_save_filings(ticker="AAPL", form="10-K", before_date="2024-01-01", after_date="2023-01-01", limit=1)
    print("Finished downloading and saving filings.")

