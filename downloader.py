from pathlib import Path
from datetime import datetime
from sec_edgar_downloader._types import DownloadMetadata
from utils import (
    fetch_ticker_to_cik_mapping,
    custom_fetch_and_save_filings,
    create_table,
)

# Function to download and save filings with a date range
def download_and_save_filings(ticker: str, form: str, before_date: str, after_date: str, limit: int = 1) -> None:
    user_agent = "My Name /1.0 (myname@gmail.com)"
    
    # Fetch CIK using the ticker
    cik = fetch_ticker_to_cik_mapping(ticker)
    
    if not cik:
        print(f"CIK for ticker {ticker} not found.")
        return

    # Prepare download metadata
    metadata = DownloadMetadata(
        download_folder=Path("/tmp"),  # Temp folder for any intermediate operations
        form=form,
        cik=cik,
        ticker=ticker,
        limit=limit,
        before=datetime.strptime(before_date, "%Y-%m-%d").date(),
        after=datetime.strptime(after_date, "%Y-%m-%d").date()
    )
    
    # Fetch and save filings using the custom function from utils
    successfully_downloaded = custom_fetch_and_save_filings(metadata, user_agent)
    print(f"Successfully downloaded {successfully_downloaded} filing(s) for {ticker}.")

def main():
    # Define table name and structure
    table_name = "sec_filings"
    column_definitions = {
        'company_identifier': 'VARCHAR(50)',
        'form': 'VARCHAR(10)',
        'accession_number': 'VARCHAR(20)',
        'filing_date': 'DATE',
        'report_date': 'DATE',
        'file_url': 'VARCHAR(255)',
        'content': 'LONGBLOB',
    }
    
    # Create table if it does not exist
    create_table(table_name, column_definitions)

    # Example: Download filings for AAPL within date range 2023-01-01 to 2024-01-01
    download_and_save_filings(ticker="MSFT", form="10-K", before_date="2024-01-01", after_date="2023-01-01", limit=1)
    print("Finished downloading and saving filings.")

# Main execution
if __name__ == "__main__":
    main()
