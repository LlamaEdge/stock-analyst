import requests
import mysql.connector
import os
import json
from datetime import datetime
from pathlib import Path
from sec_downloader import Downloader
from sec_edgar_downloader._orchestrator import (
    aggregate_filings_to_download,
    get_ticker_to_cik_mapping
)
from sec_edgar_downloader._types import DownloadMetadata
from sec_edgar_downloader._sec_gateway import download_filing
from dotenv import load_dotenv
import inspect

# Load environment variables from .env file if required
load_dotenv()

# Setup API keys and model name
GAIANET_API_KEY = os.getenv("GAIANET_API_KEY", "GAIA")
GAIANET_API_URL = os.getenv("GAIANET_API_URL", "https://llamatool.us.gaianet.network/v1")
MODEL_NAME = os.getenv("MODEL_NAME", "llama")

# MySQL database connection details
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "sec")


def create_database_connection():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            return connection
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
    return None


def create_database_and_table():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME};")
        cursor.execute(f"USE {DB_NAME};")
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
    except mysql.connector.Error as e:
        print(f"Error creating database and table: {e}")


def save_filing_to_db(filing_contents: bytes, save_info: dict) -> None:
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
    except mysql.connector.Error as e:
        print(f"Error saving filing to database: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def custom_fetch_and_save_filings(download_metadata: DownloadMetadata, user_agent: str) -> int:
    successfully_downloaded = 0
    cik_mapping = get_ticker_to_cik_mapping(user_agent)  
    cik = cik_mapping.get(download_metadata.ticker)
    if cik is None:
        print(f"Error: No CIK found for ticker {download_metadata.ticker}.")
        return successfully_downloaded
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
                    'company_identifier': download_metadata.ticker or cik,
                    'form': filing_metadata.form_type,
                    'accession_number': filing_metadata.accession_number,
                    'filing_date': filing_metadata.filing_date,
                    'report_date': filing_metadata.report_date,
                    'file_url': filing_metadata.primary_doc_url
                }
                save_filing_to_db(raw_filing, save_info)
                successfully_downloaded += 1
        except Exception as e:
            print(f"Error occurred while downloading filing {td.accession_number}: {e}")
            continue
    return successfully_downloaded


def create_task(ticker: str, form: str, before_date: str, after_date: str, limit: int = 1):
    try:
        # Print the signature of the DownloadMetadata class
        # print(f"DownloadMetadata signature: {inspect.signature(DownloadMetadata)}")
        date_after = datetime.strptime(after_date, "%Y-%m-%d").date()
        date_before = datetime.strptime(before_date, "%Y-%m-%d").date()
        user_agent = "Your-App-Name/1.0 (your-email@example.com)"      
        ticker_to_cik = get_ticker_to_cik_mapping(user_agent)
        cik = ticker_to_cik.get(ticker.upper())
        if cik is None:
            return {"result": "error", "message": f"CIK not found for ticker {ticker}"}
        download_metadata = DownloadMetadata(
            download_folder="None",
            ticker=ticker,
            cik=cik,
            form=form,
            after=date_after,
            before=date_before,
            limit=limit
        )
        custom_fetch_and_save_filings(download_metadata, user_agent)
        return {"result": "ok", "message": f"SEC filings for {ticker} saved."}
    except Exception as e:
        return {"result": "error", "message": str(e)}


def get_tasks():
    connection = create_database_connection()
    if connection is None:
        print("Failed to connect to the database.")
        return {"result": "error", "message": "Database connection failed"}

    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM sec_filings")
        tasks = cursor.fetchall()
        cursor.close()
        connection.close()
        return {"result": "ok", "tasks": tasks}
    except mysql.connector.Error as e:
        return {"result": "error", "message": str(e)}

# Tool definitions
Tools = [
    {
        "type": "function",
        "function": {
            "name": "create_task",
            "description": "Download and save SEC filings",
            "parameters": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "Stock ticker, e.g., AAPL"},
                    "form": {"type": "string", "description": "Form type, e.g., 10-K"},
                    "before_date": {"type": "string", "description": "End date in YYYY-MM-DD"},
                    "after_date": {"type": "string", "description": "Start date in YYYY-MM-DD"},
                    "limit": {"type": "number", "description": "Number of filings to download"}
                },
                "required": ["ticker", "form", "before_date", "after_date"]
            },
        },
    },
]


def eval_tools(tools):
    result = []
    for tool in tools:
        fun = tool["function"]
        if fun["name"] == "create_task":
            arguments = json.loads(fun.get("arguments", "{}"))
            result.append(create_task(
                ticker=arguments.get("ticker", ""),
                form=arguments.get("form", ""),
                before_date=arguments.get("before_date", ""),
                after_date=arguments.get("after_date", ""),
                limit=arguments.get("limit", 1)
            ))
        else:
            result.append({"result": "error", "message": f"Unknown function {fun['name']}"})

    if len(result) > 0:
        print("Tool Results:")
        print(result)

    return result


def chat_completions(messages):
    try:
        payload = {
            "model": MODEL_NAME,
            "messages": messages,
            "tools": Tools,
            "tool_choice": "auto",
            "stream": True
        }

        response = requests.post(
            f"{GAIANET_API_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {GAIANET_API_KEY}",
                "Content-Type": "application/json"
            },
            json=payload,
            stream=True
        )
        response.raise_for_status()
        process_streaming_response(response, messages)
    except requests.exceptions.RequestException as e:
        print(f"Error with GaiaNet API: {e}")

def process_streaming_response(response, messages):
    tools = []
    content = ""
    print("Response:")
    for line in response.iter_lines():
        if line:
            line_str = line.decode('utf-8')
            if line_str == "data: [DONE]":
                break
            try:
                chunk = json.loads(line_str.split('data: ')[1])
                if 'choices' in chunk and len(chunk['choices']) > 0:
                    delta = chunk['choices'][0].get('delta', {})
                    if 'content' in delta:
                        print(delta['content'], end="")
                        content += delta['content']
                    if 'tool_calls' in delta:
                        tools.extend(delta['tool_calls'])
            except json.JSONDecodeError:
                print(f"Failed to decode JSON: {line_str}")
            except IndexError:
                print(f"Unexpected response format: {line_str}")

    print()  
    if tools:
        messages.append({"role": "assistant", "content": content, "tool_calls": tools})
        eval_tools(tools)
    else:
        messages.append({"role": "assistant", "content": content})

# Main 
if __name__ == "__main__":
    create_database_and_table()
    messages = [
        {"role": "system", "content": "You are an assistant that can help download SEC filings. The user will provide queries, and you should interpret them to use the appropriate tools."}
    ]
    print("Type 'exit', 'quit', or 'bye' to end the conversation.")
    while True:
        user_input = input("User: ")      
        if user_input.lower() in ['exit', 'quit', 'bye']:
            break        
        messages.append({"role": "user", "content": user_input})      
        chat_completions(messages)
        messages = [messages[0]]
