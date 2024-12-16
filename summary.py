# summary.py
import os
from typing import List, Optional
import openai
from utils import (
    create_database_connection,
    check_column_exists,
    decode_blob,
    encode_blob
)
from dotenv import load_dotenv

load_dotenv('./.env', override=False)
GAIA_API_KEY = os.getenv("GAIA_API_KEY") or os.environ.get("GAIA_API_KEY")
GAIA_API_URL = os.getenv("GAIA_API_URL") or os.environ.get("GAIA_API_URL")
MODEL_NAME = os.getenv("GAIA_MODEL", "llama")  or os.environ.get("GAIA_MODEL", "llama")

client = openai.OpenAI(
    base_url=GAIA_API_URL,
    api_key=GAIA_API_KEY
)

def chunk_text(text: str, chunk_size: int = 4000) -> List[str]:
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

def get_gaia_summary(text: str) -> str:
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME, 
            messages=[
                {"role": "system", "content": "You are a financial document summarizer. Summarize the following text concisely:"},
                {"role": "user", "content": text}
            ],
            temperature=0.7,
            max_tokens=500
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error in GaiaNet API call: {e}")
        return ""

def summarize_filing(accession_number: str) -> Optional[bytes]:
    connection = create_database_connection()
    if not connection:
        return None

    try:
        if not check_column_exists(connection, 'summary', 'sec_filings'):
            with connection.cursor() as cursor:
                cursor.execute("ALTER TABLE sec_filings ADD COLUMN summary LONGBLOB")
            print("Added 'summary' column to sec_filings table")

        with connection.cursor() as cursor:
            cursor.execute("SELECT cleaned_text FROM sec_filings WHERE accession_number = %s", (accession_number,))
            result = cursor.fetchone()
            if not result:
                print(f"No cleaned content found for accession number: {accession_number}")
                return None
            cleaned_content_blob = result[0]
        
        cleaned_content = decode_blob(cleaned_content_blob)
        chunks = chunk_text(cleaned_content)
        summaries = []
        
        for chunk in chunks:
            summary = get_gaia_summary(chunk)
            summaries.append(summary)
            
        final_summary = " ".join(summaries)
        summary_blob = encode_blob(final_summary)
        
        with connection.cursor() as cursor:
            cursor.execute("UPDATE sec_filings SET summary = %s WHERE accession_number = %s", 
                           (summary_blob, accession_number))
            connection.commit()

        print(f"Updated summary for accession number: {accession_number}")
        return summary_blob

    except Exception as e:
        print(f"Error summarizing filing: {e}")
        return None
    finally:
        connection.close()
