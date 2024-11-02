# summary.py
import os
from typing import List, Optional
import openai
from utils import (
    create_database_connection,
    handle_error,
    check_column_exists,
    decode_blob,
    encode_blob
)


client = openai.OpenAI(base_url="https://llama.us.gaianet.network/v1", api_key="GAIA")

def chunk_text(text: str, chunk_size: int = 4000) -> List[str]:
    return [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

def get_gaia_summary(text: str) -> str:
    try:
        response = client.chat.completions.create(
            model="llama", 
            messages=[
                {"role": "system", "content": "You are a financial document summarizer. Summarize the following text concisely:"},
                {"role": "user", "content": text}
            ],
            temperature=0.7,
            max_tokens=500
        )
        return response.choices[0].message.content
    except Exception as e:
        handle_error(f"Error in GaiaNet API call: {e}")
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
        handle_error(f"Error summarizing filing: {e}")
        return None
    finally:
        connection.close()

