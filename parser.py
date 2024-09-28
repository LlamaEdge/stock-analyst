import os
import mysql.connector
from tempfile import NamedTemporaryFile
from mysql.connector import Error
from dotenv import load_dotenv
from llama_parse import LlamaParse
from llama_index.core import SimpleDirectoryReader

# Function to create a database connection
def create_database_connection():
    try:
        connection = mysql.connector.connect(
            host='',
            database='sec',
            user='',
            password=''
        )
        return connection if connection.is_connected() else None
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

# Check if the 'parsed_text' column exists
def check_column_exists(cursor, column_name: str, table_name: str) -> bool:
    cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE '{column_name}';")
    return cursor.fetchone() is not None

# Alter the table and add parsed_text column if it doesn't exist
def ensure_parsed_text_column_exists():
    connection = create_database_connection()
    if connection is None:
        print("Failed to connect to the database.")
        return

    try:
        cursor = connection.cursor()
        # Check if 'parsed_text' column exists
        if not check_column_exists(cursor, 'parsed_text', 'sec_filings'):
            alter_table_query = """
            ALTER TABLE sec_filings 
            ADD COLUMN parsed_text LONGTEXT;
            """
            cursor.execute(alter_table_query)
            connection.commit()
            print("'parsed_text' column added to sec_filings table.")
        else:
            print("'parsed_text' column already exists.")
    except Error as e:
        print(f"Error altering the table: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Function to retrieve BLOB content, parse it, and save the result as text
def retrieve_and_save_parsed_blob(accession_number: str) -> None:
    connection = create_database_connection()
    if connection is None:
        print("Failed to connect to the database.")
        return

    try:
        cursor = connection.cursor()
        cursor.execute("SELECT content FROM sec_filings WHERE accession_number = %s", (accession_number,))
        result = cursor.fetchone()
        if result:
            html_content = result[0].decode('utf-8') if isinstance(result[0], bytes) else result[0]
            # Create a temporary HTML file
            with NamedTemporaryFile(delete=False, suffix='.html', mode='w', encoding='utf-8') as temp_file:
                temp_file.write(html_content)
                temp_file_path = temp_file.name
            load_dotenv()
            #Add LLAMA-PARSE API key
            api_key = os.getenv('LLAMA_CLOUD_API_KEY', 'llx-')
            parser = LlamaParse(api_key=api_key, result_type="markdown")
            documents = SimpleDirectoryReader(input_files=[temp_file_path], file_extractor={".html": parser}).load_data()
            # Clean up the temporary file
            os.remove(temp_file_path)
            parsed_markdown = "\n\n".join(doc.text for doc in documents if hasattr(doc, 'text'))
            update_query = "UPDATE sec_filings SET parsed_text = %s WHERE accession_number = %s"
            cursor.execute(update_query, (parsed_markdown, accession_number))
            connection.commit()
            print(f"Parsed content saved for accession number: {accession_number}")
        else:
            print("No content found for the given accession number.")
    except Error as e:
        print(f"Error retrieving or updating the database: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Example usage
if __name__ == "__main__":
    ensure_parsed_text_column_exists()
    retrieve_and_save_parsed_blob(accession_number="0000320193-23-000106")
