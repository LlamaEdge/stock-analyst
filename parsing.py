import os
import nest_asyncio
from tempfile import NamedTemporaryFile, gettempdir
from dotenv import load_dotenv
from llama_parse import LlamaParse
from llama_index.core import SimpleDirectoryReader
from utils import (
    create_database_connection,
    create_column,
    check_column_exists,
    decode_blob,
    encode_blob,
    update_parsed_text,
)

nest_asyncio.apply()

load_dotenv('./.env', override=False)

def cleanup_temp_files():
    temp_dir = gettempdir()
    for filename in os.listdir(temp_dir):
        if filename.startswith('tmp') and filename.endswith('.html'):
            try:
                file_path = os.path.join(temp_dir, filename)
                os.remove(file_path)
                print(f"Cleaned up temporary file: {filename}")
            except Exception as e:
                print(f"Error cleaning up {filename}: {e}")

def ensure_parsed_text_column_exists() -> None:
    connection = create_database_connection()
    if not connection:
        print("Failed to connect to the database.")
        return
    try:
        if not check_column_exists(connection, 'parsed_text', 'sec_filings'):
            create_column('sec_filings', 'parsed_text', 'LONGBLOB', connection)
            print("'parsed_text' column added to sec_filings table.")
    except Exception as e:
        print(f"Error altering the table: {e}")
    finally:
        if connection:
            connection.close()

def retrieve_and_save_parsed_blob(accession_number: str) -> None:
    parts_size = 20  
    connection = create_database_connection()
    if not connection:
        print("Failed to connect to the database.")
        return
    
    temp_files = []  # Track temp files for cleanup
    
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT content FROM sec_filings WHERE accession_number = %s", (accession_number,))
        result = cursor.fetchone()

        if not result:
            print(f"No filing found for accession number: {accession_number}")
            return
            
        html_content = decode_blob(result[0]) if result[0] else None
        if not html_content:
            print(f"No content found for accession number {accession_number}")
            return

        total_lines = html_content.splitlines()
        lines_per_part = max(1, len(total_lines) // parts_size)
        content_parts = [
            "\n".join(total_lines[i:i + lines_per_part])
            for i in range(0, len(total_lines), lines_per_part)
        ]
        
        api_key = os.environ.get('LLAMA_CLOUD_API_KEY') or os.getenv('LLAMA_CLOUD_API_KEY')
        if not api_key:
            print("LLAMA_CLOUD_API_KEY not found in environment variables")
            return
            
        parser = LlamaParse(
            api_key=api_key,
            result_type="markdown",
            verbose=True
        )
        
        all_parsed_content = []
        
        for index, part_content in enumerate(content_parts):
            print(f"Processing part {index + 1}/{len(content_parts)}")
            try:
                temp_file = NamedTemporaryFile(
                    delete=False,
                    suffix='.html',
                    mode='w',
                    encoding='utf-8'
                )
                temp_files.append(temp_file.name)
                
                temp_file.write(part_content)
                temp_file.close()
                
                documents = SimpleDirectoryReader(
                    input_files=[temp_file.name],
                    file_extractor={".html": parser}
                ).load_data()
                
                parsed_part = "\n\n".join(
                    doc.text for doc in documents 
                    if hasattr(doc, 'text')
                )
                all_parsed_content.append(parsed_part)
                
            except Exception as e:
                print(f"Error processing part {index + 1}: {e}")
                continue

        if all_parsed_content:
            complete_parsed_content = "\n\n".join(all_parsed_content)
            parsed_blob = encode_blob(complete_parsed_content)
            update_parsed_text(connection, accession_number, parsed_blob)
            print(f"Updated 'parsed_text' for accession number: {accession_number}")
        else:
            print("No content was successfully parsed")
            
    except Exception as e:
        print(f"Error retrieving or updating the database: {e}")
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        
        for temp_file in temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception as e:
                print(f"Error removing temporary file {temp_file}: {e}")
        
        cleanup_temp_files()
