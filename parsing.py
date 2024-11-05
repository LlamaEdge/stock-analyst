import os
from tempfile import NamedTemporaryFile
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
load_dotenv('./.env', override=False)
def ensure_parsed_text_column_exists() -> None:
    connection = create_database_connection()
    if not connection:
        print("Failed to connect to the database.")
        return
    try:
        if not check_column_exists(connection, 'parsed_text', 'sec_filings'):
            create_column('sec_filings', 'parsed_text', 'LONGBLOB', connection)
            print("'parsed_text' column added to sec_filings table.")
        else:
            print("'parsed_text' column already exists.")
    except Exception as e:
        print(f"Error altering the table: {e}")
    finally:
        connection.close()
def retrieve_and_save_parsed_blob(accession_number: str) -> None:
    parts_size = 20  
    connection = create_database_connection()
    if not connection:
        print("Failed to connect to the database.")
        return
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT content FROM sec_filings WHERE accession_number = %s", (accession_number,))
        result = cursor.fetchone()

        if result:
            html_content = decode_blob(result[0]) if result[0] else None
            if html_content:
                total_lines = html_content.splitlines()
                lines_per_part = len(total_lines) // parts_size
                content_parts = [
                    "\n".join(total_lines[i:i + lines_per_part]) for i in range(0, len(total_lines), lines_per_part)
                ]
                api_key = os.environ.get('LLAMA_CLOUD_API_KEY') or os.getenv('LLAMA_CLOUD_API_KEY') 
                parser = LlamaParse(api_key=api_key, result_type="markdown", show_progress=True)
                output_file_path = os.path.join(os.path.expanduser('~/Desktop'), f"parsed_output_{accession_number}.txt")
                with open(output_file_path, 'w', encoding='utf-8') as output_file:
                    for index, part_content in enumerate(content_parts):
                        with NamedTemporaryFile(delete=False, suffix='.html', mode='w', encoding='utf-8') as temp_file:
                            temp_file.write(part_content)
                            temp_file_path = temp_file.name
                        try:
                            documents = SimpleDirectoryReader(input_files=[temp_file_path], file_extractor={".html": parser}).load_data()
                            parsed_part = "\n\n".join(doc.text for doc in documents if hasattr(doc, 'text'))
                            output_file.write(parsed_part + "\n\n")
                        except Exception as e:
                            print(f"Error while parsing part {index + 1}: {e}")
                        os.remove(temp_file_path)
                with open(output_file_path, 'r', encoding='utf-8') as output_file:
                    parsed_markdown = output_file.read()
                parsed_blob = encode_blob(parsed_markdown) 
                update_parsed_text(connection, accession_number, parsed_blob)
            else:
                print(f"No content found for accession number {accession_number}.")
        else:
            print(f"No filing found for the accession number: {accession_number}.")
    except Exception as e:
        print(f"Error retrieving or updating the database: {e}")
    finally:
        cursor.close()
        connection.close()
