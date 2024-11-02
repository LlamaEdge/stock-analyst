from utils import (
    create_database_connection,
    handle_error,
    check_column_exists,
    encode_blob,
    decode_blob
)

def is_potential_encoded_text(line, encoding_symbols='+=/$'):
    line = line.strip()
    if not line:
        return False
    if not all(c.isupper() or not c.isalpha() for c in line):
        return False
    if not any(symbol in line for symbol in encoding_symbols):
        return False
    return True

def clean_text(text: str) -> str:
    cleaned_lines = []
    previous_line_empty = False
    for line in text.split('\n'):
        if not is_potential_encoded_text(line):
            current_line_empty = len(line.strip()) == 0
            if not current_line_empty or not previous_line_empty:
                cleaned_lines.append(line)
            previous_line_empty = current_line_empty

    return '\n'.join(cleaned_lines)

def clean_and_store_filing(accession_number: str) -> bool:
    connection = create_database_connection()
    if not connection:
        return False

    try:
        # Check if 'cleaned_text' column exists, create if not
        if not check_column_exists(connection, 'cleaned_text', 'sec_filings'):
            with connection.cursor() as cursor:
                cursor.execute("ALTER TABLE sec_filings ADD COLUMN cleaned_text LONGBLOB")
            print("Added 'cleaned_text' column to sec_filings table")
        with connection.cursor() as cursor:
            cursor.execute("SELECT parsed_text FROM sec_filings WHERE accession_number = %s", (accession_number,))
            result = cursor.fetchone()
            if not result:
                print(f"No parsed content found for accession number: {accession_number}")
                return False
            parsed_content_blob = result[0]
        parsed_content = decode_blob(parsed_content_blob)
        cleaned_content = clean_text(parsed_content)
        cleaned_content_blob = encode_blob(cleaned_content)
        with connection.cursor() as cursor:
            cursor.execute("UPDATE sec_filings SET cleaned_text = %s WHERE accession_number = %s", 
                           (cleaned_content_blob, accession_number))
            connection.commit()

        print(f"Cleaned and stored content for accession number: {accession_number}")
        return True
    except Exception as e:
        handle_error(f"Error cleaning and storing filing: {e}")
        return False
    finally:
        connection.close()
