import streamlit as st
import pandas as pd
from datetime import datetime
from typing import Optional
from utils import (
    create_database_connection,
    get_filing_text_by_accession_number,
    fetch_ticker_to_cik_mapping,
    custom_fetch_and_save_filings,
    encode_blob,
    decode_blob,
    update_parsed_text,
    execute_query,
    create_database_if_not_exists,
    create_table,
    DownloadMetadata,
)
from mysql.connector import Error
from pathlib import Path

class SECFilingApp:
    def __init__(self):
        create_database_if_not_exists()
        self.connection = create_database_connection()
        if not self.connection:
            st.error("Failed to connect to database. Please check your database configuration.")
            st.stop()
        table_name = "sec_filings"
        columns = {
            'company_identifier': 'VARCHAR(50)',
            'form': 'VARCHAR(10)',
            'accession_number': 'VARCHAR(20)',
            'filing_date': 'DATE',
            'report_date': 'DATE',
            'file_url': 'VARCHAR(255)',
            'content': 'LONGBLOB',
            'parsed_text': 'LONGBLOB',
            'cleaned_text': 'LONGBLOB',
            'summary': 'LONGBLOB'
        }
        create_table(table_name, columns, self.connection)
        self.setup_page_config()

    def setup_page_config(self) -> None:
        st.set_page_config(
            page_title="SEC Filings Manager",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        st.title('SEC Filings Database Manager')

    def handle_error(self, error: Exception, operation: str) -> None:
        error_message = f"Error during {operation}: {str(error)}"
        st.error(error_message)
        if st.checkbox("Show technical details"):
            st.code(f"Error type: {type(error).__name__}\n{str(error)}")

    def load_filings(self) -> Optional[pd.DataFrame]:
        try:
            query = """
                SELECT 
                    company_identifier, 
                    form, 
                    accession_number, 
                    filing_date, 
                    report_date, 
                    file_url 
                FROM sec_filings
                ORDER BY filing_date DESC
            """
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                
            if results:
                return pd.DataFrame(
                    results,
                    columns=['Company', 'Form', 'Accession Number', 
                            'Filing Date', 'Report Date', 'File URL']
                )
            return None
        except Exception as e:
            self.handle_error(e, "loading filings")
            return None

    def view_filings(self) -> None:
        st.header('View SEC Filings')
        
        df = self.load_filings()
        if df is not None:
            # Add filters
            col1, col2 = st.columns(2)
            with col1:
                company_filter = st.multiselect(
                    'Filter by Company:',
                    options=sorted(df['Company'].unique())
                )
            with col2:
                form_filter = st.multiselect(
                    'Filter by Form Type:',
                    options=sorted(df['Form'].unique())
                )
            
            # Apply filters
            filtered_df = df.copy()
            if company_filter:
                filtered_df = filtered_df[filtered_df['Company'].isin(company_filter)]
            if form_filter:
                filtered_df = filtered_df[filtered_df['Form'].isin(form_filter)]
            
            st.dataframe(
                filtered_df,
                use_container_width=True,
                column_config={
                    "File URL": st.column_config.LinkColumn("File URL")
                }
            )

            self._display_filing_content(filtered_df)
        else:
            st.info('No filings found in the database.')

    def _display_filing_content(self, df: pd.DataFrame) -> None:
        st.subheader('View Filing Content')
        selected_accession = st.selectbox(
            'Select an Accession Number:',
            df['Accession Number'].tolist()
        )
        
        if st.button('View Content', key='view_content'):
            with st.spinner('Loading filing content...'):
                try:
                    content = get_filing_text_by_accession_number(
                        selected_accession, 
                        self.connection
                    )
                    if content:
                        st.text_area(
                            'Filing Content',
                            content,
                            height=400
                        )
                    else:
                        st.warning('No content found for this filing.')
                except Exception as e:
                    self.handle_error(e, "retrieving filing content")

    def download_filings(self) -> None:
        st.header('Download New SEC Filings')

        with st.form("download_form"):
            col1, col2 = st.columns(2)
            with col1:
                ticker = st.text_input(
                    'Company Ticker',
                    help='Enter the stock ticker symbol (e.g., AAPL)'
                ).upper()
                form_type = st.selectbox(
                    'Form Type',
                    ['10-K', '10-Q', '8-K', '20-F', '6-K']
                )
            with col2:
                limit = st.number_input(
                    'Number of Filings',
                    min_value=1,
                    max_value=100,
                    value=1
                )
                
                today = datetime.now().date()
                after_date = st.date_input(
                    "Start Date",
                    value=datetime(today.year - 1, today.month, today.day).date()
                )
                before_date = st.date_input("End Date", value=today)

            user_agent = st.text_input(
                'User Agent',
                value="MyCompanyName/1.0 (contact@example.com)",
                help='Identify yourself to the SEC EDGAR system'
            )

            submitted = st.form_submit_button("Download Filings")
            
            if submitted:
                try:
                    with st.spinner('Fetching CIK...'):
                        cik = fetch_ticker_to_cik_mapping(ticker)
                    
                    if not cik:
                        st.error(f"Could not find CIK for ticker {ticker}")
                        return

                    metadata = DownloadMetadata(
                        download_folder=Path("/tmp"),
                        form=form_type,
                        cik=cik,
                        ticker=ticker,
                        limit=limit,
                        before=before_date,
                        after=after_date
                    )
                    
                    with st.spinner('Downloading filings...'):
                        downloaded = custom_fetch_and_save_filings(
                            metadata,
                            user_agent,
                            self.connection
                        )
                    st.success(f"Successfully downloaded {downloaded} filings.")
                except Exception as e:
                    self.handle_error(e, "downloading filings")
    def generate_summary(self) -> None:
        st.header('Generate Filing Summary')

        df = self.load_filings()
        if df is not None:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                selected_filing = st.selectbox(
                    'Select Filing to Summarize:',
                    df['Accession Number'].tolist(),
                    format_func=lambda x: (
                        f"{df[df['Accession Number']==x]['Company'].iloc[0]} - "
                        f"{df[df['Accession Number']==x]['Form'].iloc[0]} - {x}"
                    )
                )

            with col2:
                show_full = st.checkbox('Show Full Summary', value=False)

            generate_col, view_col = st.columns(2)
            with generate_col:
                if st.button('Generate New Summary', type='primary'):
                    try:
                        with st.spinner('Generating summary... This may take a few minutes.'):
                            progress_placeholder = st.empty()
                            progress_placeholder.info('Checking for cleaned text...')
                            cursor = self.connection.cursor()
                            cursor.execute(
                                "SELECT cleaned_text FROM sec_filings WHERE accession_number = %s",
                                (selected_filing,)
                            )
                            result = cursor.fetchone()
                            cursor.close()
                            if not result or not result[0]:
                                progress_placeholder.error(
                                    'No cleaned text found. Please clean the filing first '
                                    'using the Update Parsed Text function.'
                                )
                                return
                            progress_placeholder.info('Generating summary using GaiaNet LLM...')
                            from summary import summarize_filing                            
                            summary_blob = summarize_filing(selected_filing)                            
                            if summary_blob:
                                summary_text = decode_blob(summary_blob)
                                progress_placeholder.success('Summary generated successfully!')                                
                                if show_full:
                                    st.text_area(
                                        'Complete Summary:',
                                        value=summary_text,
                                        height=400,
                                        key='new_summary_full'
                                    )
                                else:
                                    preview_length = min(1000, len(summary_text))
                                    st.text_area(
                                        'Summary Preview:',
                                        value=f"{summary_text[:preview_length]}{'...' if len(summary_text) > preview_length else ''}",
                                        height=200,
                                        key='new_summary_preview'
                                    )
                                    if len(summary_text) > preview_length:
                                        st.info("Showing preview. Select 'Show Full Summary' for complete text.")
                            else:
                                progress_placeholder.error('Failed to generate summary.')                   
                    except Exception as e:
                        self.handle_error(e, "generating summary")
            with view_col:
                if st.button('View Existing Summary'):
                    try:
                        cursor = self.connection.cursor()
                        cursor.execute(
                            "SELECT summary FROM sec_filings WHERE accession_number = %s",
                            (selected_filing,)
                        )
                        result = cursor.fetchone()
                        cursor.close()
                        if result and result[0]:
                            summary_text = decode_blob(result[0])           
                            if show_full:
                                st.text_area(
                                    'Existing Summary:',
                                    value=summary_text,
                                    height=400,
                                    key='existing_summary_full'
                                )
                            else:
                                preview_length = min(1000, len(summary_text))
                                st.text_area(
                                    'Summary Preview:',
                                    value=f"{summary_text[:preview_length]}{'...' if len(summary_text) > preview_length else ''}",
                                    height=200,
                                    key='existing_summary_preview'
                                )
                                if len(summary_text) > preview_length:
                                    st.info("Showing preview. Select 'Show Full Summary' for complete text.")
                        else:
                            st.warning("No existing summary found for this filing.")                   
                    except Exception as e:
                        self.handle_error(e, "retrieving existing summary")
            if st.checkbox('Show Summary Statistics'):
                try:
                    cursor = self.connection.cursor()
                    cursor.execute(
                        """
                        SELECT 
                            CASE 
                                WHEN cleaned_text IS NOT NULL THEN 'Yes'
                                ELSE 'No'
                            END as has_cleaned_text,
                            CASE 
                                WHEN summary IS NOT NULL THEN 'Yes'
                                ELSE 'No'
                            END as has_summary
                        FROM sec_filings 
                        WHERE accession_number = %s
                        """,
                        (selected_filing,)
                    )
                    result = cursor.fetchone()
                    cursor.close()

                    if result:
                        st.markdown("### Filing Status")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Cleaned Text Available", result[0])
                        with col2:
                            st.metric("Summary Available", result[1])
                    else:
                        st.warning("Could not retrieve filing statistics.")

                except Exception as e:
                    self.handle_error(e, "retrieving filing statistics")

        else:
            st.info('No filings available to summarize.')
    def update_filing(self) -> None:
        st.header('Update Parsed Text')
        df = self.load_filings()
        if df is not None:
            col1, col2 = st.columns(2)    
            with col1:
                selected_filing = st.selectbox(
                    'Select Filing to Update:',
                    df['Accession Number'].tolist(),
                    format_func=lambda x: f"{df[df['Accession Number']==x]['Company'].iloc[0]} - {x}"
                )       
            with col2:
                update_type = st.radio(
                    "Update Method:",
                    ["View/Edit Current Text", "Parse Filing with LlamaParse"],
                    help="Choose whether to manually edit the text or parse the filing using LlamaParse"
                )   
            try:
                if update_type == "View/Edit Current Text":
                    current_text = get_filing_text_by_accession_number(
                        selected_filing,
                        self.connection
                    )
                    
                    updated_text = st.text_area(
                        'Edit Parsed Text:',
                        value=current_text if current_text else '',
                        height=400
                    )
                    
                    if st.button('Update Text'):
                        with st.spinner('Updating parsed text...'):
                            parsed_blob = encode_blob(updated_text)
                            update_parsed_text(
                                self.connection,
                                selected_filing,
                                parsed_blob
                            )
                        st.success('Successfully updated parsed text.')
                
                else:  
                    from parsing import ensure_parsed_text_column_exists, retrieve_and_save_parsed_blob
                    ensure_parsed_text_column_exists()                  
                    if st.button('Parse Filing'):
                        try:
                            with st.spinner('Parsing filing content... This may take a few minutes.'):
                                status_container = st.empty()
                                status_container.info('Initializing parsing process...')
                                retrieve_and_save_parsed_blob(selected_filing)
                                status_container.success('Parsing completed successfully!')
                                cursor = self.connection.cursor()
                                cursor.execute(
                                    "SELECT parsed_text FROM sec_filings WHERE accession_number = %s",
                                    (selected_filing,)
                                )
                                result = cursor.fetchone()
                                cursor.close()                           
                                if result and result[0]:
                                    parsed_content = decode_blob(result[0])
                                    st.text_area(
                                        'Parsed Content Preview:',
                                        value=parsed_content[:1000] + "...",
                                        height=200
                                    )
                                    st.info("Above is a preview of the first 1000 characters. The complete parsed text has been saved to the database.")
                                else:
                                    st.warning("No parsed content found after parsing.")                       
                        except Exception as e:
                            st.error(f"Error during parsing: {str(e)}")
                            self.handle_error(e, "parsing filing")
                    if st.button('View Current Parsed Content'):
                        try:
                            cursor = self.connection.cursor()
                            cursor.execute(
                                "SELECT parsed_text FROM sec_filings WHERE accession_number = %s",
                                (selected_filing,)
                            )
                            result = cursor.fetchone()
                            cursor.close()        
                            if result and result[0]:
                                parsed_content = decode_blob(result[0])
                                st.text_area(
                                    'Current Parsed Content:',
                                    value=parsed_content,
                                    height=400
                                )
                            else:
                                st.warning("No parsed content found for this filing.")
                        except Exception as e:
                            self.handle_error(e, "retrieving parsed content")      
            except Exception as e:
                self.handle_error(e, "updating/parsing filing")
        else:
            st.info('No filings available to update.')
    def clean_filing(self) -> None:
        st.header('Clean Filing Text')
        df = self.load_filings()
        if df is not None:
            col1, col2 = st.columns([2, 1])        
            with col1:
                selected_filing = st.selectbox(
                    'Select Filing to Clean:',
                    df['Accession Number'].tolist(),
                    format_func=lambda x: (
                        f"{df[df['Accession Number']==x]['Company'].iloc[0]} - "
                        f"{df[df['Accession Number']==x]['Form'].iloc[0]} - {x}"
                    )
                )               
            with col2:
                show_full = st.checkbox('Show Full Text', value=False)
            clean_tab, view_tab, stats_tab = st.tabs(["Clean Text", "View Text", "Statistics"])
            with clean_tab:
                from cleaner import clean_and_store_filing, clean_text         
                if st.button('Clean Filing Text', type='primary'):
                    try:
                        with st.spinner('Cleaning filing text...'):
                            progress_placeholder = st.empty()
                            progress_placeholder.info('Checking for parsed text...')
                            cursor = self.connection.cursor()
                            cursor.execute(
                                "SELECT parsed_text FROM sec_filings WHERE accession_number = %s",
                                (selected_filing,)
                            )
                            result = cursor.fetchone()
                            cursor.close()
                            if not result or not result[0]:
                                progress_placeholder.error(
                                    'No parsed text found. Please parse the filing first '
                                    'using the Update Parsed Text function.'
                                )
                                return
                            progress_placeholder.info('Cleaning and storing text...')
                            success = clean_and_store_filing(selected_filing)                      
                            if success:
                                progress_placeholder.success('Text cleaned and stored successfully!')
                                cursor = self.connection.cursor()
                                cursor.execute(
                                    "SELECT cleaned_text FROM sec_filings WHERE accession_number = %s",
                                    (selected_filing,)
                                )
                                cleaned_result = cursor.fetchone()
                                cursor.close()

                                if cleaned_result and cleaned_result[0]:
                                    cleaned_text = decode_blob(cleaned_result[0])
                                    if show_full:
                                        st.text_area(
                                            'Cleaned Text:',
                                            value=cleaned_text,
                                            height=400
                                        )
                                    else:
                                        preview_length = min(1000, len(cleaned_text))
                                        st.text_area(
                                            'Cleaned Text Preview:',
                                            value=f"{cleaned_text[:preview_length]}{'...' if len(cleaned_text) > preview_length else ''}",
                                            height=200
                                        )
                                        if len(cleaned_text) > preview_length:
                                            st.info("Showing preview. Select 'Show Full Text' for complete text.")
                            else:
                                progress_placeholder.error('Failed to clean and store text.')

                    except Exception as e:
                        self.handle_error(e, "cleaning filing text")

            with view_tab:
                if st.button('View Current Text'):
                    try:
                        parsed_col, cleaned_col = st.columns(2)            
                        cursor = self.connection.cursor()
                        with parsed_col:
                            st.subheader("Parsed Text")
                            cursor.execute(
                                "SELECT parsed_text FROM sec_filings WHERE accession_number = %s",
                                (selected_filing,)
                            )
                            parsed_result = cursor.fetchone()
                            if parsed_result and parsed_result[0]:
                                parsed_text = decode_blob(parsed_result[0])
                                if show_full:
                                    st.text_area(
                                        'Current Parsed Text:',
                                        value=parsed_text,
                                        height=400
                                    )
                                else:
                                    preview_length = min(1000, len(parsed_text))
                                    st.text_area(
                                        'Parsed Text Preview:',
                                        value=f"{parsed_text[:preview_length]}{'...' if len(parsed_text) > preview_length else ''}",
                                        height=200
                                    )
                            else:
                                st.warning("No parsed text available.")
                        with cleaned_col:
                            st.subheader("Cleaned Text")
                            cursor.execute(
                                "SELECT cleaned_text FROM sec_filings WHERE accession_number = %s",
                                (selected_filing,)
                            )
                            cleaned_result = cursor.fetchone()
                            if cleaned_result and cleaned_result[0]:
                                cleaned_text = decode_blob(cleaned_result[0])
                                if show_full:
                                    st.text_area(
                                        'Current Cleaned Text:',
                                        value=cleaned_text,
                                        height=400
                                    )
                                else:
                                    preview_length = min(1000, len(cleaned_text))
                                    st.text_area(
                                        'Cleaned Text Preview:',
                                        value=f"{cleaned_text[:preview_length]}{'...' if len(cleaned_text) > preview_length else ''}",
                                        height=200
                                    )
                            else:
                                st.warning("No cleaned text available.")
                        
                        cursor.close()

                    except Exception as e:
                        self.handle_error(e, "viewing filing text")

            with stats_tab:
                if st.button('Show Text Statistics'):
                    try:
                        cursor = self.connection.cursor()
                        cursor.execute("""
                            SELECT 
                                LENGTH(parsed_text) as parsed_length,
                                LENGTH(cleaned_text) as cleaned_length,
                                CASE 
                                    WHEN parsed_text IS NOT NULL THEN 'Yes'
                                    ELSE 'No'
                                END as has_parsed,
                                CASE 
                                    WHEN cleaned_text IS NOT NULL THEN 'Yes'
                                    ELSE 'No'
                                END as has_cleaned
                            FROM sec_filings 
                            WHERE accession_number = %s
                        """, (selected_filing,))
                        result = cursor.fetchone()
                        cursor.close()

                        if result:
                            st.markdown("### Text Statistics")
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Parsed Text Available", result[2])
                                if result[0]:
                                    st.metric("Parsed Text Length", f"{result[0]:,} bytes")
                            with col2:
                                st.metric("Cleaned Text Available", result[3])
                                if result[1]:
                                    st.metric("Cleaned Text Length", f"{result[1]:,} bytes")
                                    st.metric("Size Reduction", 
                                            f"{((result[0] - result[1]) / result[0] * 100):.1f}%")
                        else:
                            st.warning("Could not retrieve text statistics.")

                    except Exception as e:
                        self.handle_error(e, "retrieving text statistics")
        else:
            st.info('No filings available to clean.')

    def delete_filing(self) -> None:
        st.header('Delete Filing')
        
        df = self.load_filings()
        if df is not None:
            selected_filing = st.selectbox(
                'Select Filing to Delete:',
                df['Accession Number'].tolist(),
                format_func=lambda x: f"{df[df['Accession Number']==x]['Company'].iloc[0]} - {x}"
            )
            
            confirm = st.checkbox('I confirm I want to delete this filing')
            
            if st.button('Delete Filing', type='primary') and confirm:
                try:
                    with st.spinner('Deleting filing...'):
                        execute_query(
                            "DELETE FROM sec_filings WHERE accession_number = %s",
                            (selected_filing,),
                            self.connection
                        )
                    st.success('Filing deleted successfully.')
                    st.rerun()  
                except Exception as e:
                    self.handle_error(e, "deleting filing")
        else:
            st.info('No filings available to delete.')
    
    def run(self) -> None:
        
        try:
            with st.sidebar:
                st.header('Navigation')
                option = st.radio(
                    'Choose an operation:',
                    ['View Filings', 
                    'Download New Filings', 
                    'Update Parsed Text',
                    'Clean Filing Text', 
                    'Generate Summary',
                    'Delete Filing']
                )
            
                st.markdown('---')
                st.markdown(
                    '📊 **SEC Filings Manager**\n\n'
                    'This tool helps you manage SEC filings by:'
                    '\n- Viewing existing filings'
                    '\n- Downloading new filings'
                    '\n- Updating parsed text'
                    '\n- Cleaning filing text'  
                    '\n- Generating summaries'
                    '\n- Deleting filings'
                )
            if option == 'View Filings':
                self.view_filings()
            elif option == 'Download New Filings':
                self.download_filings()
            elif option == 'Update Parsed Text':
                self.update_filing()
            elif option == 'Clean Filing Text':  
                self.clean_filing()
            elif option == 'Generate Summary':
                self.generate_summary()
            elif option == 'Delete Filing':
                self.delete_filing()

        except Exception as e:
            self.handle_error(e, "running application")
        finally:
            if hasattr(self, 'connection') and self.connection:
                self.connection.close()

if __name__ == "__main__":
    app = SECFilingApp()
    app.run()
