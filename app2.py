import streamlit as st
import yfinance as yf
import openai
from datetime import datetime
from utils import (
    create_database_connection, 
    decode_blob
)
from widgets import get_tradingview_widget, get_company_news, get_stock_data, get_full_company_data
from dotenv import load_dotenv
import os
import json
load_dotenv('./.env', override=False)
GAIA_API_KEY = os.getenv("GAIA_API_KEY") or os.environ.get("GAIA_API_KEY")
GAIA_API_URL = os.getenv("GAIA_API_URL") or os.environ.get("GAIA_API_URL")
MODEL_NAME = os.getenv("MODEL_NAME", "llama") or os.environ.get("MODEL_NAME", "llama")

general_client = openai.OpenAI(
    base_url=GAIA_API_URL,
    api_key=GAIA_API_KEY
)

st.set_page_config(
    page_title="Financial Analyst",
    page_icon="📈",
    layout="wide"
)

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "current_system_message" not in st.session_state:
    st.session_state.current_system_message = "You are a helpful assistant."
if "selected_ticker" not in st.session_state:
    st.session_state.selected_ticker = None

def get_sec_filings_for_ticker(ticker):
    connection = create_database_connection()
    if not connection:
        return []
    
    try:
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute("""
                SELECT 
                    accession_number,
                    form,
                    filing_date,
                    summary
                FROM sec_filings 
                WHERE company_identifier = %s
                ORDER BY filing_date DESC
            """, (ticker.upper(),))
            
            filings = cursor.fetchall()
            
            processed_filings = []
            for filing in filings:
                processed_filing = {
                    'accession_number': filing['accession_number'],
                    'form': filing['form'],
                    'filing_date': filing['filing_date'].strftime('%Y-%m-%d') if filing['filing_date'] else 'N/A',
                    'summary': decode_blob(filing['summary']) if filing.get('summary') else None
                }
                processed_filings.append(processed_filing)
            
            return processed_filings
            
    except Exception as e:
        st.error(f"Error fetching SEC filings: {e}")
        return []
    finally:
        if connection:
            connection.close()

def format_stock_value(value, is_price=True):
    try:
        if isinstance(value, (int, float)):
            return f"{value:.2f}"
        elif isinstance(value, str) and value.replace('.', '').isdigit():
            return f"{float(value):.2f}"
        else:
            return "N/A"
    except:
        return "N/A"

def update_system_message(summary=None, news=None):
    system_message = "You are a helpful assistant."
    if summary:
        system_message += f"\n\nSEC Filing Summary:\n{summary}"
    if news:
        system_message += "\n\nRecent News Articles:"
        for article in news:
            system_message += f"\n\nTitle: {article['title']}"
            if article.get('content'):
                system_message += f"\nContent: {article['content']}..." 
    st.session_state.current_system_message = system_message
def display_stock_info(ticker):
    stock_info = get_stock_data(ticker)
    name = stock_info.get('name', 'N/A')
    price = format_stock_value(stock_info.get('price', 'N/A'))
    change = format_stock_value(stock_info.get('change', 'N/A'))
    
    st.sidebar.subheader("Stock Information")
    st.sidebar.write(f"Company: {name}")
    st.sidebar.write(f"Price: ${price}")
    st.sidebar.write(f"Change: {change}%")

def process_message(message):
    try:
        general_response = general_client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": st.session_state.current_system_message},
                {"role": "user", "content": message},
                *st.session_state.chat_history
            ]
        )
        return general_response.choices[0].message.content
    except Exception as e:
        st.error(f"Error: {e}")
        return "An error occurred while processing your request."

st.sidebar.title("Stock Selection")
def search_tickers(query: str):
    try:
        prompt = f"""Given the search query "{query}", provide relevant stock ticker symbols and company names. 
        Return exactly 5 most relevant results as a JSON array. Format as:
        [
            {{"ticker": "TSLA", "name": "Tesla, Inc."}},
            {{"ticker": "TM", "name": "Toyota Motor Corporation"}},
            {{"ticker": "F", "name": "Ford Motor Company"}},
            {{"ticker": "GM", "name": "General Motors Company"}},
            {{"ticker": "RIVN", "name": "Rivian Automotive, Inc."}}
        ]"""
        
        response = general_client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "You are a financial data assistant specializing in stock tickers."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3  # Lower temperature for more focused results
        )
        content = response.choices[0].message.content.strip()
        try:
            # Find the JSON array in the response
            start = content.find('[')
            end = content.rfind(']') + 1
            if start != -1 and end != -1:
                json_str = content[start:end]
                suggestions = json.loads(json_str)
                return suggestions
            else:
                st.error("Invalid response format from LLM")
                return []
        except json.JSONDecodeError as e:
            st.error(f"Error parsing LLM response: {e}")
            return []
            
    except Exception as e:
        st.error(f"Error in ticker search: {e}")
        return []

def handle_stock_selection(ticker):
    st.session_state.selected_ticker = ticker.upper()
    if st.session_state.selected_ticker:
        update_system_message(news=get_company_news(st.session_state.selected_ticker))
        st.rerun()

ticker_input = st.sidebar.text_input("Enter Stock Ticker:", 
    value=st.session_state.get("selected_ticker", ""),
    placeholder="e.g., AAPL, TSLA"
)
if st.sidebar.button("Load Stock Data"):
    handle_stock_selection(ticker_input)

st.sidebar.divider()

# Search
query = st.sidebar.text_input("Or search for a company:", placeholder="Example: Tesla, tech companies")

if query:
    suggestions = search_tickers(query)
    if suggestions:
        for s in suggestions:
            st.sidebar.write(f"**{s['ticker']}** - {s['name']}")
            if st.sidebar.button("Select", key=f"select_{s['ticker']}"):
                handle_stock_selection(s['ticker'])
            st.sidebar.divider()
    else:
        st.sidebar.info("No matching companies found.")

# Stock Info
if ticker := st.session_state.get("selected_ticker"):
    display_stock_info(ticker)

    with st.sidebar:
        st.divider()
        st.subheader("Stock Chart")
        st.components.v1.html(get_tradingview_widget(ticker), height=400)

        st.subheader("SEC Filings")
        filings = get_sec_filings_for_ticker(ticker)
        if filings:
            for filing in filings:
                with st.expander(f"{filing['form']} - {filing['filing_date']}"):
                    st.text_area("Summary", filing.get('summary', 'No summary available'), height=150, disabled=True)
                    if st.button("Use This Summary", key=f"summary_{filing['accession_number']}"):
                        update_system_message(summary=filing['summary'], news=get_company_news(ticker))
                        st.success("Summary selected!")
        else:
            st.info(f"No SEC filings found for {ticker}")

        st.subheader("Recent News")
        for article in get_company_news(ticker) or []:
            with st.expander(article['title']):
                st.write(f"*{article['publisher']}*")
                st.write(f"[Read more]({article['link']})")
                st.write("Content Preview:", article['content'][:200], "...")
st.title("Financial Analyst")

for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

user_input = st.chat_input("Ask about the selected stock or anything else:")

if user_input:
    st.chat_message("user").markdown(user_input)
    st.session_state.chat_history.append({"role": "user", "content": user_input})
    
    with st.spinner('Processing...'):
        assistant_response = process_message(user_input)
        st.session_state.chat_history.append({"role": "assistant", "content": assistant_response})
        with st.chat_message("assistant"):
            st.markdown(assistant_response)

# if st.sidebar.checkbox("Show Current System Message"):
#     st.sidebar.text_area("Current System Message", st.session_state.current_system_message, height=200)
