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
load_dotenv('./.env', override=False)
GAIA_API_KEY = os.getenv("GAIANET_API_KEY") or os.environ.get("GAIANET_API_KEY")
GAIA_API_URL = os.getenv("GAIANET_API_URL") or os.environ.get("GAIANET_API_URL")
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
ticker_input = st.sidebar.text_input("Enter Stock Ticker:", value=st.session_state.selected_ticker if st.session_state.selected_ticker else "")

if st.sidebar.button("Load Stock Data"):
    st.session_state.selected_ticker = ticker_input.upper()
    if st.session_state.selected_ticker:
        news = get_company_news(st.session_state.selected_ticker)
        update_system_message(news=news)  

if st.session_state.selected_ticker:
    display_stock_info(st.session_state.selected_ticker)
    with st.sidebar:
        st.subheader("Stock Chart")
        st.components.v1.html(
            get_tradingview_widget(st.session_state.selected_ticker),
            height=400
        )

    st.sidebar.subheader("SEC Filings")
    filings = get_sec_filings_for_ticker(st.session_state.selected_ticker)
    
    if not filings:
        st.sidebar.info(f"No SEC filings found for {st.session_state.selected_ticker}")
    else:
        for filing in filings:
            filing_label = f"{filing['form']} - {filing['filing_date']}"
            with st.sidebar.expander(filing_label):
                summary = filing.get('summary', 'No summary available')
                st.text_area(
                    "Summary",
                    value=summary,
                    height=150,
                    key=f"summary_{filing['accession_number']}_sidebar",
                    disabled=True
                )
                if st.button("Use This Summary", key=f"select_{filing['accession_number']}"):
                    current_news = get_company_news(st.session_state.selected_ticker)
                    update_system_message(summary=summary, news=current_news) 
                    st.success("Summary selected! Future responses will be informed by this summary and recent news.")


    st.sidebar.subheader("Recent News")
    news = get_company_news(st.session_state.selected_ticker)
    if news:
        for article in news:
            with st.sidebar.expander(article['title']):
                st.write(f"*{article['publisher']}*")
                st.write(f"[Read more]({article['link']})")
                st.write("Content Preview:")
                st.write(article['content'][:200] + "...")

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
