import streamlit as st
import yfinance as yf
import openai
from datetime import datetime
from utils import (
    create_database_connection, 
    decode_blob
)
from dotenv import load_dotenv
import os
import json
from tavily import TavilyClient
from typing import List, Dict, Optional


load_dotenv('./.env', override=False)
GAIA_API_KEY = os.getenv("GAIA_API_KEY") or os.environ.get("GAIA_API_KEY")
GAIA_API_URL = os.getenv("GAIA_API_URL") or os.environ.get("GAIA_API_URL")
MODEL_NAME = os.getenv("MODEL_NAME", "llama") or os.environ.get("MODEL_NAME", "llama")
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY") or os.environ.get("TAVILY_API_KEY")


if not GAIA_API_KEY or not GAIA_API_URL:
    st.error("Missing Gaia API configuration. Please check your environment variables.")
    openai_client = None
else:
    openai_client = openai.OpenAI(
        base_url=GAIA_API_URL,
        api_key=GAIA_API_KEY
    )

if not TAVILY_API_KEY:
    st.error("Tavily API key not found in environment variables")
    tavily_client = None
else:
    tavily_client = TavilyClient(api_key=TAVILY_API_KEY)


st.set_page_config(
    page_title="Financial Analyst",
    page_icon="📈",
    layout="wide"
)

# Initialize session state
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "current_system_message" not in st.session_state:
    st.session_state.current_system_message = "You are a helpful assistant."
if "selected_ticker" not in st.session_state:
    st.session_state.selected_ticker = None
if "news_loaded" not in st.session_state:
    st.session_state.news_loaded = False
if "news_data" not in st.session_state:
    st.session_state.news_data = None
if "system_context" not in st.session_state:
    st.session_state.system_context = {
        "news": None,
        "summary": None
    }

CRYPTO_TICKERS = {
    "BTC": "Bitcoin",
    "ETH": "Ethereum",
    "SOL": "Solana",
    "BNB": "Binance Coin",
    "DOGE": "Dogecoin",
    "XRP": "Ripple",
    "ADA": "Cardano",
    "TRX": "TRON",
    "BCH": "Bitcoin Cash",
    "DOT": "Polkadot",
    "POL": "Polygon",
    "LTC": "Litecoin",
    "NEAR": "NEAR Protocol",
    "UNI": "Uniswap"
}

def is_crypto_ticker(ticker: str) -> bool:
    return ticker.upper() in CRYPTO_TICKERS

def get_crypto_news(ticker: str, tavily_client, openai_client) -> List[Dict[str, str]]:
    if not tavily_client:
        return []
    
    try:
        crypto_name = CRYPTO_TICKERS.get(ticker.upper(), ticker)
        search_query = f"{crypto_name} cryptocurrency latest news price analysis"
        search_response = tavily_client.search(
            query=search_query,
            search_depth="advanced",
            max_results=5
        )
        
        if not search_response or "results" not in search_response:
            return []
        processed_articles = []
        for result in search_response["results"]:
            content = result.get("content", "Content not available")
            summary = summarize_content(content, openai_client) if openai_client else "Summary not available"
            
            processed_articles.append({
                "title": result.get("title", "No title"),
                "publisher": result.get("source", "Unknown"),
                "link": result.get("url", "#"),
                "content": content,
                "summary": summary,
                "scraped_at": datetime.now().isoformat()
            })
        
        return processed_articles
    except Exception as e:
        print(f"Error fetching crypto news for {ticker}: {str(e)}")
        return []

def get_company_news(ticker: str, tavily_client, openai_client) -> list:
    if st.session_state.news_data:
        return st.session_state.news_data
    if is_crypto_ticker(ticker):
        articles = get_crypto_news(ticker, tavily_client, openai_client)
        st.session_state.news_data = articles
        return articles
    
    try:
        stock = yf.Ticker(ticker)
        news = stock.news
        article_urls = [article.get("link") for article in news[:5] if article.get("link")]
        articles_content = fetch_article_content_with_tavily(article_urls, tavily_client)
        processed_articles = []
        for i, article in enumerate(news[:5]):
            content = articles_content[i]["raw_content"] if i < len(articles_content) else "Content not available"
            summary = summarize_content(content, openai_client)
            processed_articles.append({
                "title": article.get("title", "No title"),
                "publisher": article.get("publisher", "Unknown"),
                "link": article.get("link", "#"),
                "content": content,
                "summary": summary,
                "scraped_at": datetime.now().isoformat()
            })
        st.session_state.news_data = processed_articles
        return processed_articles
    except Exception as e:
        print(f"Error fetching news for {ticker}: {str(e)}")
        return []
    
def fetch_article_content_with_tavily(urls: List[str], tavily_client: TavilyClient) -> List[Dict[str, str]]:
    if not urls:
        return []
    
    try:
        response = tavily_client.extract(urls=urls)
        if not response or "results" not in response:
            return []
        
        articles = [{
            "url": result.get("url", ""),
            "raw_content": result.get("raw_content", "No content available")
        } for result in response["results"]]
        
        return articles
        
    except Exception as e:
        print(f"Error extracting content using Tavily: {str(e)}")
        return []

def summarize_content(text: str, client: openai.OpenAI) -> str:
    if not text or text == "No content available":
        return "No summary available"    
    try:
        prompt = f"Summarize the following news article in great detail:\n\n{text[:4000]}"
        response = client.chat.completions.create(
            model=MODEL_NAME, 
            messages=[
                {"role": "system", "content": "You are a financial news summarizer. Provide detailed summaries."},
                {"role": "user", "content": prompt}
            ]
        )
        
        return response.choices[0].message.content
        
    except Exception as e:
        print(f"Error generating summary: {e}")
        return "Summary generation failed"


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
    if summary:
        st.session_state.system_context["summary"] = summary
    if news:
        st.session_state.system_context["news"] = news
    system_message = "You are a helpful financial analyst assistant specializing in stock analysis and market insights."
    if st.session_state.system_context["summary"]:
        system_message += f"\n\nSEC Filing Summary:\n{st.session_state.system_context['summary']}"
    if st.session_state.system_context["news"]:
        system_message += "\n\nRecent News Context:"
        for article in st.session_state.system_context["news"][:3]:
            system_message += f"\n\nArticle: {article['title']}"
            if article.get('summary'):
                system_message += f"\nSummary: {article['summary']}"
            elif article.get('content'):
                system_message += f"\nContent: {article['content'][:500]}..."
    
    st.session_state.current_system_message = system_message

def get_stock_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        return {
            "name": info.get("longName", "N/A"),
            "price": info.get("currentPrice", "N/A"),
            "change": info.get("regularMarketChangePercent", "N/A"),
        }
    except Exception as e:
        print(f"Error fetching stock data for {ticker}: {str(e)}")
        return {}

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
    if not openai_client:
        return "API client not initialized. Please check configuration."
        
    try:
        messages = [
            {"role": "system", "content": st.session_state.current_system_message},
            *st.session_state.chat_history,
            {"role": "user", "content": message}
        ]
        
        response = openai_client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages
        )
        return response.choices[0].message.content
    except Exception as e:
        st.error(f"Error: {e}")
        return "An error occurred while processing your request."

def search_tickers(query: str):
    if not openai_client:
        st.error("API client not initialized")
        return []
        
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
        
        response = openai_client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "You are a financial data assistant specializing in stock tickers."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3
        )
        
        content = response.choices[0].message.content.strip()
        try:
            start = content.find('[')
            end = content.rfind(']') + 1
            if start != -1 and end != -1:
                json_str = content[start:end]
                return json.loads(json_str)
            else:
                st.error("Invalid response format from LLM")
                return []
        except json.JSONDecodeError as e:
            st.error(f"Error parsing LLM response: {e}")
            return []
            
    except Exception as e:
        st.error(f"Error in ticker search: {e}")
        return []

def get_tradingview_widget(ticker: str) -> str:
    return f"""
    <!-- TradingView Widget BEGIN -->
    <div class="tradingview-widget-container">
      <div id="tradingview_chart"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
      <script type="text/javascript">
      new TradingView.widget(
      {{
      "width": 600,
      "height": 400,
      "symbol": "{ticker}",
      "interval": "D",
      "timezone": "Etc/UTC",
      "theme": "light",
      "style": "1",
      "locale": "en",
      "toolbar_bg": "#f1f3f6",
      "enable_publishing": false,
      "allow_symbol_change": true,
      "container_id": "tradingview_chart"
    }}
      );
      </script>
    </div>
    <!-- TradingView Widget END -->
    """
def handle_stock_selection(ticker):
    st.session_state.selected_ticker = ticker.upper()
    st.session_state.news_loaded = False
    st.session_state.news_data = None
    st.session_state.system_context = {
        "news": None,
        "summary": None
    }

def load_news_for_ticker(ticker):
    if not st.session_state.news_loaded:
        with st.spinner('Loading company news...'):
            try:
                news = get_company_news(st.session_state.selected_ticker, tavily_client, openai_client)
                update_system_message(news=news)
                st.session_state.news_loaded = True  # Mark news as loaded
                st.success(f"News loaded for {ticker}")
            except Exception as e:
                st.error(f"Error loading news: {str(e)}")

# Main UI
st.title("Financial Analyst")

# Sidebar
st.sidebar.title("Stock Selection")
ticker_input = st.sidebar.text_input(
    "Enter Stock Ticker:", 
    value=st.session_state.get("selected_ticker", ""),
    placeholder="e.g., AAPL, TSLA"
)

if st.sidebar.button("Load Stock Data"):
    handle_stock_selection(ticker_input)

st.sidebar.divider()

# Search functionality
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

# Display stock info
if ticker := st.session_state.get("selected_ticker"):
    display_stock_info(ticker)
    
    with st.sidebar:
        st.divider()
        st.subheader("Stock Chart")
        st.components.v1.html(get_tradingview_widget(ticker), height=400)
        
        st.subheader("SEC Filings")
        if not st.session_state.news_loaded:
            if st.button("Load News"):
                load_news_for_ticker(ticker)
        else:
            st.info("News already loaded. Interact with the chatbot or view other sections.")
        filings = get_sec_filings_for_ticker(ticker)
        if filings:
            for filing in filings:
                with st.expander(f"{filing['form']} - {filing['filing_date']}"):
                    st.text_area("Summary", filing.get('summary', 'No summary available'), height=150, disabled=True)
                    if st.button("Use This Summary", key=f"summary_{filing['accession_number']}"):
                        update_system_message(summary=filing['summary'])
                        st.success("Summary selected!")
        else:
            st.info(f"No SEC filings found for {ticker}")

        st.subheader("Recent News")
        news_articles = get_company_news(ticker, tavily_client, openai_client)
        if news_articles:
            for article in news_articles:
                with st.expander(article['title']):
                    st.write(f"*{article['publisher']}*")
                    st.write(f"[Read more]({article['link']})")
                    st.write("**Summary:**")
                    st.write(article.get('summary', article.get('content', '')[:200] + "..."))
                    update_system_message(news=news_articles)
        else:
            st.info("No recent news found")

# Chat interface
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
