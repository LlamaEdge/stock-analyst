import yfinance as yf
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, List, Optional

def get_stock_data(ticker: str) -> dict:
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

def scrape_article_content(url: str) -> Optional[str]:
    try:
        response = requests.get(url)
        if not response.ok:
            print(f'Failed to fetch article. Status code: {response.status_code}')
            return None
            
        soup = BeautifulSoup(response.text, "html.parser")
        paragraphs = soup.find_all("p")
        article_text = " ".join([paragraph.text.strip() for paragraph in paragraphs])
        
        return article_text
        
    except Exception as e:
        print(f"Error scraping article {url}: {e}")
        return None

def get_company_news(ticker: str) -> list:
    try:
        stock = yf.Ticker(ticker)
        news = stock.news
        articles = []
        
        for article in news[:5]:
            content = scrape_article_content(article.get("link", "#"))
            articles.append({
                "title": article.get("title", "No title"),
                "publisher": article.get("publisher", "Unknown"),
                "link": article.get("link", "#"),
                "content": content if content else "Content not available",
                "scraped_at": datetime.now().isoformat()
            })
                
        return articles
    except Exception as e:
        print(f"Error fetching news for {ticker}: {str(e)}")
        return []

def get_tradingview_widget(ticker: str) -> str:
    widget_html = f"""
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
    return widget_html

def get_full_company_data(ticker: str) -> Dict:
    return {
        "stock_data": get_stock_data(ticker),
        "news": get_company_news(ticker),
        "chart_widget": get_tradingview_widget(ticker)
    }

