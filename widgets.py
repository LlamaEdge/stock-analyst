import yfinance as yf

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

def get_company_news(ticker: str) -> list:
    try:
        stock = yf.Ticker(ticker)
        news = stock.news
        return [
            {
                "title": article.get("title", "No title"),
                "publisher": article.get("publisher", "Unknown"),
                "link": article.get("link", "#")
            }
            for article in news[:5]  # Return top 5 news items
        ]
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

# Example usage:
ticker = "AAPL"
stock_info = get_stock_data(ticker)
news = get_company_news(ticker)
tradingview_widget = get_tradingview_widget(ticker)

print(stock_info)
print(news)
print(tradingview_widget)

