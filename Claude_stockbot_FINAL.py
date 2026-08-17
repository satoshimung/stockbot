import os
import re
import socket
import traceback
from pathlib import Path
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from telegram import Update, InputMediaPhoto
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
import io
import asyncio
from collections import defaultdict
import requests
from bs4 import BeautifulSoup

# ─── SPX 0DTE gamma pipeline (reused, not duplicated) ───
import sys
sys.path.append(str(Path(__file__).parent / "schwab_market_pipeline"))
from gamma_dashboard import run_snapshot, latest_snapshot_or_reason

# Optional extras. These are imported defensively on purpose: if a pipeline
# file is missing or out of date, the matching command simply switches off
# and says so, instead of taking the whole bot down at startup. Core /gamma
# keeps working either way.
try:
    from gamma_dashboard import run_weekly_snapshot
    WEEKLY_AVAILABLE = True
except ImportError as _e:
    WEEKLY_AVAILABLE = False
    print(f"[startup] /gammaweek disabled -- {_e}")
    print("[startup] (update gamma_dashboard.py in schwab_market_pipeline to enable it)")

try:
    from cleanup import cleanup_expired, describe as describe_cleanup
    CLEANUP_AVAILABLE = True
except ImportError as _e:
    CLEANUP_AVAILABLE = False
    print(f"[startup] daily cleanup disabled -- {_e}")
    print("[startup] (place cleanup.py in schwab_market_pipeline to enable it)")

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot token is loaded from the .env file (never hardcode it here).
load_dotenv()
BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN not set -- add it to your .env file")

# ─── Folder Watcher Config ───
WATCH_FOLDER = Path(r"C:\dev\OptionsLiveData")
SENT_FOLDER = WATCH_FOLDER / "Sent"
TARGET_CHAT_ID = -1003026087004  # TODO: set your group chat id
pending_files = set()
last_change_time = 0

# Names already sent this session. A file whose delete failed (Windows keeps
# a lock while the writing program has it open) stays on disk, and without
# this the watcher would treat it as new on the next tick and send it again.
_already_sent = set()

# Guards against a second tick starting while a send is still in flight.
_watcher_busy = False

# Last seen size per file. A file that's still being written keeps changing
# size; treating that as activity holds the quiet window open so we never
# upload a half-written image.
_last_sizes = {}

# Telegram rejects media groups larger than this.
MEDIA_GROUP_LIMIT = 10


# Store user alerts: {chat_id: {user_id: [alerts]}}
# Each alert: {'ticker': str, 'above': float, 'below': float, 'username': str}
user_alerts = defaultdict(lambda: defaultdict(list))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a message when the command /start is issued."""
    welcome_message = """
🤖 *Stock Analysis & SPX Gamma Bot*

*📊 Stocks*
/stock TICKER — full analysis, options & 1yr chart (e.g. /stock AAPL)
/news TICKER — latest news for a stock (e.g. /news TSLA)

*🌡️ Market*
/indices — major indices, VIX, gold, bitcoin, sectors
/feargreed — CNN Fear & Greed Index

*🔔 Price Alerts*
/alert TICKER above X below Y — set an alert (e.g. /alert AAPL above 300 below 200)
/myalerts — view your active alerts
/removealert NUMBER — remove an alert by its number

*📈 SPX Gamma (0DTE)*
/gamma — cached 0DTE snapshot: Call/Put Walls, Net GEX & Put/Call on both open-interest and volume, Gamma Pin, Gamma Flip
/gamma MM/DD/YY — live pull for a specific expiration (e.g. /gamma 07/30/26)

*🗓️ SPX Gamma (weekly)*
/gammaweek — live gamma from 0DTE through Friday, aggregate plus each expiration
/gammaweek MM/DD/YY — same, starting from a given date

/help — show this message

💡 Works in groups too — add me and use the same commands.
    """
    await update.message.reply_text(welcome_message, parse_mode='Markdown')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a message when the command /help is issued."""
    await start(update, context)

# ─── Moon ───, new command to get latest news for a stock
async def cmd_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message

    if not context.args:
        await message.reply_text("Usage: `/news JPM`", parse_mode="Markdown")
        return

    sym = context.args[0].upper()

    try:
        ticker = yf.Ticker(sym)
        news_items = ticker.news
    except Exception as e:
        print("News fetch error:", e)
        await message.reply_text(f"❌ Failed to fetch news for `{sym}`", parse_mode="Markdown")
        return

    if not news_items:
        await message.reply_text(f"No news found for `{sym}`.", parse_mode="Markdown")
        return

    lines = [f"*Latest news for `{sym}`:*"]

    for item in news_items[:5]:  # top 5
        if not item:
            continue  # Skip None items
            
        content = item.get('content', {})
        if not content:
            continue  # Skip items without content
            
        title = content.get("title", "No title")
        provider_data = content.get("provider") or {}
        publisher = provider_data.get("displayName", "Unknown") if isinstance(provider_data, dict) else "Unknown"
        link_data = content.get("clickThroughUrl") or {}
        link = link_data.get("url", "") if isinstance(link_data, dict) else ""
        ts_str = content.get("pubDate") or content.get("displayTime")

        if ts_str:
            try:
                dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                time_str = dt.strftime("%Y-%m-%d %H:%M UTC")
            except:
                time_str = "Unknown time"
        else:
            time_str = "Unknown time"

        if link:
            lines.append(
                f"\n• [{title}]({link})\n"
                f"  {publisher} – {time_str}"
            )
        else:
            lines.append(
                f"\n• {title}\n"
                f"  {publisher} – {time_str}"
            )

    await message.reply_text(
        "\n".join(lines),
        parse_mode="Markdown",
        disable_web_page_preview=True,
    )

# ─── Moon ───, new function to get ownership data
async def get_ownership_data(symbol: str) -> dict | None:
    """
    Fetch institutional & insider ownership % from yfinance major_holders.
    Handles both recent formats (index-based or Breakdown/Value columns).
    """
    loop = asyncio.get_running_loop()
    try:
        ticker = await loop.run_in_executor(None, yf.Ticker, symbol)
        holders = await loop.run_in_executor(None, lambda: ticker.major_holders)

        if holders is None or (isinstance(holders, pd.DataFrame) and holders.empty):
            return None

        data = {}

        if isinstance(holders, pd.DataFrame):
            # Format A: index-based keys (like your AAPL log)
            if 'insidersPercentHeld' in holders.index:
                data['insider'] = holders.loc['insidersPercentHeld'].iloc[0] * 100
            if 'institutionsPercentHeld' in holders.index:
                data['institutional'] = holders.loc['institutionsPercentHeld'].iloc[0] * 100

            # Format B: classic two-column Breakdown | Value (like OKLO example)
            if len(holders.columns) >= 2:
                val_col = holders.columns[0]
                label_col = holders.columns[1]
                for _, row in holders.iterrows():
                    val_str = str(row[val_col]).strip()
                    label = str(row[label_col]).lower()
                    try:
                        pct = float(val_str.replace('%', ''))
                        if 'insider' in label or 'all insider' in label:
                            data['insider'] = pct
                        if 'institutions' in label or 'float held by institutions' in label:
                            data['institutional'] = pct
                    except ValueError:
                        continue

        return data if data else None

    except Exception as e:
        print(f"Ownership fetch failed for {symbol}: {type(e).__name__}: {str(e)}")
        return None
# ─── End Moon ───

async def indices(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get major market indices snapshot"""
    if not update.message:
        return
    
    chat_type = update.effective_chat.type
    user = update.effective_user
    logger.info(f"Indices command from {user.username or user.first_name} in {chat_type} chat")
    
    status_message = await update.message.reply_text("📊 Fetching market data... Please wait.")
    
    try:
        # Define the tickers to fetch
        tickers = {
            'S&P 500': '^GSPC',
            # 'SPY': 'SPY',
            'Nasdaq': '^IXIC',
            'Russell 2000': '^RUT',
            'VIX': '^VIX', # CBOE Volatility Index
            'Gold': 'GC=F',
            'Bitcoin': 'BTC-USD',
            'XLE': 'XLE', # Energy Select Sector SPDR Fund
            'XLU': 'XLU', # Utilities Select Sector SPDR Fund
            'XLV': 'XLV' # Health Care Select Sector SPDR Fund
        }
        
        market_data = []
        
        # Fetch data for each ticker
        for name, symbol in tickers.items():
            try:
                ticker = yf.Ticker(symbol)
                info = ticker.info
                hist = ticker.history(period='2d')
                
                if not hist.empty and len(hist) >= 2:
                    current_price = hist['Close'].iloc[-1]
                    previous_price = hist['Close'].iloc[-2]
                    change_percent = ((current_price - previous_price) / previous_price) * 100
                else:
                    # Fallback to info if history doesn't work
                    current_price = info.get('regularMarketPrice') or info.get('currentPrice', 0)
                    previous_close = info.get('previousClose', current_price)
                    change_percent = ((current_price - previous_close) / previous_close) * 100 if previous_close else 0
                
                # Format the change with + or - sign
                change_sign = '+' if change_percent >= 0 else ''
                market_data.append(f"{name}: {current_price:,.2f} ({change_sign}{change_percent:.2f}%)")
                
            except Exception as e:
                logger.error(f"Error fetching {name} ({symbol}): {e}")
                market_data.append(f"{name}: Unable to fetch data")
        
        # Build the message
        message = "📈 *Market Snapshot*\n\n"
        message += "\n".join(market_data)
        
        # Delete the status message
        await status_message.delete()
        
        # Send the market snapshot
        await update.message.reply_text(message, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error fetching market indices: {e}")
        
        try:
            await status_message.delete()
        except:
            pass
        
        await update.message.reply_text(
            f"❌ Error fetching market data. Please try again later.\n\n"
            f"Error details: {str(e)}"
        )

async def fear_greed_index(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Get CNN Fear & Greed Index"""
    if not update.message:
        return
    
    chat_type = update.effective_chat.type
    user = update.effective_user
    logger.info(f"Fear & Greed command from {user.username or user.first_name} in {chat_type} chat")
    
    status_message = await update.message.reply_text("😨😊 Fetching Fear & Greed Index... Please wait.")
    
    try:
        # Try multiple methods to get Fear & Greed data
        
        # Method 1: Try the CNN API
        url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.cnn.com/',
            'Origin': 'https://www.cnn.com',
            'Connection': 'keep-alive'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            
            # Debug: Log the structure to understand it better
            logger.info(f"Fear & Greed data structure: {data.keys()}")
            
            # Try to extract the score - handle different possible structures
            current_score = None
            current_rating = None
            previous_close = None
            previous_week = None
            previous_month = None
            
            # Check different possible data structures
            if 'fear_and_greed' in data:
                fg_data = data['fear_and_greed']
                current_score = fg_data.get('score')
                current_rating = fg_data.get('rating')
            elif 'data' in data:
                # Alternative structure
                current_score = data['data'].get('score')
                current_rating = data['data'].get('rating')
            
            # Get historical data if available
            if 'fear_and_greed_historical' in data and 'data' in data['fear_and_greed_historical']:
                hist_data = data['fear_and_greed_historical']['data']
                if len(hist_data) > 0:
                    previous_close = hist_data[0].get('score')
                if len(hist_data) > 4:
                    previous_week = hist_data[4].get('score')
                if len(hist_data) > 19:
                    previous_month = hist_data[19].get('score')
            
            if current_score is not None:
                # Determine emoji and rating if rating is missing
                if not current_rating:
                    if current_score <= 25:
                        current_rating = 'Extreme Fear'
                    elif current_score <= 45:
                        current_rating = 'Fear'
                    elif current_score <= 55:
                        current_rating = 'Neutral'
                    elif current_score <= 75:
                        current_rating = 'Greed'
                    else:
                        current_rating = 'Extreme Greed'
                
                emoji_map = {
                    'Extreme Fear': '😱',
                    'Fear': '😨',
                    'Neutral': '😐',
                    'Greed': '😊',
                    'Extreme Greed': '🤑'
                }
                emoji = emoji_map.get(current_rating, '📊')
                
                # Build message
                message = f"""
                {emoji} *CNN Fear & Greed Index*

                *Current Status:* {current_rating}
                *Score:* {current_score}/100

                """
                
                if previous_close or previous_week or previous_month:
                    message += "📊 *Historical Comparison:*\n"
                    if previous_close:
                        message += f"• Previous Close: {previous_close}\n"
                    if previous_week:
                        message += f"• 1 Week Ago: {previous_week}\n"
                    if previous_month:
                        message += f"• 1 Month Ago: {previous_month}\n"
                    message += "\n"
                
                message += """📈 *Score Guide:*
                • 0-25: Extreme Fear 😱
                • 25-45: Fear 😨
                • 45-55: Neutral 😐
                • 55-75: Greed 😊
                • 75-100: Extreme Greed 🤑

                _The Fear & Greed Index measures market sentiment based on 7 indicators including market momentum, stock price strength, volatility, safe haven demand, and more._
                """
                
                await status_message.delete()
                await update.message.reply_text(message, parse_mode='Markdown')
                return
        
        # If we got here, the API method didn't work - use alternative
        raise Exception("CNN API structure changed or unavailable")
        
    except Exception as e:
        logger.error(f"Error fetching Fear & Greed Index: {e}")
        
        # Try alternative API source
        try:
            # Alternative: Use a third-party API that tracks CNN's index
            alt_url = "https://fear-and-greed-index.p.rapidapi.com/v1/fgi"
            
            # Note: This would require RapidAPI key, so we'll use a simpler fallback
            # For now, provide manual calculation based on VIX
            
            vix_ticker = yf.Ticker('^VIX')
            vix_price = vix_ticker.info.get('regularMarketPrice', 0)
            
            # Rough estimate: VIX inversely correlates with greed
            # VIX < 15 = Greed, VIX 15-20 = Neutral, VIX > 20 = Fear
            if vix_price < 15:
                sentiment = "Greed 😊"
                explanation = f"VIX is low ({vix_price:.2f}), suggesting low fear in markets"
            elif vix_price < 20:
                sentiment = "Neutral 😐"
                explanation = f"VIX is moderate ({vix_price:.2f}), suggesting neutral sentiment"
            else:
                sentiment = "Fear 😨"
                explanation = f"VIX is elevated ({vix_price:.2f}), suggesting increased fear"
            
            message = f"""
📊 *Market Sentiment Estimate*

⚠️ _CNN Fear & Greed Index API is currently unavailable_

*Estimated Sentiment:* {sentiment}
*Based on VIX:* {vix_price:.2f}

{explanation}

📈 *VIX Guide:*
• Below 15: Low fear (Greed)
• 15-20: Normal (Neutral)
• Above 20: Elevated fear
• Above 30: High fear

You can view the official CNN Fear & Greed Index at:
https://www.cnn.com/markets/fear-and-greed
"""
            
            await status_message.delete()
            await update.message.reply_text(message, parse_mode='Markdown')
            
        except Exception as e2:
            logger.error(f"Alternative method also failed: {e2}")
            
            try:
                await status_message.delete()
            except:
                pass
            
            await update.message.reply_text(
                f"❌ Unable to fetch Fear & Greed data at this time.\n\n"
                f"You can view it directly at:\nhttps://www.cnn.com/markets/fear-and-greed\n\n"
                f"Error: {str(e)}"
            )

# ─── Moon ───, version with volume column, between 30-50 days
def get_options_data(ticker, symbol):
    """Get top 5 options (between 30-50 days) by open interest"""
    try:
        from datetime import datetime, timedelta
        import pandas as pd
        
        expirations = ticker.options
        if not expirations:
            return None, None
        
        # Filter for expirations between 30 and 50 days out
        today = datetime.now().date()
        start_date = today + timedelta(days=30)  # 30 days from now
        end_date = today + timedelta(days=50)    # 50 days from now
        
        filtered_expirations = [
            exp for exp in expirations 
            if start_date < datetime.strptime(exp, '%Y-%m-%d').date() <= end_date
        ]
        
        if not filtered_expirations:
            logger.warning(f"No options found between 30-50 days for {symbol}")
            return None, None
        
        all_calls = []
        all_puts = []
        
        # Collect options from all filtered expirations
        for exp_date in filtered_expirations:
            try:
                opt_chain = ticker.option_chain(exp_date)
                
                # Add expiration date to each option
                calls = opt_chain.calls.copy()
                calls['expiration'] = exp_date
                puts = opt_chain.puts.copy()
                puts['expiration'] = exp_date
                
                all_calls.append(calls)
                all_puts.append(puts)
            except Exception as e:
                logger.error(f"Error fetching options for {exp_date}: {e}")
                continue
        
        if not all_calls or not all_puts:
            return None, None
        
        # Combine all calls and puts
        combined_calls = pd.concat(all_calls, ignore_index=True)
        combined_puts = pd.concat(all_puts, ignore_index=True)
        
        # Get top 5 by Open Interest (include volume column)
        top_calls = combined_calls.nlargest(5, 'openInterest')[['strike', 'openInterest', 'volume', 'expiration']]
        top_puts = combined_puts.nlargest(5, 'openInterest')[['strike', 'openInterest', 'volume', 'expiration']]
        
        return top_calls, top_puts
        
    except Exception as e:
        logger.error(f"Error getting options data for {symbol}: {e}")
        return None, None
   
# ─── Moon ───, advanced chart with SMA 50/200
def create_advanced_chart(symbol: str) -> io.BytesIO | None:
    """Create advanced chart with SMA 50/200, regime shading, and volume panel."""
    
    # Fetch MORE than 12 months to ensure SMA 200 is calculated for the entire 1-year view
    hist = yf.Ticker(symbol).history(period="2y", interval="1d")

    if hist.empty or len(hist) < 400:
        return None

    # ===== Indicators =====
    hist["SMA50"] = hist["Close"].rolling(50).mean()
    hist["SMA200"] = hist["Close"].rolling(200).mean()

    # Now slice to show only the last 12 months for plotting
    one_year_ago = hist.index[-1] - pd.Timedelta(days=365)
    hist_display = hist[hist.index >= one_year_ago].copy()

    # Trend regime
    hist_display["Signal"] = 0
    hist_display.loc[hist_display["SMA50"] > hist_display["SMA200"], "Signal"] = 1
    hist_display["Cross"] = hist_display["Signal"].diff()

    golden = hist_display[hist_display["Cross"] == 1]
    death = hist_display[hist_display["Cross"] == -1]

    # ===== Create subplots: Price + Volume =====
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), 
                                     gridspec_kw={'height_ratios': [3, 1]},
                                     sharex=True)
    
    # ===== TOP PANEL: Price Chart =====
    ax1.set_facecolor('white')

    # Plot lines
    ax1.plot(hist_display.index, hist_display["Close"], label="Close", color='#1f77b4', linewidth=1.5)
    ax1.plot(hist_display.index, hist_display["SMA50"], label="SMA 50", color='#ff7f0e', linewidth=1.5)
    ax1.plot(hist_display.index, hist_display["SMA200"], label="SMA 200", color='#2ca02c', linewidth=1.5)

    # Background shading by regime
    regime_start = hist_display.index[0]
    current_signal = hist_display["Signal"].iloc[0]
    
    first_valid_idx = hist_display["Signal"].first_valid_index()
    if first_valid_idx is not None:
        idx_pos = hist_display.index.get_loc(first_valid_idx)
        current_signal = hist_display["Signal"].iloc[idx_pos]
        regime_start = hist_display.index[idx_pos]

    for i in range(1, len(hist_display)):
        if pd.isna(hist_display["SMA50"].iloc[i]) or pd.isna(hist_display["SMA200"].iloc[i]):
            continue
            
        if hist_display["Signal"].iloc[i] != current_signal:
            ax1.axvspan(
                regime_start,
                hist_display.index[i],
                color="green" if current_signal == 1 else "red",
                alpha=0.1,
                zorder=0
            )
            regime_start = hist_display.index[i]
            current_signal = hist_display["Signal"].iloc[i]

    # Final regime
    ax1.axvspan(
        regime_start,
        hist_display.index[-1],
        color="green" if current_signal == 1 else "red",
        alpha=0.1,
        zorder=0
    )

    # Golden / Death Cross markers
    if not golden.empty:
        ax1.scatter(
            golden.index,
            golden["Close"],
            marker="^",
            color="green",
            s=120,
            label="Golden Cross",
            zorder=5,
            edgecolors='darkgreen',
            linewidths=1.5
        )

    if not death.empty:
        ax1.scatter(
            death.index,
            death["Close"],
            marker="v",
            color="red",
            s=120,
            label="Death Cross",
            zorder=5,
            edgecolors='darkred',
            linewidths=1.5
        )

    # Latest price annotation
    last_price = hist_display["Close"].iloc[-1]
    ax1.annotate(
        f"{last_price:.2f}",
        xy=(hist_display.index[-1], last_price),
        xytext=(10, 0),
        textcoords="offset points",
        bbox=dict(boxstyle="round", fc="white", ec="black", linewidth=0.8),
        va="center",
        fontsize=10,
        fontweight='bold'
    )

    # Add stats box (top right corner)
    stats_text = f"Current: ${last_price:.2f}\n"
    stats_text += f"52W High: ${hist_display['Close'].max():.2f}\n"
    stats_text += f"52W Low: ${hist_display['Close'].min():.2f}\n"
    stats_text += f"YTD: {((last_price / hist_display['Close'].iloc[0] - 1) * 100):.1f}%"

    ax1.text(0.98, 0.02, stats_text, transform=ax1.transAxes,
            fontsize=9, verticalalignment='bottom', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    ax1.set_title(f"{symbol} - Last 12 Months (SMA 50/200)", fontsize=14, fontweight='bold')
    ax1.set_ylabel("Price", fontsize=11)
    ax1.legend(loc='upper left', frameon=True, fancybox=True, shadow=False, fontsize=9)
    ax1.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
    ax1.spines['top'].set_visible(True)
    ax1.spines['right'].set_visible(True)
    ax1.spines['left'].set_linewidth(0.8)
    ax1.spines['bottom'].set_linewidth(0.8)

    # ===== BOTTOM PANEL: Volume =====
    ax2.set_facecolor('white')

    # Color volume bars: green if close > open, red otherwise
    colors = ['green' if hist_display['Close'].iloc[i] >= hist_display['Open'].iloc[i] 
            else 'red' for i in range(len(hist_display))]

    ax2.bar(hist_display.index, hist_display['Volume'], color=colors, alpha=0.6, width=0.8)

    # Calculate average volume and bounds
    avg_volume = hist_display['Volume'].mean()
    upper_bound = hist_display['Volume'].quantile(0.75)  # 75th percentile
    lower_bound = hist_display['Volume'].quantile(0.25)  # 25th percentile

    # Add horizontal lines with darker colors
    ax2.axhline(y=avg_volume, color='blue', linestyle='--', linewidth=2, label='Average Volume', alpha=0.8)
    ax2.axhline(y=upper_bound, color='darkorange', linestyle=':', linewidth=2, label='Upper Bound', alpha=0.8)
    ax2.axhline(y=lower_bound, color='darkviolet', linestyle=':', linewidth=2, label='Lower Bound', alpha=0.8)

    # Detect high volume spikes (above upper bound)
    volume_spikes_high = hist_display[hist_display['Volume'] > upper_bound * 1.5]  # Significant spikes

    # Mark high volume spikes with upward triangles
    if not volume_spikes_high.empty:
        ax2.scatter(
            volume_spikes_high.index,
            volume_spikes_high['Volume'],
            marker='^',
            color='gold',
            s=100,
            label='High Volume Spike',
            zorder=5,
            edgecolors='darkorange',
            linewidths=1.5
        )

    # Compare today's volume vs average
    today_volume = hist_display['Volume'].iloc[-1]
    volume_ratio = (today_volume / avg_volume - 1) * 100
    # comparison_text = f"Today: {today_volume/1e6:.1f}M | Avg: {avg_volume/1e6:.1f}M ({volume_ratio:+.1f}%)"
    comparison_text = f"Today: {today_volume/1e6:.1f}M | Avg: {avg_volume/1e6:.1f}M ({volume_ratio:+.1f}%)\n(Y-axis in millions, rounded)"

    # Add comparison text box to the RIGHT (corrected)
    ax2.text(0.98, 0.95, comparison_text, transform=ax2.transAxes,
            fontsize=9, verticalalignment='top', horizontalalignment='right',
            bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8, edgecolor='gray'))

    ax2.set_ylabel("Volume", fontsize=11)
    ax2.set_xlabel("Date", fontsize=11)
    # Move legend to upper LEFT
    ax2.legend(loc='upper left', fontsize=8, frameon=True, fancybox=True)
    ax2.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)
    ax2.spines['top'].set_visible(True)
    ax2.spines['right'].set_visible(True)
    ax2.spines['left'].set_linewidth(0.8)
    ax2.spines['bottom'].set_linewidth(0.8)

    # Format volume y-axis (in millions)
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1e6:.0f}M'))

    # ===== Save to BytesIO buffer instead of file =====
    buf = io.BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format="png", dpi=100, facecolor='white', edgecolor='none')
    buf.seek(0)
    plt.close()

    return buf

# ─── Moon ───, new function to get weekly options data
def get_weekly_options_data(ticker, symbol):
    """Get top 5 weekly options (expiring within 7 days) by open interest"""
    try:
        from datetime import datetime, timedelta
        import pandas as pd
        
        expirations = ticker.options
        if not expirations:
            return None, None
        
        # Filter for weekly expirations (next 7 days)
        today = datetime.now().date()
        next_week = today + timedelta(days=7)
        
        weekly_expirations = [
            exp for exp in expirations 
            if today < datetime.strptime(exp, '%Y-%m-%d').date() <= next_week
        ]
        
        if not weekly_expirations:
            return None, None
        
        all_calls = []
        all_puts = []
        
        # Collect options from all weekly expirations
        for exp_date in weekly_expirations:
            try:
                opt_chain = ticker.option_chain(exp_date)
                
                # Add expiration date to each option
                calls = opt_chain.calls.copy()
                calls['expiration'] = exp_date
                puts = opt_chain.puts.copy()
                puts['expiration'] = exp_date
                
                all_calls.append(calls)
                all_puts.append(puts)
            except Exception as e:
                logger.error(f"Error fetching options for {exp_date}: {e}")
                continue
        
        if not all_calls or not all_puts:
            return None, None
        
        # Combine all calls and puts
        combined_calls = pd.concat(all_calls, ignore_index=True)
        combined_puts = pd.concat(all_puts, ignore_index=True)
        
        # Get top 5 by Open Interest (include volume column)
        top_calls = combined_calls.nlargest(5, 'openInterest')[['strike', 'openInterest', 'volume', 'expiration']]
        top_puts = combined_puts.nlargest(5, 'openInterest')[['strike', 'openInterest', 'volume', 'expiration']]
        
        return top_calls, top_puts
        
    except Exception as e:
        logger.error(f"Error getting weekly options data for {symbol}: {e}")
        return None, None

async def stock_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Provide comprehensive stock analysis"""
    # Check if message exists (works for both private and group chats)
    if not update.message:
        return
    
    # Get chat type to log where the command came from
    chat_type = update.effective_chat.type
    chat_id = update.effective_chat.id
    user = update.effective_user
    
    logger.info(f"Stock command from {user.username or user.first_name} in {chat_type} chat (ID: {chat_id})")
    
    if not context.args:
        await update.message.reply_text("Please provide a stock ticker. Example: /stock AAPL")
        return
    
    stock_symbol = context.args[0].upper()
    
    # Send initial message
    status_message = await update.message.reply_text(f"🔍 Analyzing {stock_symbol}... Please wait.")
    
    try:
        # Get stock data
        ticker = yf.Ticker(stock_symbol)
        info = ticker.info
        
        # Get current price and daily change
        current_price = info.get('currentPrice') or info.get('regularMarketPrice', 'N/A')
        previous_close = info.get('previousClose', 0)
        
        if current_price != 'N/A' and previous_close:
            change = current_price - previous_close
            change_percent = (change / previous_close) * 100
        else:
            change = 'N/A'
            change_percent = 'N/A'
        
        # Calculate Market Maker's expected move using ATM straddle
        mm_expected_move = 'N/A'
        mm_expected_move_pct = 'N/A'
        try:
            expirations = ticker.options
            if expirations and current_price != 'N/A':
                # Get the nearest expiration (typically weekly or monthly)
                nearest_exp = expirations[0]
                opt_chain = ticker.option_chain(nearest_exp)
                
                # Find ATM (at-the-money) strike closest to current price
                calls = opt_chain.calls
                puts = opt_chain.puts
                
                if not calls.empty and not puts.empty:
                    # Find the strike closest to current price
                    calls['strike_diff'] = abs(calls['strike'] - current_price)
                    atm_call = calls.loc[calls['strike_diff'].idxmin()]
                    
                    puts['strike_diff'] = abs(puts['strike'] - current_price)
                    atm_put = puts.loc[puts['strike_diff'].idxmin()]
                    
                    # ATM straddle = call premium + put premium
                    call_price = atm_call['lastPrice']
                    put_price = atm_put['lastPrice']
                    
                    if call_price > 0 and put_price > 0:
                        straddle_price = call_price + put_price
                        mm_expected_move = straddle_price
                        mm_expected_move_pct = (straddle_price / current_price) * 100
        except Exception as e:
            logger.error(f"Error calculating MM expected move: {e}")
        
        # Basic info
        market_cap = info.get('marketCap', 'N/A')
        if market_cap != 'N/A':
            market_cap = f"${market_cap/1e9:.2f}B"
        
        # Revenue (TTM)
        revenue = info.get('totalRevenue', 'N/A')
        if revenue != 'N/A':
            revenue = f"${revenue/1e9:.2f}B"
        
        # Balance sheet
        cash = info.get('totalCash', 'N/A')
        if cash != 'N/A':
            cash = f"${cash/1e9:.2f}B"
        
        debt = info.get('totalDebt', 'N/A')
        if debt != 'N/A':
            debt = f"${debt/1e9:.2f}B"
        
        # Sales growth
        revenue_growth = info.get('revenueGrowth', 'N/A')
        if revenue_growth != 'N/A':
            revenue_growth = f"{revenue_growth*100:.2f}%"

        # ─── Moon ───, Price-to-Sales ratio (TTM)
        ps_ratio = info.get('priceToSalesTrailing12Months', 'N/A')
        if ps_ratio == 'N/A':
            # Fallback: calculate manually if data available
            total_revenue = info.get('totalRevenue')
            if market_cap != 'N/A' and total_revenue and total_revenue > 0:
                # Convert market cap back to number (it's formatted as string)
                try:
                    mc_value = float(market_cap.replace('$', '').replace('B', '')) * 1e9
                    ps_ratio = mc_value / total_revenue
                    ps_ratio = f"{ps_ratio:.2f}"
                except:
                    ps_ratio = 'N/A'
        else:
            ps_ratio = f"{ps_ratio:.2f}"

        # ─── Moon ───,PEG ratio (Price/Earnings to Growth)
        peg_ratio = info.get('pegRatio', 'N/A')
        if peg_ratio == 'N/A':
            # Fallback: calculate manually if data available
            # PEG = (P/E ratio) / (Earnings growth rate)
            pe_ratio_calc = info.get('trailingPE')
            earnings_growth = info.get('earningsGrowth')
            
            if pe_ratio_calc and earnings_growth and earnings_growth > 0:
                try:
                    peg_ratio = pe_ratio_calc / (earnings_growth * 100)
                    peg_ratio = f"{peg_ratio:.2f}"
                except:
                    peg_ratio = 'N/A'
        else:
            peg_ratio = f"{peg_ratio:.2f}"

        # ─── Moon ───, Fetch ownership 
        ownership = await get_ownership_data(stock_symbol)
        inst_own = "N/A"
        insider_own = "N/A"
        if ownership:
            inst_own    = f"{ownership.get('institutional', 0.0):.2f}%"
            insider_own = f"{ownership.get('insider',      0.0):.2f}%"
        
        # Analyst targets
        target_price = info.get('targetMeanPrice', 'N/A')
        if target_price != 'N/A':
            target_price = f"${target_price:.2f}"
        
        # Fair value estimation (using multiple approaches)
        pe_ratio = info.get('trailingPE', None)
        industry_pe = info.get('industryPE', 15)  # Default industry PE
        earnings_per_share = info.get('trailingEps', None)
        
        fair_value = 'N/A'
        if pe_ratio and earnings_per_share and industry_pe:
            fair_value_estimate = earnings_per_share * industry_pe
            fair_value = f"${fair_value_estimate:.2f}"
        
        # Get options data - both monthly (>30 days) and weekly (<7 days)
        top_calls, top_puts = get_options_data(ticker, stock_symbol)
        top_calls_weekly, top_puts_weekly = get_weekly_options_data(ticker, stock_symbol)
        
        # Get news - try multiple methods
        news = []
        try:
            # Method 1: Try yfinance news API
            if hasattr(ticker, 'news') and ticker.news:
                news = ticker.news[:5]
                logger.info(f"Got {len(news)} news items from yfinance API")
            
            # Method 2: If no news, try alternative yfinance endpoint
            if not news:
                try:
                    # Force refresh and try again
                    ticker = yf.Ticker(stock_symbol)
                    hist = ticker.history(period='1d')  # This sometimes triggers news load
                    if hasattr(ticker, 'news') and ticker.news:
                        news = ticker.news[:5]
                        logger.info(f"Got {len(news)} news items after refresh")
                except:
                    pass
            
            # Method 3: Scrape from Yahoo Finance news page
            if not news:
                logger.info(f"Attempting to scrape news for {stock_symbol}")
                news_url = f"https://finance.yahoo.com/quote/{stock_symbol}/news"
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                }
                
                try:
                    response = requests.get(news_url, headers=headers, timeout=10)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        
                        # Try different selectors for Yahoo Finance news
                        # Method 3a: Look for news stream items
                        news_items = soup.find_all('h3', class_='Mb(5px)')
                        
                        if not news_items:
                            # Method 3b: Try alternative class names
                            news_items = soup.find_all('a', attrs={'data-test': 'quoteNews-click'})
                        
                        if not news_items:
                            # Method 3c: Look for any h3 with links
                            news_items = soup.find_all('h3')
                        
                        for item in news_items[:5]:
                            link_tag = item.find('a') if item.name != 'a' else item
                            if link_tag and link_tag.get('href'):
                                title = link_tag.get_text(strip=True)
                                link = link_tag.get('href')
                                
                                # Clean up title
                                if title and len(title) > 10:
                                    # Make sure link is absolute
                                    if link.startswith('/'):
                                        link = f"https://finance.yahoo.com{link}"
                                    elif not link.startswith('http'):
                                        link = f"https://finance.yahoo.com/news/{link}"
                                    
                                    news.append({'title': title, 'link': link})
                        
                        logger.info(f"Scraped {len(news)} news items from Yahoo Finance")
                except Exception as scrape_error:
                    logger.error(f"Error scraping Yahoo Finance: {scrape_error}")
            
            # Method 4: If still no news, try Google News search
            if not news:
                logger.info(f"Attempting Google News search for {stock_symbol}")
                try:
                    # Get company name for better search
                    company_name = info.get('longName') or info.get('shortName') or stock_symbol
                    search_query = f"{company_name} stock news"
                    google_news_url = f"https://news.google.com/search?q={search_query.replace(' ', '+')}"
                    
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    }
                    
                    response = requests.get(google_news_url, headers=headers, timeout=10)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        articles = soup.find_all('article', limit=5)
                        
                        for article in articles:
                            title_tag = article.find('a', class_='gPFEn')
                            if title_tag:
                                title = title_tag.get_text(strip=True)
                                link = title_tag.get('href')
                                if link and link.startswith('./'):
                                    link = f"https://news.google.com{link[1:]}"
                                
                                if title and link:
                                    news.append({'title': title, 'link': link})
                        
                        logger.info(f"Found {len(news)} news items from Google News")
                except Exception as google_error:
                    logger.error(f"Error fetching from Google News: {google_error}")
            
        except Exception as e:
            logger.error(f"Error in news fetching process: {e}")
        
        # Build the response message
        change_sign = '+' if change != 'N/A' and change >= 0 else ''
        price_display = f"${current_price:,.2f} ({change_sign}{change_percent:.2f}%)" if change != 'N/A' else f"${current_price if current_price != 'N/A' else 'N/A'}"
        
        message = f"""
📊 *{stock_symbol} - Stock Analysis*

💵 *Price Information:*
- Current Price: {price_display}
"""
        
        # Add MM expected move if available
        if mm_expected_move != 'N/A':
            message += f"• Market Maker's Expected Move: ±${mm_expected_move:.2f} (±{mm_expected_move_pct:.2f}%)\n"
        
        # ─── Moon ───, updated ownership data, P/S ratio and PEG ratio
        message += f"""
💰 *Company Financials:*
- Market Cap: {market_cap}
- TTM Revenue: {revenue}
- Cash: {cash}
- Total Debt: {debt}
- Revenue Growth: {revenue_growth}
- P/S (TTM): {ps_ratio} 
- PEG Ratio: {peg_ratio}
- Institutional Ownership: {inst_own}
- Insider Ownership: {insider_own}

📈 *Analyst Data:*
- Consensus Price Target: {target_price}
- Estimated Fair Value: {fair_value}
⚠️ _Not financial advice!_

"""
        
        # Add WEEKLY options data first
        if top_calls_weekly is not None and not top_calls_weekly.empty:
            message += "📞 *Top 5 Call Options (by OI, weekly):*\n"
            for idx, row in top_calls_weekly.iterrows():
                volume = int(row['volume']) if pd.notna(row['volume']) else 0
                message += f"  • Strike: ${row['strike']:.2f} | OI: {int(row['openInterest']):,} | Vol: {volume:,} | Exp: {row['expiration']}\n"
        else:
            message += "📞 *Call Options (weekly):* No data available\n"
        
        message += "\n"
        
        if top_puts_weekly is not None and not top_puts_weekly.empty:
            message += "📉 *Top 5 Put Options (by OI, weekly):*\n"
            for idx, row in top_puts_weekly.iterrows():
                volume = int(row['volume']) if pd.notna(row['volume']) else 0
                message += f"  • Strike: ${row['strike']:.2f} | OI: {int(row['openInterest']):,} | Vol: {volume:,} | Exp: {row['expiration']}\n"
        else:
            message += "📉 *Put Options (weekly):* No data available\n"
        
        message += "\n"
        
        # Add MONTHLY options data (30-50 days)
        if top_calls is not None and not top_calls.empty:
            message += "📞 *Top 5 Call Options (by OI, 30-50 days):*\n"
            for idx, row in top_calls.iterrows():
                volume = int(row['volume']) if pd.notna(row['volume']) else 0
                message += f"  • Strike: ${row['strike']:.2f} | OI: {int(row['openInterest']):,} | Vol: {volume:,} | Exp: {row['expiration']}\n"
        else:
            message += "📞 *Call Options (30-50 days):* No data available\n"
        
        message += "\n"
        
        if top_puts is not None and not top_puts.empty:
            message += "📉 *Top 5 Put Options (by OI, 30-50 days):*\n"
            for idx, row in top_puts.iterrows():
                volume = int(row['volume']) if pd.notna(row['volume']) else 0
                message += f"  • Strike: ${row['strike']:.2f} | OI: {int(row['openInterest']):,} | Vol: {volume:,} | Exp: {row['expiration']}\n"
        else:
            message += "📉 *Put Options (30-50 days):* No data available\n"
            
        
        
        # Add news links
        # if news:
        #     message += "\n📰 *Latest News:*\n"
        #     for article in news:
        #         title = article.get('title', 'No title')
        #         link = article.get('link', '')
        #         message += f"  • [{title}]({link})\n"
        # else:
        #     message += "\n📰 *Latest News:* No recent news available\n"
        
        # Delete the "analyzing..." message
        await status_message.delete()
        
        # Send the analysis message
        await update.message.reply_text(message, parse_mode='Markdown', disable_web_page_preview=True)
        
        # Create and send chart
        chart_buffer = create_advanced_chart(stock_symbol)
        if chart_buffer:
            await update.message.reply_photo(photo=chart_buffer, caption=f"📈 {stock_symbol} - 1 Year Price Chart")
        else:
            await update.message.reply_text("⚠️ Unable to generate price chart.")
        
    except Exception as e:
        logger.error(f"Error analyzing stock {stock_symbol}: {e}")
        
        # Delete the "analyzing..." message
        try:
            await status_message.delete()
        except:
            pass
        
        await update.message.reply_text(
            f"❌ Error analyzing {stock_symbol}. Please check the ticker symbol and try again.\n\n"
            f"Error details: {str(e)}"
        )

async def set_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set a price alert for a stock"""
    if not update.message:
        return
    
    user = update.effective_user
    chat_id = update.effective_chat.id
    user_id = user.id
    username = user.username or user.first_name
    
    # Parse the command: /alert TICKER above 300 below 200
    if len(context.args) < 3:
        await update.message.reply_text(
            "❌ Invalid format!\n\n"
            "Usage: `/alert TICKER above X below Y`\n"
            "Examples:\n"
            "• `/alert AAPL above 300` - Alert when above 300\n"
            "• `/alert TSLA below 200` - Alert when below 200\n"
            "• `/alert NVDA above 500 below 400` - Alert for both",
            parse_mode='Markdown'
        )
        return
    
    ticker = context.args[0].upper()
    above_price = None
    below_price = None
    
    # Parse the price conditions
    try:
        i = 1
        while i < len(context.args):
            if context.args[i].lower() == 'above' and i + 1 < len(context.args):
                above_price = float(context.args[i + 1])
                i += 2
            elif context.args[i].lower() == 'below' and i + 1 < len(context.args):
                below_price = float(context.args[i + 1])
                i += 2
            else:
                i += 1
        
        if above_price is None and below_price is None:
            raise ValueError("No valid price conditions found")
        
        # Verify the ticker exists
        test_ticker = yf.Ticker(ticker)
        current_price = test_ticker.info.get('currentPrice') or test_ticker.info.get('regularMarketPrice')
        
        if not current_price:
            await update.message.reply_text(f"❌ Unable to fetch data for {ticker}. Please check the ticker symbol.")
            return
        
        # Create the alert
        alert = {
            'ticker': ticker,
            'above': above_price,
            'below': below_price,
            'username': username,
            'user_id': user_id,
            'created_at': datetime.now()
        }
        
        user_alerts[chat_id][user_id].append(alert)
        
        # Build confirmation message
        conditions = []
        if above_price:
            conditions.append(f"above ${above_price:,.2f}")
        if below_price:
            conditions.append(f"below ${below_price:,.2f}")
        
        condition_text = " and ".join(conditions)
        
        await update.message.reply_text(
            f"✅ Alert set for *{ticker}*!\n\n"
            f"Current Price: ${current_price:,.2f}\n"
            f"You'll be notified when the price goes {condition_text}.\n\n"
            f"Use /myalerts to view all your alerts.",
            parse_mode='Markdown'
        )
        
    except ValueError as e:
        await update.message.reply_text(
            "❌ Invalid price value! Please use numbers only.\n"
            "Example: `/alert AAPL above 300 below 200`",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error setting alert: {e}")
        await update.message.reply_text(
            f"❌ Error setting alert. Please try again.\n\n"
            f"Error: {str(e)}"
        )

async def view_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View user's active alerts"""
    if not update.message:
        return
    
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    alerts = user_alerts[chat_id].get(user_id, [])
    
    if not alerts:
        await update.message.reply_text(
            "📭 You don't have any active alerts.\n\n"
            "Set one with: `/alert TICKER above X below Y`",
            parse_mode='Markdown'
        )
        return
    
    message = "🔔 *Your Active Alerts:*\n\n"
    
    for idx, alert in enumerate(alerts, 1):
        conditions = []
        if alert['above']:
            conditions.append(f"above ${alert['above']:,.2f}")
        if alert['below']:
            conditions.append(f"below ${alert['below']:,.2f}")
        
        condition_text = " and ".join(conditions)
        created = alert['created_at'].strftime('%Y-%m-%d %H:%M')
        
        message += f"{idx}. *{alert['ticker']}* - {condition_text}\n"
        message += f"   _Created: {created}_\n\n"
    
    message += f"Total alerts: {len(alerts)}\n"
    message += f"Use `/removealert NUMBER` to remove an alert"
    
    await update.message.reply_text(message, parse_mode='Markdown')

async def remove_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a specific alert"""
    if not update.message:
        return
    
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text(
            "❌ Please specify the alert number.\n"
            "Example: `/removealert 1`\n\n"
            "Use /myalerts to see your alert numbers.",
            parse_mode='Markdown'
        )
        return
    
    alert_num = int(context.args[0])
    alerts = user_alerts[chat_id].get(user_id, [])
    
    if alert_num < 1 or alert_num > len(alerts):
        await update.message.reply_text(
            f"❌ Invalid alert number. You have {len(alerts)} alert(s).\n"
            "Use /myalerts to see your alerts.",
            parse_mode='Markdown'
        )
        return
    
    removed_alert = alerts.pop(alert_num - 1)
    
    await update.message.reply_text(
        f"✅ Alert removed!\n\n"
        f"*{removed_alert['ticker']}* alert has been deleted.",
        parse_mode='Markdown'
    )

async def check_alerts(context: ContextTypes.DEFAULT_TYPE):
    """Background task to check all alerts periodically"""
    logger.info("Checking alerts...")
    
    for chat_id, users in list(user_alerts.items()):
        for user_id, alerts in list(users.items()):
            for alert in alerts[:]:  # Create a copy to iterate
                try:
                    ticker = yf.Ticker(alert['ticker'])
                    current_price = ticker.info.get('currentPrice') or ticker.info.get('regularMarketPrice')
                    
                    if not current_price:
                        continue
                    
                    triggered = False
                    trigger_message = ""
                    
                    # Check if alert conditions are met
                    if alert['above'] and current_price >= alert['above']:
                        triggered = True
                        trigger_message = f"🚀 *ALERT TRIGGERED!*\n\n@{alert['username']}\n\n*{alert['ticker']}* has gone ABOVE ${alert['above']:,.2f}!\n\nCurrent Price: ${current_price:,.2f}"
                    
                    elif alert['below'] and current_price <= alert['below']:
                        triggered = True
                        trigger_message = f"📉 *ALERT TRIGGERED!*\n\n@{alert['username']}\n\n*{alert['ticker']}* has gone BELOW ${alert['below']:,.2f}!\n\nCurrent Price: ${current_price:,.2f}"
                    
                    if triggered:
                        # Send notification
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=trigger_message,
                            parse_mode='Markdown'
                        )
                        
                        # Remove the triggered alert
                        user_alerts[chat_id][user_id].remove(alert)
                        logger.info(f"Alert triggered for {alert['username']}: {alert['ticker']}")
                
                except Exception as e:
                    logger.error(f"Error checking alert for {alert['ticker']}: {e}")
                    continue

# ─── SPX 0DTE Gamma command ───
def parse_expiration_arg(args):
    """
    Looks for a date like MM/DD/YY or MM/DD/YYYY anywhere in the command's
    arguments -- e.g. "exp: 07/30/26", "exp:07/30/2026", or just "07/30/26"
    all work, since it scans rather than requiring exact syntax.

    Returns a "YYYY-MM-DD" string, or None if no args were given at all
    (meaning: use the default nearest-expiration behavior). Raises
    ValueError if something date-shaped was found but isn't a real date,
    so the caller can tell "no date requested" apart from "bad date".
    """
    if not args:
        return None

    text = " ".join(args)
    match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", text)
    if not match:
        raise ValueError("no date pattern found")

    month, day, year = match.groups()
    if len(year) == 2:
        year = "20" + year
    parsed = datetime.strptime(f"{month}/{day}/{year}", "%m/%d/%Y")
    return parsed.strftime("%Y-%m-%d")


# ─── Gamma cache: background worker keeps the 0DTE snapshot fresh ───
EASTERN = ZoneInfo("America/New_York")


def _is_market_hours():
    """Roughly true on a weekday between 9:30am and 4:00pm Eastern. It does
    NOT know about market holidays -- on a holiday the fetch simply returns
    no data and nothing gets cached, which is harmless."""
    now = datetime.now(EASTERN)
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    open_time = now.replace(hour=9, minute=30, second=0, microsecond=0)
    close_time = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return open_time <= now <= close_time


async def gamma_cache_worker(context: ContextTypes.DEFAULT_TYPE):
    """Runs on a timer the whole time the bot is up. During market hours it
    quietly pulls a fresh SPX 0DTE snapshot and saves it to the database, so
    that /gamma (with no date) can answer instantly from cache instead of
    waiting on Schwab. Outside market hours it does nothing."""
    if not _is_market_hours():
        return

    loop = asyncio.get_running_loop()
    try:
        # Same run-in-a-thread pattern as /gamma: the blocking network call
        # must not stall the bot's event loop.
        result = await loop.run_in_executor(None, run_snapshot, None)
    except Exception as e:
        logger.error(f"gamma cache worker error: {e}")
        return

    if result is None:
        logger.info("gamma worker: no contracts came back (market may be closed)")
    elif result.get("zero_oi"):
        logger.info("gamma worker: zero open interest, nothing cached yet")
    else:
        logger.info(f"gamma worker: cached fresh snapshot, spot {result['spot']}")


def _g(v, spec=",.0f"):
    """Format a number for the gamma reply, or 'n/a' if it's missing."""
    return format(v, spec) if v is not None else "n/a"


def _sign_word(v):
    if v is None:
        return ""
    return " (positive)" if v > 0 else " (negative)"


def build_gamma_reply(d, header, subtitle=None):
    """Format one gamma reading for Telegram. Reports the open-interest and
    volume bases side by side rather than picking one -- they answer
    different questions and often disagree."""
    lines = [f"*{header}*"]
    if subtitle:
        lines.append(f"_{subtitle}_")
    lines.append("")
    lines.append(f"Spot: ${_g(d.get('spot'), ',.2f')}")
    lines.append("")
    lines.append("*By Open Interest* (positioning)")
    lines.append(f"  Net GEX: ${_g(d.get('net_gamma'))}{_sign_word(d.get('net_gamma'))}")
    lines.append(f"  Call Wall: {_g(d.get('call_wall'), ',.1f')}")
    lines.append(f"  Put Wall: {_g(d.get('put_wall'), ',.1f')}")
    pc = d.get("pc_ratio")
    if pc is not None:
        lean = "put-heavy" if pc > 1.1 else "call-heavy" if pc < 0.9 else "balanced"
        lines.append(f"  Put/Call: {pc:.2f} ({lean})")
    lines.append("")
    lines.append("*By Volume* (today's flow)")
    lines.append(f"  Net GEX: ${_g(d.get('net_gamma_vol'))}{_sign_word(d.get('net_gamma_vol'))}")
    lines.append(f"  Call Wall: {_g(d.get('call_wall_vol'), ',.1f')}")
    lines.append(f"  Put Wall: {_g(d.get('put_wall_vol'), ',.1f')}")
    pcv = d.get("pc_ratio_vol")
    if pcv is not None:
        lean_v = "put-heavy" if pcv > 1.1 else "call-heavy" if pcv < 0.9 else "balanced"
        lines.append(f"  Put/Call: {pcv:.2f} ({lean_v})")
    # Delta basis only appears when there's prior-day history to compare
    # against -- on a fresh database it's absent rather than showing zeros.
    stats = d.get("delta_stats") or {}
    if d.get("net_gamma_delta") is not None:
        lines.append("")
        baseline = stats.get("baseline_date")
        label = f"*New Positioning* (change since {baseline})" if baseline \
            else "*New Positioning* (open-interest change)"
        lines.append(label)
        lines.append(
            f"  Net GEX: ${_g(d.get('net_gamma_delta'))}"
            f"{_sign_word(d.get('net_gamma_delta'))}"
        )
        lines.append(f"  Call Wall: {_g(d.get('call_wall_delta'), ',.1f')}")
        lines.append(f"  Put Wall: {_g(d.get('put_wall_delta'), ',.1f')}")
        if stats.get("churn") is not None:
            lines.append(f"  Churn: {stats['churn']}x volume vs net new OI")

    lines.append("")
    lines.append(f"Gamma Pin: {_g(d.get('gamma_pin'), ',.1f')}")
    lines.append(f"Gamma Flip: {_g(d.get('gamma_flip'), ',.2f')}")
    return "\n".join(lines)


async def cmd_gamma(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """SPX gamma. With no date it answers instantly from the cached 0DTE
    snapshot the background worker keeps fresh. With '/gamma exp: MM/DD/YY'
    it pulls that specific expiration live from Schwab."""
    message = update.effective_message

    try:
        requested_date = parse_expiration_arg(context.args)
    except ValueError:
        await message.reply_text(
            "Couldn't read that date. Try: `/gamma exp: 07/30/26`",
            parse_mode='Markdown',
        )
        return

    loop = asyncio.get_running_loop()

    # ── No date given: serve the cached 0DTE snapshot (no Schwab call) ──
    if requested_date is None:
        cached, why_not = await loop.run_in_executor(
            None, latest_snapshot_or_reason)

        if cached is not None:
            exp = cached.get("target_date")
            header = f"SPX Gamma -- {exp}" if exp else "SPX 0DTE Gamma"
            reply = build_gamma_reply(
                cached, header, f"as of {cached['timestamp']} UTC"
            )
            await message.reply_text(reply, parse_mode='Markdown')
            return

        # Either nothing is cached, or what's there can't be trusted (an
        # expired expiration, or a stalled worker). Say which, then pull live.
        await message.reply_text(
            f"📡 {why_not or 'no usable cache'} -- pulling SPX 0DTE live...")

    # ── A specific date was requested (or the cache was empty): pull live ──
    if requested_date:
        await message.reply_text(f"📡 Pulling SPX chain for {requested_date} and calculating gamma...")

    try:
        # run_snapshot() makes blocking network + file calls, so it runs in
        # a worker thread rather than on the bot's own event loop.
        result = await loop.run_in_executor(None, run_snapshot, requested_date)
    except Exception as e:
        logger.error(f"/gamma error: {e}")
        await message.reply_text(f"❌ Failed to pull gamma data: {e}")
        return

    if result is None:
        which = requested_date or "the nearest expiration"
        await message.reply_text(f"No contracts came back for {which} -- double check the date, or the market may be closed.")
        return

    if result["zero_oi"]:
        await message.reply_text(
            f"⚠️ Got {len(result['contracts'])} contracts for {result['target_date']}, but "
            f"{result.get('reason', 'the data came back unusable')}.\n\n"
            "No numbers to report -- showing walls from this would be misleading. "
            "Try again during market hours (9:30am-4:00pm ET, weekdays)."
        )
        return

    reply = build_gamma_reply(
        result,
        f"SPX Gamma -- {result['target_date']}",
        "live pull",
    )
    await message.reply_text(reply, parse_mode='Markdown')


def _compact(v):
    """$1.2B / $340M / $12K -- keeps the per-expiration rows narrow enough
    to read on a phone."""
    if v is None:
        return "n/a"
    sign = "-" if v < 0 else ""
    a = abs(v)
    for cutoff, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if a >= cutoff:
            return f"{sign}${a/cutoff:.1f}{suffix}"
    return f"{sign}${a:.0f}"


async def cmd_gamma_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Gamma across every expiration from the nearest live one through
    Friday. '/gammaweek MM/DD/YY' starts the range at that date instead.

    Always a live pull -- the 5-minute cache worker only maintains the 0DTE
    snapshot, since caching every expiration would multiply both the API
    load and the stored rows for a view that gets used far less often."""
    message = update.effective_message

    if not WEEKLY_AVAILABLE:
        await message.reply_text(
            "Weekly gamma isn't available -- gamma_dashboard.py needs updating. "
            "The bot's startup log has the details. /gamma still works."
        )
        return

    try:
        start_date = parse_expiration_arg(context.args)
    except ValueError:
        await message.reply_text(
            "Couldn't read that date. Try: `/gammaweek 07/23/26`",
            parse_mode='Markdown',
        )
        return

    await message.reply_text("📡 Pulling the full week's SPX chain -- this takes longer than /gamma...")

    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(None, run_weekly_snapshot, start_date)
    except Exception as e:
        logger.error(f"/gammaweek error: {e}")
        await message.reply_text(f"❌ Failed to pull weekly gamma data: {e}")
        return

    if result is None:
        await message.reply_text(
            "No contracts came back for that range -- double check the date, "
            "or the market may be closed."
        )
        return

    if result["zero_oi"]:
        await message.reply_text(
            f"⚠️ Got {result['total_contracts']} contracts for "
            f"{result['from_date']} to {result['to_date']}, but {result['reason']}.\n\n"
            "No numbers to report. Try again during market hours "
            "(9:30am-4:00pm ET, weekdays)."
        )
        return

    agg = result["aggregate"]
    lines = [
        f"*SPX Gamma -- Week*",
        f"_{result['from_date']} to {result['to_date']}, live_",
        f"_{result['total_contracts']} contracts across "
        f"{len(result['expirations'])} expirations_",
        "",
        f"Spot: ${_g(result['spot'], ',.2f')}",
        "",
        "*AGGREGATE -- all expirations*",
        "By Open Interest",
        f"  Net GEX: ${_g(agg['net_gamma'])}{_sign_word(agg['net_gamma'])}",
        f"  Call Wall: {_g(agg['call_wall'], ',.1f')}",
        f"  Put Wall: {_g(agg['put_wall'], ',.1f')}",
        "By Volume",
        f"  Net GEX: ${_g(agg['net_gamma_vol'])}{_sign_word(agg['net_gamma_vol'])}",
        f"  Call Wall: {_g(agg['call_wall_vol'], ',.1f')}",
        f"  Put Wall: {_g(agg['put_wall_vol'], ',.1f')}",
        f"Gamma Pin: {_g(agg['gamma_pin'], ',.1f')}",
        f"Gamma Flip: {_g(agg['gamma_flip'], ',.2f')}",
        "",
        "*BY EXPIRATION* (open interest)",
    ]

    for row in result["by_expiration"]:
        label = f"{row['expiration'][5:]} ({row['dte']}DTE)"
        if not row.get("usable"):
            lines.append(f"  {label}  no Greeks")
            continue
        lines.append(
            f"  {label}  C {_g(row['call_wall'], ',.0f')}"
            f"  P {_g(row['put_wall'], ',.0f')}"
            f"  net {_compact(row['net_gamma'])}"
        )

    await message.reply_text("\n".join(lines), parse_mode='Markdown')


async def cleanup_worker(context: ContextTypes.DEFAULT_TYPE):
    """Once a day, drop raw option contracts whose expiration has already
    passed (beyond the retention window set in cleanup.py). Runs a couple
    of minutes after startup and every 24 hours after that, so it happens
    at least once per bot session even if the bot is restarted often.

    Only touches raw contract rows -- the gamma_snapshots timeline that
    /gamma reads is never affected."""
    if not CLEANUP_AVAILABLE:
        return
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(None, cleanup_expired)
    except Exception as e:
        logger.error(f"cleanup worker error: {e}")
        return
    logger.info(f"cleanup: {describe_cleanup(result)}")


# ─── Single-instance guard ───
# Telegram allows only ONE poller per bot token. If a second copy of this
# script starts, the two fight over getUpdates and each message goes to
# whichever grabs it first -- so commands appear to work intermittently
# with no error anywhere. That's maddening to debug, so we prevent it.
#
# Binding a local port is a simple, reliable lock: the OS refuses a second
# bind, and it's released automatically when the process dies (including
# on a hard kill, unlike a lock file that can be left behind).
_INSTANCE_LOCK_PORT = 47201
_instance_lock = None


def acquire_instance_lock():
    """Return True if this is the only instance, False if one is running."""
    global _instance_lock
    _instance_lock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        _instance_lock.bind(("127.0.0.1", _INSTANCE_LOCK_PORT))
        _instance_lock.listen(1)
        return True
    except OSError:
        _instance_lock.close()
        _instance_lock = None
        return False


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Catch anything a command handler throws, log it with a full traceback,
    and tell the user what happened.

    Without this, an exception inside a handler produces NO reply at all --
    the command just goes silent while every other command keeps working,
    which is close to impossible to diagnose from the chat window. This
    turns that into a visible error message."""
    logger.error("Unhandled exception:", exc_info=context.error)
    tb = "".join(traceback.format_exception(
        type(context.error), context.error, context.error.__traceback__
    ))
    logger.error(f"Traceback:\n{tb}")

    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text(
                "❌ That command hit an error:\n"
                f"`{type(context.error).__name__}: {context.error}`\n\n"
                "The bot's console has the full traceback.",
                parse_mode='Markdown',
            )
    except Exception:
        # Never let the error handler itself throw.
        pass


async def new_chat_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Greet the bot when it's added to a group"""
    for member in update.message.new_chat_members:
        if member.id == context.bot.id:
            # The bot was added to the group
            welcome_text = """
👋 Thanks for adding me to this group!

I'm a Stock Analysis Bot. Anyone in this group can use me to get detailed stock market analysis.

*Commands:*
/stock TICKER - Get comprehensive stock analysis
/indices - Get major market indices snapshot
/feargreed - Get CNN Fear & Greed Index
/alert TICKER above X below Y - Set price alerts
/myalerts - View your active alerts
/removealert NUMBER - Remove an alert
/help - Show available commands

Example: `/stock AAPL` to analyze Apple Inc.
Example: `/alert TSLA above 500 below 300` to get notified when Tesla hits those prices

Let's analyze some stocks! 📈
            """
            await update.message.reply_text(welcome_text, parse_mode='Markdown')


# ─── Folder Watcher ───
import time

def _retire_image(path):
    """Get a sent image out of the watch folder.

    Deleting is preferred, but Windows will refuse while another program
    holds the file open. In that case fall back to moving it into Sent/,
    which gets it out of the glob just as effectively. If both fail the
    name stays in _already_sent, so it still won't be re-sent."""
    try:
        path.unlink()
        return True
    except Exception:
        try:
            path.rename(SENT_FOLDER / path.name)
            return True
        except Exception as e:
            logger.warning(f"couldn't remove or move {path.name}: {e}")
            return False


async def watch_folder(context):
    global pending_files, last_change_time, _watcher_busy

    # A send can take longer than the 2-second tick. Without this guard a
    # second tick could read the same files and upload them again.
    if _watcher_busy:
        return

    WATCH_FOLDER.mkdir(exist_ok=True)
    SENT_FOLDER.mkdir(exist_ok=True)

    now = time.time()
    current_files = set()

    for f in WATCH_FOLDER.glob("*"):
        if f.suffix.lower() not in [".png", ".jpg", ".jpeg"]:
            continue
        if not f.is_file():
            continue

        if f.name in _already_sent:
            # Already went out; it's only still here because cleanup failed.
            # Retry the cleanup, but never send it a second time.
            _retire_image(f)
            continue

        # If the file is still growing, the writing program isn't done with
        # it. Count that as activity so the quiet window restarts.
        try:
            size = f.stat().st_size
        except OSError:
            continue
        if _last_sizes.get(f.name) != size:
            _last_sizes[f.name] = size
            last_change_time = now

        current_files.add(f)

    for f in current_files:
        if f.name not in pending_files:
            pending_files.add(f.name)
            last_change_time = now

    if not pending_files or (now - last_change_time) <= 5:
        return

    _watcher_busy = True
    try:
        names = [n for n in sorted(pending_files) if (WATCH_FOLDER / n).exists()]

        # Telegram caps a media group at 10, so send in batches.
        for i in range(0, len(names), MEDIA_GROUP_LIMIT):
            batch = names[i:i + MEDIA_GROUP_LIMIT]
            media = []
            for fname in batch:
                # Read fully into memory and close the handle immediately, so
                # the file isn't locked by us when we try to remove it below.
                with open(WATCH_FOLDER / fname, "rb") as fh:
                    media.append(InputMediaPhoto(io.BytesIO(fh.read())))

            if not media:
                continue

            await context.bot.send_media_group(chat_id=TARGET_CHAT_ID, media=media)

            # Mark as sent BEFORE attempting cleanup. If the delete fails, the
            # file remains on disk -- but it can never be sent again, which is
            # the bug this ordering exists to prevent.
            _already_sent.update(batch)
            for fname in batch:
                _retire_image(WATCH_FOLDER / fname)

    except Exception as e:
        logger.error(f"Folder watch error: {e}")
    finally:
        for n in list(pending_files):
            _last_sizes.pop(n, None)
        pending_files.clear()
        _watcher_busy = False

        # Keep _already_sent from growing forever: the only names worth
        # remembering are ones still sitting on disk.
        if len(_already_sent) > 500:
            still_here = {p.name for p in WATCH_FOLDER.glob("*")}
            _already_sent.intersection_update(still_here)


def main():
    """Start the bot."""
    if not acquire_instance_lock():
        print()
        print("=" * 62)
        print(" ANOTHER COPY OF THIS BOT IS ALREADY RUNNING.")
        print()
        print(" Two copies fight over Telegram updates, which makes commands")
        print(" respond only some of the time. Refusing to start a second one.")
        print()
        print(" To see them:     tasklist | findstr python")
        print(" To stop one:     taskkill /F /PID <the number>")
        print("=" * 62)
        print()
        return

    # Create the Application
    application = Application.builder().token(BOT_TOKEN).build()

    # Register command handlers (work in both private chats and groups)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("stock", stock_analysis))
    application.add_handler(CommandHandler("indices", indices))
    application.add_handler(CommandHandler("feargreed", fear_greed_index))
    application.add_handler(CommandHandler("news", cmd_news))
    application.add_handler(CommandHandler("alert", set_alert))
    application.add_handler(CommandHandler("myalerts", view_alerts))
    application.add_handler(CommandHandler("removealert", remove_alert))
    application.add_handler(CommandHandler("gamma", cmd_gamma))
    application.add_handler(CommandHandler("gammaweek", cmd_gamma_week))
    
    # Handler for when bot is added to a group
    application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, new_chat_members))

    # Must be registered, or handler exceptions vanish silently.
    application.add_error_handler(error_handler)

    # Schedule the alert checking task (runs every 60 seconds)
    job_queue = application.job_queue
    job_queue.run_repeating(check_alerts, interval=60, first=10)

    # Folder watcher (every 2 seconds)
    job_queue.run_repeating(watch_folder, interval=2, first=5)

    # Gamma cache worker: refresh the SPX 0DTE snapshot every 5 minutes so
    # /gamma answers instantly from cache. Only pulls during market hours.
    job_queue.run_repeating(gamma_cache_worker, interval=300, first=15)

    # Daily housekeeping: drop raw contracts for expirations that have passed.
    # 86400 seconds = 24 hours. Starts 2 minutes after boot. Skipped entirely
    # if cleanup.py isn't present -- housekeeping must never block the bot.
    if CLEANUP_AVAILABLE:
        job_queue.run_repeating(cleanup_worker, interval=86400, first=120)
    else:
        logger.warning("cleanup.py not found -- daily housekeeping is off")

    # Start the Bot
    logger.info("Bot is starting... Ready to work in private chats and groups!")
    logger.info("Alert monitoring active - checking every 60 seconds")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()