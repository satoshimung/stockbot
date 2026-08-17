# stockbot

Telegram bot for stock analysis and SPX gamma. Companion to `schwab_market_pipeline`.

## Setup

1. Create and activate the virtual environment:

   ```
   python -m venv venv
   venv\Scripts\activate
   ```

2. Install dependencies:

   ```
   pip install -r requirements.txt
   ```

3. Create a `.env` file in this folder containing:

   ```
   TELEGRAM_BOT_TOKEN=your_token_from_botfather
   ```

4. Make the pipeline importable. Line 25 imports `gamma_dashboard` from the
   `schwab_market_pipeline` project, which lives in a separate folder:

   ```
   python -c "import sys,pathlib; p=pathlib.Path([x for x in sys.path if x.endswith('site-packages')][0])/'pipeline.pth'; p.write_text(r'C:\dev\schwab_market_pipeline'); print('wrote', p)"
   ```

   This `.pth` file lives inside `venv/` and is NOT tracked by Git.
   Recreate it any time the venv is rebuilt.

## Paths to check on a new machine

- `WATCH_FOLDER` (line 61) — currently `C:\dev\OptionsLiveData`
- `TARGET_CHAT_ID` (line 63) — the group the watcher posts images to

## Run

```
python Claude_stockbot_FINAL.py
```

## Background jobs

Four repeating jobs start with the bot:

| Job | Interval | Purpose |
|---|---|---|
| `check_alerts` | 60s | Price alert polling |
| `watch_folder` | 2s | Sends new images from `WATCH_FOLDER` to `TARGET_CHAT_ID` |
| `gamma_cache_worker` | 300s | Refreshes the cached gamma snapshot |
| `cleanup_worker` | 24h | Prunes expired data (skipped if pipeline unavailable) |

`watch_folder` waits for 5 seconds of no file-size change before sending, so
images post a few seconds after they land rather than instantly.
