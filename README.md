# The Hindu Editorial Pipeline (Python)

This pipeline does the following:
- Crawls The Hindu opinion sections by default:
  - `https://www.thehindu.com/opinion/editorial/`
  - `https://www.thehindu.com/opinion/op-ed/`
  - `https://www.thehindu.com/opinion/lead/`
  - `https://www.thehindu.com/opinion/`
- Extracts editorial article text and summarizes each editorial with Gemini 2.5 Flash
- Builds daily editorial JSON + PDF report
- Uploads editorial JSON/PDF to S3 and optionally posts PDF to Slack

## 1) Setup

```powershell
cd D:\editorial_dpt
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m playwright install chromium
Copy-Item .env.example .env
```

Update `.env` with:
- `GEMINI_API_KEY` (for editorial summaries and daily takeaways)
- Optional: `GEMINI_MODEL` (defaults to `gemini-2.5-flash`)
- `S3_BUCKET` (+ AWS credentials in your environment/IAM role)
- Optional S3 alternatives: `S3_BUCKET_EVENT` or `S3_URI` (example: `s3://pulse-narrative/editorials/`)
- Optional source overrides:
  - `HINDU_EDITORIAL_URL` to set the primary section URL
  - `HINDU_EDITORIAL_URLS` as a comma-separated override list for all opinion source pages
- Hindu login/browser options:
  - `HINDU_USE_BROWSER_LOGIN=true` (recommended for subscriber-only access)
  - `HINDU_BROWSER_HEADLESS=false` to watch the login flow while debugging
  - `HINDU_INTERACTIVE_LOGIN_WAIT_SECONDS=90` to complete captcha/OTP manually before run starts
  - `HINDU_STORAGE_STATE_PATH=.hindu_storage_state.json` to persist/reuse authenticated session
  - `HINDU_PREFER_PRINT_VIEW=true` to use print/amp content when available for editorial extraction
- Optional selector overrides (comma-separated): `HINDU_SIGNIN_SELECTORS`, `HINDU_EMAIL_SELECTORS`, `HINDU_PASSWORD_SELECTORS`, `HINDU_SUBMIT_SELECTORS`
- No separate Gemini SDK is required; this pipeline calls the Gemini API directly with `requests`

## 2) Run

```powershell
python editorial_pipeline.py
```

Optional date override:

```powershell
python editorial_pipeline.py --date 2026-03-12
```

Outputs are written to:
- `outputs/YYYY-MM-DD/editorials.json`
- `outputs/YYYY-MM-DD/daily-report.pdf`

S3 upload target:
- `s3://<bucket>/<prefix>/YYYY-MM-DD/editorials.json`
- `s3://<bucket>/<prefix>/YYYY-MM-DD/daily-report.pdf`

## 3) Daily Scheduling (Windows Task Scheduler)

Use this action command:

```powershell
Program/script: D:\editorial_dpt\.venv\Scripts\python.exe
Add arguments: D:\editorial_dpt\editorial_pipeline.py
Start in: D:\editorial_dpt
```

Set trigger to run daily at your preferred IST time.

## Notes
- The script first tries browser login (Playwright), then falls back to form-login if needed.
- Browser login supports iframe/popup forms and can reuse a saved Playwright storage state.
- This module now handles editorials only. A separate Playwright-based e-paper module can be added independently.
