import argparse
import hashlib
import html
import json
import logging
import os
import re
import time
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
import boto3
import requests
import dateutil
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from dateutil import tz as date_tz
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import HRFlowable, Paragraph, SimpleDocTemplate, Spacer
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_HINDU_EDITORIAL_URL = "https://www.thehindu.com/opinion/lead/"
DEFAULT_HINDU_EDITORIAL_URLS = [
    "https://www.thehindu.com/opinion/lead/",
    "https://www.thehindu.com/opinion/editorial/",
    "https://www.thehindu.com/opinion/op-ed/",
    "https://www.thehindu.com/data/"
]
DEFAULT_INDIAN_EXPRESS_EDITORIAL_URLS = [
    "https://indianexpress.com/section/opinion/editorials/?ref=l1_section",
    "https://indianexpress.com/section/opinion/columns/?ref=l1_section",
]
ALLOWED_OPINION_PATH_PREFIXES = (
    "/opinion/lead/",
    "/opinion/editorial/",
    "/opinion/op-ed/",
)
BLOCKED_OPINION_PATH_PREFIXES = (
    "/opinion/columns/",
)
PDF_SECTION_ORDER = {
    "Lead": 0,
    "Editorial": 1,
    "Op-Ed": 2,
    "Column": 3,
}
SOURCE_SECTION_ORDER = {
    "The Hindu": 0,
    "Indian Express": 1,
    "BusinessLine": 2,
}
DEFAULT_HINDU_USERNAME = "admin@tailnode.com"
DEFAULT_HINDU_PASSWORD = "ng3#*wU4pDwHjt4"
DEFAULT_TIMEZONE = "Asia/Kolkata"
DEFAULT_OUTPUT_DIR = "outputs"
DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"
DEFAULT_S3_PREFIX = "hindu-editorials"
GEMINI_API_URL_TEMPLATE = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
REQUEST_TIMEOUT_SECONDS = 30
INDIAN_EXPRESS_SECTION_MAX_PAGES = 10
DATE_WINDOW_CUTOFF_HOUR = 9
DEFAULT_SLACK_UPLOAD_MAX_RETRIES = 4
DEFAULT_SLACK_RETRY_BACKOFF_SECONDS = 5
DEFAULT_SLACK_FALLBACK_LINK_EXPIRY_SECONDS = 7 * 24 * 60 * 60
SLACK_RETRYABLE_ERRORS = {
    "internal_error",
    "ratelimited",
    "request_timeout",
    "service_unavailable",
    "fatal_error",
    "temporary_error",
}
MIN_DAILY_TAKEAWAYS = 3
MAX_DAILY_TAKEAWAYS = 5
SUMMARY_WORD_LIMIT = 70
IMPLICATIONS_WORD_LIMIT = 10
KEY_TAKEAWAY_CHAR_LIMIT = 80
MOJIBAKE_MARKERS = ("Ã¢â‚¬", "Ã¢â‚¬â„¢", "Ã¢â‚¬Å“", "Ã¢â‚¬Ëœ", "Ã¢â‚¬â€œ", "Ã¢â‚¬â€", "Ã‚", "Ãƒ")
ZERO_WIDTH_TRANSLATION = str.maketrans("", "", "\u200b\u200c\u200d\ufeff")
DAILY_TAKEAWAY_STOPWORDS = {
    "about",
    "after",
    "also",
    "among",
    "because",
    "between",
    "could",
    "daily",
    "editorial",
    "first",
    "from",
    "have",
    "into",
    "itself",
    "more",
    "must",
    "need",
    "only",
    "over",
    "same",
    "should",
    "some",
    "such",
    "than",
    "that",
    "their",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "today",
    "under",
    "very",
    "what",
    "when",
    "where",
    "which",
    "while",
    "with",
    "would",
}
PDF_HIGHLIGHT_STOPWORDS = {
    "about",
    "ahead",
    "article",
    "comment",
    "challenges",
    "editorial",
    "government",
    "governments",
    "india",
    "indian",
    "interview",
    "losing",
    "national",
    "opinion",
    "path",
    "report",
    "series",
    "should",
    "summary",
    "their",
    "these",
    "they",
    "this",
    "using",
    "whole",
    "world",
}
ARTICLE_TAIL_NOISE_PHRASES = (
    "this is a premium article",
    "available exclusively to subscribers",
    "the hindu delivers independent",
    "this offer is only for first-time subscribers",
    "to continue reading",
)
ARTICLE_BODY_NOISE_PREFIXES = (
    "also read |",
    "comment |",
    "editorial |",
    "editoial |",
    "explained |",
    "podcast |",
    "watch |",
    "video |",
    "read more |",
)
GENERIC_AUTHOR_LABELS = {
    "editorial",
    "ie online",
    "pti",
    "ani",
    "reuters",
    "associated press",
    "ap",
    "express web desk",
}
AUTHOR_LINE_BLOCKLIST_PHRASES = (
    "make us preferred source on google",
    "preferred source on google",
    "follow us",
    "bookmark",
    "comment",
    "sign in",
    "log in",
    "subscribe",
)
AUTHOR_BIO_ABBREVIATIONS = (
    "Dr.",
    "Mr.",
    "Ms.",
    "Mrs.",
    "Prof.",
    "Sr.",
    "Jr.",
    "St.",
)
COMMON_MOJIBAKE_REPLACEMENTS = {
    "â€™": "'",
    "â€œ": '"',
    "â€": '"',
    "â€˜": "'",
    "â€”": "-",
    "â€“": "-",
    "â€¦": "...",
    "â‚¹": "Rs. ",
    "Â ": " ",
    "Â": "",
}


@dataclass
class PipelineConfig:
    target_date: str
    timezone: str
    output_dir: str
    editorial_url: str
    editorial_urls: list[str]
    hindu_editorial_urls: list[str]
    hindu_username: str
    hindu_password: str
    hindu_login_urls: list[str]
    hindu_use_browser_login: bool
    hindu_browser_headless: bool
    hindu_browser_timeout_seconds: int
    hindu_interactive_login_wait_seconds: int
    hindu_storage_state_path: str
    hindu_prefer_print_view: bool
    indian_express_editorial_urls: list[str]
    indian_express_username: str
    indian_express_password: str
    indian_express_login_urls: list[str]
    indian_express_use_browser_login: bool
    indian_express_browser_headless: bool
    indian_express_browser_timeout_seconds: int
    indian_express_interactive_login_wait_seconds: int
    indian_express_storage_state_path: str
    indian_express_prefer_print_view: bool
    gemini_api_key: str
    gemini_model: str
    aws_region: str
    s3_bucket: str
    s3_prefix: str
    slack_bot_token: str
    slack_channel_id: str
    slack_webhook_url: str
    slack_upload_max_retries: int
    slack_retry_backoff_seconds: int
    slack_fallback_link_expiry_seconds: int


@dataclass
class Editorial:
    id: str
    title: str
    url: str
    published_at: str
    editorial_date: str
    body: str
    summary: str = ""
    key_points: list[str] | None = None
    tone: str = ""
    implications: str = ""
    author: str = ""
    publisher: str = ""
    author_lines: list[str] | None = None
    highlight_phrases: list[str] | None = None
    summary_markup: str = ""
    key_takeaway_markup: str = ""
    implications_markup: str = ""
    tone_markup: str = ""


def load_env_file(file_path: str = ".env") -> None:
    env_path = Path(file_path)
    if not env_path.exists():
        return

    def parse_env_value(raw_value: str) -> str:
        trimmed = raw_value.strip()
        if not trimmed:
            return ""
        if len(trimmed) >= 2 and trimmed[0] == trimmed[-1] and trimmed[0] in {"'", '"'}:
            return trimmed[1:-1]
        return re.sub(r"\s+#.*$", "", trimmed).strip()

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = parse_env_value(value)
        if key and key not in os.environ:
            os.environ[key] = value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Editorial and opinion pipeline")
    parser.add_argument("--date", help="Target date in YYYY-MM-DD")
    parser.add_argument("--timezone", help="Timezone for date filtering")
    parser.add_argument("--output-dir", help="Local output directory")
    return parser.parse_args()


def parse_s3_uri(value: str) -> tuple[str, str]:
    if not value:
        return "", ""
    parsed = urlparse(value.strip())
    if parsed.scheme != "s3" or not parsed.netloc:
        return "", ""
    bucket = parsed.netloc.strip()
    prefix = parsed.path.strip("/")
    return bucket, prefix


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None or not value.strip():
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def dedupe_preserve_order(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return unique


def parse_url_list_config(csv_env_name: str, single_env_name: str, default_urls: list[str]) -> list[str]:
    raw_csv = os.getenv(csv_env_name, "").strip()
    if raw_csv:
        urls = [item.strip() for item in raw_csv.split(",") if item.strip()]
    else:
        primary_url = os.getenv(single_env_name, default_urls[0] if default_urls else "").strip()
        urls = [primary_url, *default_urls] if primary_url else list(default_urls)
    return dedupe_preserve_order([item for item in urls if item])


def parse_optional_url_list_config(csv_env_name: str, single_env_name: str) -> list[str]:
    raw_csv = os.getenv(csv_env_name, "").strip()
    if raw_csv:
        return dedupe_preserve_order([item.strip() for item in raw_csv.split(",") if item.strip()])
    single_value = os.getenv(single_env_name, "").strip()
    return [single_value] if single_value else []


def resolve_target_date(date_arg: str | None, timezone_name: str) -> str:
    if date_arg:
        datetime.strptime(date_arg, "%Y-%m-%d")
        return date_arg
    return datetime.now(get_tzinfo(timezone_name)).date().isoformat()


def get_tzinfo(timezone_name: str):
    candidates = [timezone_name]
    if timezone_name == "Asia/Kolkata":
        candidates.append("Asia/Calcutta")

    for candidate in candidates:
        try:
            return ZoneInfo(candidate)
        except ZoneInfoNotFoundError:
            continue

    for candidate in candidates:
        tz_info = date_tz.gettz(candidate)
        if tz_info is not None:
            return tz_info

    logging.warning(
        "Timezone '%s' not found. Falling back to UTC. Install tzdata to fix this.",
        timezone_name,
    )
    return timezone.utc


def build_config(args: argparse.Namespace) -> PipelineConfig:
    timezone_name = args.timezone or os.getenv("TIMEZONE", DEFAULT_TIMEZONE)
    target_date = resolve_target_date(args.date or os.getenv("TARGET_DATE"), timezone_name)
    s3_uri_bucket, s3_uri_prefix = parse_s3_uri(os.getenv("S3_URI", ""))
    s3_bucket = os.getenv("S3_BUCKET", "") or os.getenv("S3_BUCKET_EVENT", "") or s3_uri_bucket
    s3_prefix = os.getenv("S3_PREFIX", "") or s3_uri_prefix or DEFAULT_S3_PREFIX
    hindu_editorial_urls = parse_url_list_config(
        "HINDU_EDITORIAL_URLS",
        "HINDU_EDITORIAL_URL",
        DEFAULT_HINDU_EDITORIAL_URLS,
    )
    indian_express_editorial_urls = parse_url_list_config(
        "INDIAN_EXPRESS_EDITORIAL_URLS",
        "INDIAN_EXPRESS_EDITORIAL_URL",
        DEFAULT_INDIAN_EXPRESS_EDITORIAL_URLS,
    )
    editorial_urls = dedupe_preserve_order([*hindu_editorial_urls, *indian_express_editorial_urls])
    hindu_login_urls = parse_optional_url_list_config("HINDU_LOGIN_URLS", "HINDU_LOGIN_URL")
    if not hindu_login_urls:
        hindu_login_urls = ["https://www.thehindu.com/login"]
    indian_express_login_urls = parse_optional_url_list_config(
        "INDIAN_EXPRESS_LOGIN_URLS",
        "INDIAN_EXPRESS_LOGIN_URL",
    )
    return PipelineConfig(
        target_date=target_date,
        timezone=timezone_name,
        output_dir=args.output_dir or os.getenv("OUTPUT_DIR", DEFAULT_OUTPUT_DIR),
        editorial_url=editorial_urls[0],
        editorial_urls=editorial_urls,
        hindu_editorial_urls=hindu_editorial_urls,
        hindu_username=os.getenv("HINDU_USERNAME", DEFAULT_HINDU_USERNAME),
        hindu_password=os.getenv("HINDU_PASSWORD", DEFAULT_HINDU_PASSWORD),
        hindu_login_urls=hindu_login_urls,
        hindu_use_browser_login=parse_bool(os.getenv("HINDU_USE_BROWSER_LOGIN"), default=True),
        hindu_browser_headless=parse_bool(os.getenv("HINDU_BROWSER_HEADLESS"), default=True),
        hindu_browser_timeout_seconds=int(os.getenv("HINDU_BROWSER_TIMEOUT_SECONDS", "60")),
        hindu_interactive_login_wait_seconds=int(os.getenv("HINDU_INTERACTIVE_LOGIN_WAIT_SECONDS", "0")),
        hindu_storage_state_path=os.getenv("HINDU_STORAGE_STATE_PATH", ".hindu_storage_state.json"),
        hindu_prefer_print_view=parse_bool(os.getenv("HINDU_PREFER_PRINT_VIEW"), default=True),
        indian_express_editorial_urls=indian_express_editorial_urls,
        indian_express_username=os.getenv("INDIAN_EXPRESS_USERNAME", ""),
        indian_express_password=os.getenv("INDIAN_EXPRESS_PASSWORD", ""),
        indian_express_login_urls=indian_express_login_urls,
        indian_express_use_browser_login=parse_bool(os.getenv("INDIAN_EXPRESS_USE_BROWSER_LOGIN"), default=True),
        indian_express_browser_headless=parse_bool(os.getenv("INDIAN_EXPRESS_BROWSER_HEADLESS"), default=True),
        indian_express_browser_timeout_seconds=int(os.getenv("INDIAN_EXPRESS_BROWSER_TIMEOUT_SECONDS", "60")),
        indian_express_interactive_login_wait_seconds=int(
            os.getenv("INDIAN_EXPRESS_INTERACTIVE_LOGIN_WAIT_SECONDS", "0")
        ),
        indian_express_storage_state_path=os.getenv(
            "INDIAN_EXPRESS_STORAGE_STATE_PATH",
            ".indian_express_storage_state.json",
        ),
        indian_express_prefer_print_view=parse_bool(os.getenv("INDIAN_EXPRESS_PREFER_PRINT_VIEW"), default=False),
        gemini_api_key=os.getenv("GEMINI_API_KEY", ""),
        gemini_model=os.getenv("GEMINI_MODEL", DEFAULT_GEMINI_MODEL),
        aws_region=os.getenv("AWS_REGION", "ap-south-1"),
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        slack_bot_token=os.getenv("SLACK_BOT_TOKEN", "").strip(),
        slack_channel_id=os.getenv("SLACK_CHANNEL_ID", "").strip(),
        slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL", "").strip(),
        slack_upload_max_retries=max(1, int(os.getenv("SLACK_UPLOAD_MAX_RETRIES", str(DEFAULT_SLACK_UPLOAD_MAX_RETRIES)))),
        slack_retry_backoff_seconds=max(
            1,
            int(os.getenv("SLACK_RETRY_BACKOFF_SECONDS", str(DEFAULT_SLACK_RETRY_BACKOFF_SECONDS))),
        ),
        slack_fallback_link_expiry_seconds=max(
            300,
            int(
                os.getenv(
                    "SLACK_FALLBACK_LINK_EXPIRY_SECONDS",
                    str(DEFAULT_SLACK_FALLBACK_LINK_EXPIRY_SECONDS),
                )
            ),
        ),
    )


def normalize_editorial_url(raw_url: str) -> str | None:
    try:
        parsed = urlparse(raw_url)
    except Exception:
        return None

    if parsed.scheme not in {"http", "https"}:
        return None

    host = (parsed.netloc or "").lower()
    path = re.sub(r"/{2,}", "/", parsed.path).rstrip("/")
    if host.endswith("thehindu.com"):
        if not path.startswith("/opinion/"):
            return None
        if any(path.startswith(prefix.rstrip("/")) or path.startswith(prefix) for prefix in BLOCKED_OPINION_PATH_PREFIXES):
            return None
        if not any(path.startswith(prefix) for prefix in ALLOWED_OPINION_PATH_PREFIXES):
            return None
        if not path.endswith(".ece"):
            return None
        if path in {
            "/opinion",
            "/opinion/editorial",
            "/opinion/op-ed",
            "/opinion/lead",
        }:
            return None
        return f"https://www.thehindu.com{path}"

    if host.endswith("indianexpress.com"):
        if path.startswith("/section/"):
            return None
        if path.startswith("/article/opinion/editorials/") or path.startswith("/article/opinion/columns/"):
            return f"https://indianexpress.com{path}/"
        return None

    return None


def parse_json_ld_nodes(payload: Any) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []

    def walk(item: Any) -> None:
        if isinstance(item, dict):
            if "@graph" in item and isinstance(item["@graph"], list):
                for child in item["@graph"]:
                    walk(child)
            else:
                nodes.append(item)
        elif isinstance(item, list):
            for child in item:
                walk(child)

    walk(payload)
    return nodes


def parse_datetime(value: str | None, timezone_name: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = date_parser.parse(value)
    except Exception:
        return None

    tz = get_tzinfo(timezone_name)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


def extract_date_from_url(article_url: str) -> str | None:
    match = re.search(r"/(20\d{2})/(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/", article_url)
    if not match:
        return None
    return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"


def first_non_empty(*values: str | None) -> str:
    for value in values:
        if value and value.strip():
            return value.strip()
    return ""


def flatten_json_ld_names(value: Any) -> list[str]:
    names: list[str] = []
    if isinstance(value, str):
        cleaned = clean_text(value)
        if cleaned:
            names.append(cleaned)
        return names
    if isinstance(value, dict):
        for key in ("name", "headline", "legalName"):
            if value.get(key):
                cleaned = clean_text(str(value[key]))
                if cleaned:
                    names.append(cleaned)
                    break
        return names
    if isinstance(value, list):
        for item in value:
            names.extend(flatten_json_ld_names(item))
    return names


def unique_non_empty(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = clean_text(value)
        lowered = cleaned.lower()
        if not cleaned or lowered in seen:
            continue
        seen.add(lowered)
        unique.append(cleaned)
    return unique


def normalize_person_name(value: str) -> str:
    cleaned = clean_text(value)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.strip(" ,;|/&")
    return cleaned


def is_probable_person_name(value: str) -> bool:
    cleaned = normalize_person_name(value)
    if not cleaned:
        return False
    lowered = cleaned.lower()
    if lowered in GENERIC_AUTHOR_LABELS:
        return False
    if any(phrase in lowered for phrase in AUTHOR_LINE_BLOCKLIST_PHRASES):
        return False
    if cleaned.startswith("#"):
        return False

    without_title = re.sub(r"^(?:Dr\.|Mr\.|Ms\.|Mrs\.|Prof\.|Sri|Smt\.?)\s+", "", cleaned, flags=re.IGNORECASE)
    name_tokens = re.findall(r"[A-Z][A-Za-z.'-]+", without_title)
    return len(name_tokens) >= 2


def normalize_author_candidate_text(value: str) -> str:
    cleaned = normalize_person_name(value)
    cleaned = re.sub(r"^(?:by|written by)\s*[:\-]?\s+", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip(" ,;|/&")


def is_invalid_author_line(value: str) -> bool:
    cleaned = normalize_author_candidate_text(value)
    if not cleaned:
        return True
    lowered = cleaned.lower()
    if lowered in GENERIC_AUTHOR_LABELS:
        return True
    if any(phrase in lowered for phrase in AUTHOR_LINE_BLOCKLIST_PHRASES):
        return True
    if cleaned.startswith("#"):
        return True
    if " - " in cleaned:
        name, _detail = cleaned.split(" - ", 1)
        return not is_probable_person_name(name)
    return not is_probable_person_name(cleaned)


def unique_person_names(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned_value = normalize_person_name(value)
        if not cleaned_value:
            continue
        parts = [
            normalize_person_name(part)
            for part in re.split(r"\s*(?:,|&|\band\b)\s*", cleaned_value, flags=re.IGNORECASE)
            if normalize_person_name(part)
        ]
        for part in parts:
            lowered = part.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            unique.append(part)
    return unique


def strip_indian_express_title_prefix(value: str) -> str:
    cleaned = clean_text(value)
    if not cleaned:
        return ""

    # Indian Express article pages can prepend a section label such as
    # "Opinion" before the actual headline text.
    prefix_pattern = r"^(?:Opinion|Editorial|Column|Columns)\s*(?:(?::|\||/|-)\s*)?"
    previous = None
    while cleaned and cleaned != previous:
        previous = cleaned
        match = re.match(prefix_pattern, cleaned, flags=re.IGNORECASE)
        if not match:
            break
        remainder = clean_text(cleaned[match.end() :])
        first_ascii_alnum = next(
            (char for char in remainder if char.isascii() and char.isalnum()),
            "",
        )
        if not first_ascii_alnum or (not first_ascii_alnum.isupper() and not first_ascii_alnum.isdigit()):
            break
        cleaned = remainder
        cleaned = clean_text(cleaned)
    return cleaned


def normalize_article_title(value: str, article_url: str = "") -> str:
    cleaned = clean_text(value)
    host = (urlparse(article_url).netloc or "").lower()
    if host.endswith("indianexpress.com"):
        cleaned = strip_indian_express_title_prefix(cleaned)
    cleaned = re.sub(r"\s*[-|]\s*(?:The Hindu|The Indian Express)\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*(?:[-|:]\s*)?Premium\s*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s{2,}", " ", cleaned)
    return cleaned


def extract_author_lines(value: Any) -> list[str]:
    lines: list[str] = []
    if isinstance(value, str):
        for raw_segment in re.split(r"[\r\n]+", value):
            cleaned = normalize_author_candidate_text(raw_segment)
            if cleaned and not is_invalid_author_line(cleaned):
                lines.append(cleaned)
        return lines
    if isinstance(value, dict):
        name = normalize_person_name(first_non_empty(str(value.get("name", "")), str(value.get("headline", ""))))
        if not name or not is_probable_person_name(name):
            return lines
        description = clean_text(str(value.get("description", "")))
        job_title = clean_text(str(value.get("jobTitle", "")))
        affiliations = unique_non_empty(
            flatten_json_ld_names(value.get("affiliation")) + flatten_json_ld_names(value.get("worksFor"))
        )
        detail = ""
        if description:
            detail = description
        elif job_title and affiliations:
            detail = f"{job_title} at {'; '.join(affiliations)}"
        elif job_title:
            detail = job_title
        elif affiliations:
            detail = "; ".join(affiliations)
        if detail:
            candidate = f"{name} - {detail}"
            if not is_invalid_author_line(candidate):
                lines.append(candidate)
        else:
            if not is_invalid_author_line(name):
                lines.append(name)
        return lines
    if isinstance(value, list):
        for item in value:
            lines.extend(extract_author_lines(item))
    return lines


def unique_author_lines(values: list[str]) -> list[str]:
    merged: list[tuple[str, str, int]] = []
    index_by_name: dict[str, int] = {}

    for raw in values:
        cleaned = normalize_author_candidate_text(raw)
        if not cleaned or is_invalid_author_line(cleaned):
            continue
        if " - " in cleaned:
            name, detail = cleaned.split(" - ", 1)
            name_parts = [normalize_person_name(name)]
        else:
            name, detail = cleaned, ""
            name_parts = unique_person_names([name])
        normalized_detail = clean_text(detail)

        for normalized_name in name_parts:
            if not normalized_name:
                continue
            key = normalized_name.lower()
            existing_index = index_by_name.get(key)
            if existing_index is None:
                index_by_name[key] = len(merged)
                merged.append((normalized_name, normalized_detail, len(normalized_detail)))
                continue
            existing_name, existing_detail, existing_score = merged[existing_index]
            new_score = len(normalized_detail)
            if new_score > existing_score:
                merged[existing_index] = (normalized_name, normalized_detail, new_score)

    return [
        f"{name} - {detail}" if detail else name
        for name, detail, _ in merged
    ]


def extract_name_from_author_line(value: str) -> str:
    cleaned = normalize_author_candidate_text(value)
    if " - " in cleaned:
        cleaned = cleaned.split(" - ", 1)[0]
    return normalize_person_name(cleaned)


def extract_header_author_lines(soup: BeautifulSoup) -> list[str]:
    lines: list[str] = []
    selectors = [
        "[class*='byline' i]",
        "[class*='author' i]",
        "[data-testid*='author' i]",
        "[itemprop='author']",
        "[rel='author']",
    ]
    for selector in selectors:
        for node in soup.select(selector):
            text = clean_text(node.get_text(" ", strip=True))
            if not text or len(text) > 220:
                continue
            lines.extend(extract_author_lines(text))
    byline_pattern = re.compile(r"^\s*By\s*[:\-]?\s+(?P<name>.+?)\s*$", flags=re.IGNORECASE)
    for text_node in soup.find_all(string=byline_pattern):
        candidate = clean_text(str(text_node))
        match = byline_pattern.match(candidate)
        if not match:
            continue
        lines.extend(extract_author_lines(match.group("name")))
    return unique_author_lines(lines)


def is_article_tail_noise(paragraph: str) -> bool:
    lowered = clean_text(paragraph).lower()
    return any(phrase in lowered for phrase in ARTICLE_TAIL_NOISE_PHRASES)


def is_author_tail_annotation(sentence: str) -> bool:
    lowered = clean_text(sentence).strip(" .").lower()
    return (
        lowered.startswith("the views expressed are personal")
        or lowered.startswith("views expressed are personal")
        or lowered.startswith("both ")
        or lowered.startswith("all authors ")
        or lowered.startswith("together ")
    )


def parse_author_bio_sentence(sentence: str, known_author_names: list[str]) -> str | None:
    cleaned = clean_text(sentence).strip(" .")
    if not cleaned:
        return None

    if cleaned.lower().startswith("moderated by "):
        name = normalize_person_name(cleaned[len("moderated by ") :])
        return f"{name} - Moderator" if name else None

    candidate_names = sorted(
        [name for name in unique_person_names(known_author_names) if is_probable_person_name(name)],
        key=len,
        reverse=True,
    )
    writer_match = re.match(r"^(?:the\s+writer|the\s+author)\s+(?P<detail>(?:is|was)\s+.+)$", cleaned, flags=re.IGNORECASE)
    if writer_match and len(candidate_names) == 1:
        detail = clean_text(writer_match.group("detail"))
        detail = re.sub(r"^(?:is|was)\s+", "", detail, flags=re.IGNORECASE)
        return f"{candidate_names[0]} - {detail}" if detail else candidate_names[0]

    for name in candidate_names:
        lowered_name = name.lower()
        if not cleaned.lower().startswith(f"{lowered_name} "):
            continue
        detail = cleaned[len(name) :].strip(" ,;:-")
        detail = re.sub(r"^(?:is|was)\s+", "", detail, flags=re.IGNORECASE)
        detail = clean_text(detail)
        return f"{name} - {detail}" if detail else name

    generic_match = re.match(
        r"^(?P<name>(?:[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,5}))\s+(?P<detail>(?:is|was|teaches|works(?: on)?|serves(?: as)?|served(?: as)?|writes|heads|head[s]?|leads|lead[s]?|moderates?|moderated).+)$",
        cleaned,
    )
    if not generic_match:
        return None

    name = normalize_person_name(generic_match.group("name"))
    detail = clean_text(generic_match.group("detail"))
    detail = re.sub(r"^(?:is|was)\s+", "", detail, flags=re.IGNORECASE)
    return f"{name} - {detail}" if name and detail else name or None


def split_author_bio_sentences(paragraph: str) -> list[str]:
    protected = clean_text(paragraph)
    if not protected:
        return []

    for title in AUTHOR_BIO_ABBREVIATIONS:
        protected = protected.replace(title, title.replace(".", "<DOT>"))

    protected = re.sub(r"\b([A-Z])\.", r"\1<DOT>", protected)
    return [
        clean_text(part.replace("<DOT>", "."))
        for part in re.split(r"(?<=[.?!])\s+", protected)
        if clean_text(part.replace("<DOT>", "."))
    ]


def extract_author_bios_from_body_tail(body: str, known_author_names: list[str]) -> tuple[str, list[str]]:
    paragraphs = [clean_text(paragraph) for paragraph in body.split("\n\n") if clean_text(paragraph)]
    extracted_lines: list[str] = []

    while paragraphs and is_article_tail_noise(paragraphs[-1]):
        paragraphs.pop()

    while paragraphs:
        candidate = paragraphs[-1]
        sentences = split_author_bio_sentences(candidate)
        parsed_lines = [parse_author_bio_sentence(sentence, known_author_names) for sentence in sentences]
        valid_lines = [line for line in parsed_lines if line]
        if not valid_lines:
            break
        invalid_index = next((index for index, line in enumerate(parsed_lines) if not line), len(parsed_lines))
        trailing_sentences = sentences[invalid_index:]
        has_valid_after_invalid = any(parsed_lines[index] for index in range(invalid_index, len(parsed_lines)))
        if has_valid_after_invalid:
            break
        if trailing_sentences and not all(is_author_tail_annotation(sentence) for sentence in trailing_sentences):
            break
        extracted_lines = [line for line in parsed_lines[:invalid_index] if line] + extracted_lines
        paragraphs.pop()

    return "\n\n".join(paragraphs), unique_author_lines(extracted_lines)


def extract_moderator_line_from_body_intro(body: str) -> str | None:
    paragraphs = [clean_text(paragraph) for paragraph in body.split("\n\n") if clean_text(paragraph)]
    for paragraph in paragraphs[:3]:
        match = re.search(
            r"\bmoderated by (?P<name>(?:[A-Z][A-Za-z.'-]+(?:\s+[A-Z][A-Za-z.'-]+){1,5}))",
            paragraph,
        )
        if not match:
            continue
        name = normalize_person_name(match.group("name"))
        if name:
            return f"{name} - Moderator"
    return None


def mojibake_score(text: str) -> int:
    return sum(text.count(marker) for marker in MOJIBAKE_MARKERS)


def clean_text(value: str | None) -> str:
    text = str(value or "")
    if not text:
        return ""

    best = text
    best_score = mojibake_score(best)
    try:
        repaired = text.encode("latin-1").decode("utf-8")
        repaired_score = mojibake_score(repaired)
        if repaired_score < best_score:
            best = repaired
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass

    for source, target in COMMON_MOJIBAKE_REPLACEMENTS.items():
        best = best.replace(source, target)

    best = best.translate(ZERO_WIDTH_TRANSLATION).replace("\xa0", " ")
    best = re.sub(r"[ \t]+\n", "\n", best)
    best = re.sub(r"\n{3,}", "\n\n", best)
    best = re.sub(r" {2,}", " ", best)
    return best.strip()


def trim_to_sentence_or_clause(text: str, max_chars: int) -> str:
    cleaned = clean_text(text)
    if len(cleaned) <= max_chars:
        return cleaned

    window = cleaned[: max_chars + 1].rstrip()
    punctuation_positions = [window.rfind(token) for token in (". ", "! ", "? ", "; ", ": ")]
    punctuation_positions += [window.rfind(token) for token in (".", "!", "?", ";", ":")]
    best_position = max(punctuation_positions)
    if best_position >= int(max_chars * 0.6):
        trimmed = window[: best_position + 1].rstrip()
        return trimmed

    last_space = window.rfind(" ")
    if last_space >= int(max_chars * 0.6):
        trimmed = window[:last_space].rstrip(",;:- ")
    else:
        trimmed = window.rstrip(",;:- ")

    if trimmed.endswith((".", "!", "?")):
        return trimmed
    return f"{trimmed}."


def truncate_words(text: str, max_words: int) -> str:
    cleaned = clean_text(text)
    words = cleaned.split()
    if len(words) <= max_words:
        return cleaned

    sentences = [item.strip() for item in re.split(r"(?<=[.!?])\s+", cleaned) if item.strip()]
    selected: list[str] = []
    word_count = 0
    for sentence in sentences:
        sentence_words = len(sentence.split())
        if not selected and sentence_words > max_words:
            return trim_to_sentence_or_clause(sentence, max_chars=max(160, max_words * 8))
        if word_count + sentence_words > max_words:
            break
        selected.append(sentence)
        word_count += sentence_words

    if selected:
        return " ".join(selected).strip()

    truncated = " ".join(words[:max_words]).rstrip(",;:")
    if truncated.endswith((".", "!", "?")):
        return truncated
    return f"{truncated}."


def pdf_highlight_terms_from_title(title: str) -> list[str]:
    terms: list[str] = []
    for token in re.findall(r"[A-Za-z][A-Za-z'.-]+", clean_text(title)):
        normalized = token.strip(" .,:;!?()[]{}\"'")
        lowered = normalized.lower()
        if not normalized:
            continue
        if len(normalized) < 4 and not normalized.isupper():
            continue
        if lowered in PDF_HIGHLIGHT_STOPWORDS:
            continue
        if normalized not in terms:
            terms.append(normalized)
    return sorted(terms, key=len, reverse=True)[:8]


def normalize_highlight_phrases(values: list[str] | None) -> list[str]:
    phrases: list[str] = []
    for value in values or []:
        cleaned = clean_text(value).strip(" \t\r\n.,;:!?-")
        if not cleaned or len(cleaned) < 3:
            continue
        lowered = cleaned.lower()
        if any(lowered == existing.lower() for existing in phrases):
            continue
        if any(lowered in existing.lower() for existing in phrases):
            continue
        phrases = [existing for existing in phrases if existing.lower() not in lowered]
        phrases.append(cleaned)
    return sorted(phrases, key=len, reverse=True)[:6]


def sanitize_pdf_plain_text(value: str | None) -> str:
    text = clean_text(value)
    replacements = {
        "₹": "Rs. ",
        "•": "- ",
        "–": "-",
        "—": "-",
        "−": "-",
        "’": "'",
        "‘": "'",
        "“": '"',
        "”": '"',
        "…": "...",
        "\u00ad": "",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)

    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def apply_pdf_bold_limit(value: str, max_segments: int) -> str:
    if max_segments <= 0:
        return re.sub(r"PDFBOLDSTARTTOKEN(.*?)PDFBOLDENDTOKEN", r"\1", value, flags=re.DOTALL)

    seen_segments = 0

    def replace_match(match: re.Match[str]) -> str:
        nonlocal seen_segments
        seen_segments += 1
        content = match.group(1).strip()
        if not content:
            return ""
        if seen_segments <= max_segments:
            return f"<b>{content}</b>"
        return content

    return re.sub(r"PDFBOLDSTARTTOKEN(.*?)PDFBOLDENDTOKEN", replace_match, value, flags=re.DOTALL)


def sanitize_pdf_markup(value: str | None, max_bold_segments: int = 0) -> str:
    raw = str(value or "")
    if not raw:
        return ""

    normalized = re.sub(r"(?is)<\s*strong\s*>", "<b>", raw)
    normalized = re.sub(r"(?is)<\s*/\s*strong\s*>", "</b>", normalized)
    normalized = re.sub(r"(?is)<\s*b\s*>", "PDFBOLDSTARTTOKEN", normalized)
    normalized = re.sub(r"(?is)<\s*/\s*b\s*>", "PDFBOLDENDTOKEN", normalized)
    normalized = re.sub(r"(?is)<\s*br\s*/?\s*>", "\n", normalized)
    normalized = re.sub(r"(?is)<[^>]+>", "", normalized)

    cleaned = sanitize_pdf_plain_text(normalized)
    escaped = html.escape(cleaned, quote=False)
    return apply_pdf_bold_limit(escaped, max_bold_segments)


def format_pdf_emphasis(text: str, highlight_phrases: list[str] | None = None, title: str = "") -> str:
    cleaned_text = clean_text(text)
    if not cleaned_text:
        return ""

    phrases = normalize_highlight_phrases(highlight_phrases)
    if not phrases:
        phrases = pdf_highlight_terms_from_title(title)

    marked_text = cleaned_text
    replacement_tokens: list[tuple[str, str]] = []
    for index, phrase in enumerate(phrases):
        start_token = f"PDFBOLDSTART{index}TOKEN"
        end_token = f"PDFBOLDEND{index}TOKEN"
        replacement_tokens.append((start_token, end_token))
        prefix = r"(?<!\w)" if phrase[:1].isalnum() else ""
        suffix = r"(?!\w)" if phrase[-1:].isalnum() else ""
        pattern = re.compile(f"{prefix}{re.escape(phrase)}{suffix}", flags=re.IGNORECASE)
        marked_text = pattern.sub(lambda match: f"{start_token}{match.group(0)}{end_token}", marked_text)

    escaped = html.escape(marked_text, quote=False)
    for start_token, end_token in replacement_tokens:
        escaped = escaped.replace(start_token, "<b>").replace(end_token, "</b>")
    return escaped


def decode_response_text(response: requests.Response) -> str:
    raw = response.content or b""
    candidates: list[str] = []

    if response.text:
        candidates.append(response.text)

    for encoding in ("utf-8", response.encoding, getattr(response, "apparent_encoding", None)):
        if not encoding:
            continue
        try:
            decoded = raw.decode(encoding, errors="replace")
        except (LookupError, UnicodeDecodeError):
            continue
        candidates.append(decoded)

    best = ""
    best_score: tuple[int, int] | None = None
    for candidate in candidates:
        cleaned = clean_text(candidate)
        score = (mojibake_score(cleaned), cleaned.count("\ufffd"))
        if best_score is None or score < best_score:
            best = cleaned
            best_score = score

    return best


def format_report_date(target_date: str) -> str:
    parsed = datetime.strptime(target_date, "%Y-%m-%d")
    return f"{parsed.strftime('%B')} {parsed.day}, {parsed.year}"


def format_header_date(target_date: str) -> str:
    parsed = datetime.strptime(target_date, "%Y-%m-%d")
    return parsed.strftime("%d-%m-%Y")


def build_editorial_window(target_date: str, timezone_name: str) -> tuple[datetime, datetime]:
    tz_info = get_tzinfo(timezone_name)
    target_day = datetime.strptime(target_date, "%Y-%m-%d").date()
    window_end = datetime.combine(target_day, dt_time(hour=DATE_WINDOW_CUTOFF_HOUR), tzinfo=tz_info)
    window_start = window_end - timedelta(days=1)
    return window_start, window_end


def previous_calendar_date(target_date: str) -> str:
    target_day = datetime.strptime(target_date, "%Y-%m-%d").date()
    return (target_day - timedelta(days=1)).isoformat()


def infer_editorial_datetime(editorial: Editorial, timezone_name: str) -> datetime | None:
    parsed_dt = parse_datetime(editorial.published_at, timezone_name)
    if parsed_dt:
        return parsed_dt

    if not editorial.editorial_date:
        return None

    try:
        parsed_date = datetime.strptime(editorial.editorial_date, "%Y-%m-%d").date()
    except ValueError:
        return None

    # When only the calendar date is available, place the article in the middle
    # of the day so date-window checks can still include likely matches.
    return datetime.combine(parsed_date, dt_time(hour=12), tzinfo=get_tzinfo(timezone_name))


def is_editorial_in_window(editorial: Editorial, target_date: str, timezone_name: str) -> bool:
    editorial_dt = infer_editorial_datetime(editorial, timezone_name)
    if not editorial_dt:
        return False
    window_start, window_end = build_editorial_window(target_date, timezone_name)
    return window_start <= editorial_dt < window_end


def load_sent_article_urls(output_root: Path, target_date: str) -> set[str]:
    previous_date = previous_calendar_date(target_date)
    payload_path = output_root / previous_date / "editorials.json"
    if not payload_path.exists():
        return set()

    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logging.warning("Failed to read previous editorial payload %s: %s", payload_path, exc)
        return set()

    items = payload.get("items")
    if not isinstance(items, list):
        return set()

    urls: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        normalized = normalize_editorial_url(str(item.get("url", "")).strip())
        if normalized:
            urls.add(normalized)
    return urls


def source_name_from_url(url: str) -> str:
    host = (urlparse(url).netloc or "").lower()
    if host.endswith("thehindu.com"):
        return "The Hindu"
    if host.endswith("indianexpress.com"):
        return "Indian Express"
    if host.endswith("thehindubusinessline.com"):
        return "BusinessLine"
    if host.startswith("www."):
        host = host[4:]
    host = host.split(":")[0]
    if not host:
        return ""
    label = host.split(".")[0]
    return " ".join(part.capitalize() for part in re.split(r"[-_]+", label) if part)


def format_author_details(editorial: Editorial) -> list[str]:
    if editorial.author_lines:
        return [clean_text(line) for line in editorial.author_lines if clean_text(line)]
    if clean_text(editorial.author):
        return [clean_text(editorial.author)]
    return []


def editorial_to_payload(editorial: Editorial) -> dict[str, Any]:
    payload = asdict(editorial)
    for key in (
        "highlight_phrases",
        "summary_markup",
        "key_takeaway_markup",
        "implications_markup",
        "tone_markup",
    ):
        payload.pop(key, None)
    return payload


def filter_author_lines_to_candidates(author_lines: list[str], candidate_author_lines: list[str]) -> list[str]:
    candidate_names = {
        extract_name_from_author_line(line).lower()
        for line in candidate_author_lines
        if extract_name_from_author_line(line)
    }
    if not candidate_names:
        return []

    filtered: list[str] = []
    for line in author_lines:
        name = extract_name_from_author_line(line).lower()
        if not name or name not in candidate_names:
            continue
        filtered.append(line)
    return unique_author_lines(filtered)


def report_section_name_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = re.sub(r"/{2,}", "/", parsed.path).rstrip("/").lower()

    if host.endswith("thehindu.com") and path.startswith("/opinion/"):
        if path.startswith("/opinion/lead/"):
            return "Lead"
        if path.startswith("/opinion/editorial/"):
            return "Editorial"
        if path.startswith("/opinion/op-ed/"):
            return "Op-Ed"
        return "Opinion"

    if host.endswith("indianexpress.com"):
        if path.startswith("/article/opinion/editorials/"):
            return "Editorial"
        if path.startswith("/article/opinion/columns/"):
            return "Column"
        return "Opinion"

    return clean_text(source_name_from_url(url)) or "Other Sources"


def report_section_sort_key(section_name: str) -> tuple[int, int, str]:
    if section_name in PDF_SECTION_ORDER:
        return (0, PDF_SECTION_ORDER[section_name], section_name.lower())
    return (1, 999, section_name.lower())


def editorial_report_sort_key(editorial: Editorial) -> tuple[int, int, str]:
    section_name = report_section_name_from_url(editorial.url)
    return (
        report_section_sort_key(section_name)[1],
        0 if editorial.published_at else 1,
        editorial.published_at or "",
    )


def build_key_takeaway(summary: str, key_points: list[str] | None) -> str:
    cleaned_points = [clean_text(point) for point in (key_points or []) if clean_text(point)]
    if cleaned_points:
        first_point = re.split(r"(?<=[.!?])\s+", cleaned_points[0].strip())[0].strip()
        return trim_to_sentence_or_clause(first_point, KEY_TAKEAWAY_CHAR_LIMIT)

    sentences = [item.strip() for item in re.split(r"(?<=[.!?])\s+", clean_text(summary)) if item.strip()]
    return trim_to_sentence_or_clause(clean_text(sentences[0] if sentences else summary), KEY_TAKEAWAY_CHAR_LIMIT)


def target_daily_takeaway_count(editorials: list[Editorial]) -> int:
    editorial_count = len(editorials)
    if editorial_count <= 4:
        return 3
    if editorial_count <= 8:
        return 4
    return 5


def normalize_takeaway_point(text: str) -> str:
    cleaned = clean_text(text)
    cleaned = re.sub(r"^\s*(?:[-*]|\u2022|\d+[.)])\s*", "", cleaned)
    return trim_to_sentence_or_clause(cleaned, 240).strip()


def takeaway_keywords(text: str) -> set[str]:
    words = re.findall(r"[a-z]{4,}", normalize_takeaway_point(text).lower())
    return {word for word in words if word not in DAILY_TAKEAWAY_STOPWORDS}


def takeaway_points_are_similar(first: str, second: str) -> bool:
    first_normalized = normalize_takeaway_point(first).lower()
    second_normalized = normalize_takeaway_point(second).lower()
    if not first_normalized or not second_normalized:
        return False
    if first_normalized == second_normalized:
        return True
    if first_normalized in second_normalized or second_normalized in first_normalized:
        return True

    first_keywords = takeaway_keywords(first_normalized)
    second_keywords = takeaway_keywords(second_normalized)
    if not first_keywords or not second_keywords:
        return False

    overlap = first_keywords & second_keywords
    union = first_keywords | second_keywords
    if len(overlap) >= 3:
        return True
    return (len(overlap) / max(1, len(union))) >= 0.45


def build_daily_takeaways(editorials: list[Editorial], point_count: int | None = None) -> list[str]:
    target_count = point_count or target_daily_takeaway_count(editorials)
    candidates: list[str] = []

    for editorial in editorials:
        candidates.extend(
            [
                *(editorial.key_points or []),
                build_key_takeaway(editorial.summary, editorial.key_points),
                editorial.implications,
            ]
        )

    for editorial in editorials:
        sentences = [
            normalize_takeaway_point(item)
            for item in re.split(r"(?<=[.!?])\s+", clean_text(editorial.summary))
            if normalize_takeaway_point(item)
        ]
        candidates.extend(sentences[:2])

    points = clean_takeaway_points(candidates)[:target_count]
    if len(points) >= target_count:
        return points

    for editorial in editorials:
        fallback_point = normalize_takeaway_point(f"{clean_text(editorial.title)} remains one of the day's key opinion themes.")
        if not fallback_point:
            continue
        points = clean_takeaway_points([*points, fallback_point])[:target_count]
        if len(points) >= target_count:
            return points

    return points


def clean_takeaway_points(points: list[str]) -> list[str]:
    cleaned_points: list[str] = []
    for point in points:
        normalized = normalize_takeaway_point(point)
        if not normalized:
            continue
        if any(takeaway_points_are_similar(normalized, existing) for existing in cleaned_points):
            continue
        cleaned_points.append(normalized)
    return cleaned_points


def group_editorials_by_source(editorials: list[Editorial]) -> list[tuple[str, list[Editorial]]]:
    grouped: dict[str, list[Editorial]] = {}
    for editorial in editorials:
        source_name = clean_text(source_name_from_url(editorial.url)) or "Other Sources"
        grouped.setdefault(source_name, []).append(editorial)
    ordered_sources = sorted(
        grouped,
        key=lambda source_name: (
            SOURCE_SECTION_ORDER.get(source_name, 999),
            source_name.lower(),
        ),
    )
    return [
        (source_name, sorted(grouped[source_name], key=editorial_report_sort_key))
        for source_name in ordered_sources
    ]


def extract_form_fields(form: BeautifulSoup) -> tuple[str | None, str | None, dict[str, str]]:
    username_key = None
    password_key = None
    payload: dict[str, str] = {}
    user_key_candidates = {"email", "username", "user", "loginid", "userid"}

    for input_tag in form.find_all("input"):
        name = (input_tag.get("name") or "").strip()
        input_type = (input_tag.get("type") or "").lower()
        value = (input_tag.get("value") or "").strip()
        if not name:
            continue
        if input_type == "hidden":
            payload[name] = value
        if input_type == "password" and not password_key:
            password_key = name
        if name.lower() in user_key_candidates and not username_key:
            username_key = name
        if input_type in {"email", "text"} and not username_key:
            username_key = name

    return username_key, password_key, payload


def authenticate_with_form(
    session: requests.Session,
    login_urls: list[str],
    username: str,
    password: str,
    source_label: str,
) -> bool:
    headers = {"User-Agent": "Mozilla/5.0 (EditorialPipeline/1.0)"}
    for login_url in login_urls:
        try:
            get_resp = session.get(login_url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            get_resp.raise_for_status()
            soup = BeautifulSoup(get_resp.text, "html.parser")
            form = soup.find("form")
            if not form:
                continue

            username_key, password_key, payload = extract_form_fields(form)
            if not username_key or not password_key:
                continue

            payload[username_key] = username
            payload[password_key] = password
            action = form.get("action") or login_url
            post_url = urljoin(login_url, action)
            post_resp = session.post(
                post_url,
                data=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
                allow_redirects=True,
            )
            if post_resp.status_code < 400:
                logging.info("%s form login attempt completed against %s", source_label, login_url)
                return True
        except Exception as exc:
            logging.warning("%s form login attempt failed for %s: %s", source_label, login_url, exc)
    return False


def try_click_first(page, selectors: list[str]) -> bool:
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if locator.count() > 0 and locator.is_visible():
                locator.click(timeout=2000)
                return True
        except Exception:
            continue
    return False


def try_fill_first(page, selectors: list[str], value: str) -> bool:
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if locator.count() > 0 and locator.is_visible():
                locator.fill(value, timeout=2000)
                return True
        except Exception:
            continue
    return False


def parse_csv_env(name: str) -> list[str]:
    raw = os.getenv(name, "")
    if not raw.strip():
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def all_playwright_contexts(page: Any) -> list[Any]:
    contexts = [page]
    for frame in page.frames:
        if frame != page.main_frame:
            contexts.append(frame)
    return contexts


def try_click_anywhere(page: Any, selectors: list[str]) -> bool:
    for target in all_playwright_contexts(page):
        if try_click_first(target, selectors):
            return True
    return False


def try_fill_login_anywhere(
    page: Any,
    username: str,
    password: str,
    email_selectors: list[str],
    password_selectors: list[str],
    submit_selectors: list[str],
) -> bool:
    for target in all_playwright_contexts(page):
        email_ok = try_fill_first(target, email_selectors, username)
        pwd_ok = try_fill_first(target, password_selectors, password)
        if email_ok and pwd_ok:
            if not try_click_first(target, submit_selectors):
                try:
                    page.keyboard.press("Enter")
                except Exception:
                    pass
            return True
    return False


def browser_selector_sets(selector_env_prefix: str) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    sign_in_selectors = [
        "a:has-text('Sign In')",
        "a:has-text('SIGN IN')",
        "button:has-text('Sign In')",
        "a:has-text('Login')",
        "button:has-text('Login')",
        "a:has-text('Log in')",
        "button:has-text('Log in')",
        "[data-testid*='login']",
        "[class*='login' i]",
        "[id*='login' i]",
    ]
    email_selectors = [
        "input[type='email']",
        "input[name*='email' i]",
        "input[id*='email' i]",
        "input[name*='user' i]",
        "input[id*='user' i]",
    ]
    password_selectors = [
        "input[type='password']",
        "input[name*='password' i]",
        "input[id*='password' i]",
    ]
    submit_selectors = [
        "button[type='submit']",
        "input[type='submit']",
        "button:has-text('Continue')",
        "button:has-text('Login')",
        "button:has-text('Sign In')",
        "button:has-text('Log in')",
    ]
    cookie_accept_selectors = [
        "button:has-text('Accept')",
        "button:has-text('I Agree')",
        "button:has-text('Got it')",
        "[id*='accept' i]",
        "[class*='accept' i]",
    ]
    sign_in_selectors.extend(parse_csv_env(f"{selector_env_prefix}_SIGNIN_SELECTORS"))
    email_selectors.extend(parse_csv_env(f"{selector_env_prefix}_EMAIL_SELECTORS"))
    password_selectors.extend(parse_csv_env(f"{selector_env_prefix}_PASSWORD_SELECTORS"))
    submit_selectors.extend(parse_csv_env(f"{selector_env_prefix}_SUBMIT_SELECTORS"))
    return (
        sign_in_selectors,
        email_selectors,
        password_selectors,
        submit_selectors,
        cookie_accept_selectors,
    )


def try_click_keywords(target: Any, keywords: list[str]) -> bool:
    script = """
keywords => {
  const loweredKeywords = keywords.map(item => String(item).toLowerCase());
  const matches = [];
  const isVisible = (el) => {
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style && style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
  };
  const visit = (root) => {
    const elements = root.querySelectorAll('*');
    for (const el of elements) {
      if (el.shadowRoot) {
        visit(el.shadowRoot);
      }
      const textBits = [
        el.innerText || '',
        el.textContent || '',
        el.getAttribute('aria-label') || '',
        el.getAttribute('title') || '',
        el.getAttribute('role') || '',
        el.getAttribute('id') || '',
        el.getAttribute('class') || '',
      ].join(' ').toLowerCase();
      const score = loweredKeywords.reduce((total, keyword) => total + (textBits.includes(keyword) ? 1 : 0), 0);
      if (score > 0 && isVisible(el)) {
        matches.push({ el, score });
      }
    }
  };
  visit(document);
  matches.sort((a, b) => b.score - a.score);
  for (const match of matches) {
    try {
      match.el.scrollIntoView({ block: 'center', inline: 'center' });
      match.el.click();
      return true;
    } catch (error) {
      continue;
    }
  }
  return false;
}
"""
    try:
        return bool(target.evaluate(script, keywords))
    except Exception:
        return False


def try_click_keywords_anywhere(page: Any, keywords: list[str]) -> bool:
    for target in all_playwright_contexts(page):
        if try_click_keywords(target, keywords):
            return True
    return False


def read_page_text(target: Any) -> str:
    try:
        return str(target.evaluate("() => (document.body ? document.body.innerText : '')"))[:4000]
    except Exception:
        return ""


def apply_browser_cookies_to_session(session: requests.Session, cookies: list[dict[str, Any]]) -> None:
    for cookie in cookies:
        name = cookie.get("name")
        value = cookie.get("value")
        if not name:
            continue
        session.cookies.set(
            name=name,
            value=value or "",
            domain=cookie.get("domain"),
            path=cookie.get("path", "/"),
        )


def authenticate_with_browser(
    session: requests.Session,
    editorial_urls: list[str],
    login_urls: list[str],
    username: str,
    password: str,
    browser_headless: bool,
    browser_timeout_seconds: int,
    interactive_login_wait_seconds: int,
    storage_state_path_value: str,
    selector_env_prefix: str,
    source_label: str,
) -> bool:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        logging.warning(
            "Playwright not available for browser login (%s). Install with: pip install playwright && playwright install chromium",
            exc,
        )
        return False

    timeout_ms = max(5000, browser_timeout_seconds * 1000)
    (
        sign_in_selectors,
        email_selectors,
        password_selectors,
        submit_selectors,
        cookie_accept_selectors,
    ) = browser_selector_sets(selector_env_prefix)

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=browser_headless)
            context_args: dict[str, Any] = {}
            storage_state_path = Path(storage_state_path_value).resolve()
            if storage_state_path.exists():
                context_args["storage_state"] = str(storage_state_path)
                logging.info("Loaded %s browser storage state from %s", source_label, storage_state_path)

            context = browser.new_context(**context_args)
            page = context.new_page()
            visited_urls: list[str] = []
            for candidate in [*editorial_urls, *login_urls]:
                if candidate and candidate not in visited_urls:
                    visited_urls.append(candidate)

            auto_submit_done = False
            for candidate_url in visited_urls:
                try:
                    page.goto(candidate_url, wait_until="domcontentloaded", timeout=timeout_ms)
                except Exception:
                    continue
                try_click_anywhere(page, cookie_accept_selectors)
                try_click_anywhere(page, sign_in_selectors)
                page.wait_for_timeout(1200)
                if try_fill_login_anywhere(
                    page,
                    username,
                    password,
                    email_selectors,
                    password_selectors,
                    submit_selectors,
                ):
                    auto_submit_done = True
                    page.wait_for_timeout(3000)
                    break

            manual_wait_used = False
            if not auto_submit_done:
                if (not browser_headless) and interactive_login_wait_seconds > 0:
                    manual_wait_used = True
                    logging.info(
                        "Automatic field detection failed. Complete %s login manually in the opened browser within %d second(s).",
                        source_label,
                        interactive_login_wait_seconds,
                    )
                    try:
                        target_url = login_urls[0] if login_urls else (editorial_urls[0] if editorial_urls else "")
                        if target_url:
                            page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
                    except Exception:
                        pass
                    page.wait_for_timeout(interactive_login_wait_seconds * 1000)
                else:
                    logging.warning("%s browser login form fields were not found.", source_label)

            for candidate_url in editorial_urls:
                try:
                    page.goto(candidate_url, wait_until="domcontentloaded", timeout=timeout_ms)
                    break
                except Exception:
                    continue
            cookies = context.cookies()

            try:
                storage_state_path.parent.mkdir(parents=True, exist_ok=True)
                context.storage_state(path=str(storage_state_path))
                logging.info("Saved %s browser storage state to %s", source_label, storage_state_path)
            except Exception as exc:
                logging.warning("Could not save %s browser storage state: %s", source_label, exc)

            browser.close()

            if not cookies:
                logging.warning("%s browser login completed but no cookies were captured.", source_label)
                return False

            apply_browser_cookies_to_session(session, cookies)
            if auto_submit_done or manual_wait_used or context_args.get("storage_state"):
                logging.info("%s browser login flow completed with %d cookies.", source_label, len(cookies))
                return True

            cookie_names = {str(cookie.get("name", "")).lower() for cookie in cookies}
            likely_auth = any(
                token in name
                for name in cookie_names
                for token in ("auth", "token", "sso", "member", "login", "user")
            )
            if likely_auth:
                logging.info("%s browser login likely succeeded based on auth cookie names.", source_label)
                return True
            return False
    except Exception as exc:
        logging.warning("%s browser login failed: %s", source_label, exc)
        return False


def authenticate_hindu(session: requests.Session, config: PipelineConfig) -> bool:
    if not config.hindu_username or not config.hindu_password:
        logging.info("Hindu credentials missing. Continuing without login.")
        return False

    if config.hindu_use_browser_login:
        if authenticate_with_browser(
            session=session,
            editorial_urls=config.hindu_editorial_urls,
            login_urls=config.hindu_login_urls,
            username=config.hindu_username,
            password=config.hindu_password,
            browser_headless=config.hindu_browser_headless,
            browser_timeout_seconds=config.hindu_browser_timeout_seconds,
            interactive_login_wait_seconds=config.hindu_interactive_login_wait_seconds,
            storage_state_path_value=config.hindu_storage_state_path,
            selector_env_prefix="HINDU",
            source_label="The Hindu",
        ):
            return True
        logging.warning("Browser login did not succeed; trying form-based login.")

    if authenticate_with_form(
        session=session,
        login_urls=config.hindu_login_urls,
        username=config.hindu_username,
        password=config.hindu_password,
        source_label="The Hindu",
    ):
        return True

    logging.warning("Could not confirm login. Continuing without authenticated session.")
    return False


def authenticate_indian_express(session: requests.Session, config: PipelineConfig) -> bool:
    if not config.indian_express_username or not config.indian_express_password:
        logging.info("Indian Express credentials missing. Continuing without login.")
        return False

    if config.indian_express_use_browser_login:
        if authenticate_with_browser(
            session=session,
            editorial_urls=config.indian_express_editorial_urls,
            login_urls=config.indian_express_login_urls,
            username=config.indian_express_username,
            password=config.indian_express_password,
            browser_headless=config.indian_express_browser_headless,
            browser_timeout_seconds=config.indian_express_browser_timeout_seconds,
            interactive_login_wait_seconds=config.indian_express_interactive_login_wait_seconds,
            storage_state_path_value=config.indian_express_storage_state_path,
            selector_env_prefix="INDIAN_EXPRESS",
            source_label="Indian Express",
        ):
            return True
        logging.warning("Indian Express browser login did not succeed; trying form-based login.")

    if authenticate_with_form(
        session=session,
        login_urls=config.indian_express_login_urls,
        username=config.indian_express_username,
        password=config.indian_express_password,
        source_label="Indian Express",
    ):
        return True

    logging.warning("Could not confirm Indian Express login. Continuing without authenticated session.")
    return False


def build_paginated_section_urls(editorial_url: str) -> list[str]:
    urls = [editorial_url]
    parsed = urlparse(editorial_url)
    host = (parsed.netloc or "").lower()
    normalized_path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if not host.endswith("indianexpress.com") or not normalized_path.startswith("/section/"):
        return urls

    clean_path = normalized_path.rstrip("/") or "/"
    for page_number in range(2, INDIAN_EXPRESS_SECTION_MAX_PAGES + 1):
        paginated_path = f"{clean_path}/page/{page_number}/"
        paginated_url = parsed._replace(path=paginated_path).geturl()
        urls.append(paginated_url)
    return dedupe_preserve_order(urls)


def fetch_editorial_links(session: requests.Session, editorial_urls: list[str]) -> list[str]:
    links: set[str] = set()
    for editorial_url in editorial_urls:
        empty_page_streak = 0
        for candidate_url in build_paginated_section_urls(editorial_url):
            try:
                response = session.get(
                    candidate_url,
                    headers={"User-Agent": "Mozilla/5.0 (EditorialPipeline/1.0)"},
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                soup = BeautifulSoup(decode_response_text(response), "html.parser")

                before_count = len(links)
                for anchor in soup.select("a[href]"):
                    href = anchor.get("href")
                    if not href:
                        continue
                    absolute = urljoin(candidate_url, href)
                    normalized = normalize_editorial_url(absolute)
                    if normalized:
                        links.add(normalized)
                collected_count = len(links) - before_count
                logging.info(
                    "Collected %d opinion article link(s) from %s.",
                    collected_count,
                    candidate_url,
                )
                if collected_count == 0:
                    empty_page_streak += 1
                    if empty_page_streak >= 2:
                        break
                else:
                    empty_page_streak = 0
            except Exception as exc:
                logging.warning("Failed to collect links from %s: %s", candidate_url, exc)
                break
    return sorted(links)


def is_article_body_noise(paragraph: str) -> bool:
    lowered = clean_text(paragraph).lower()
    if not lowered:
        return True
    if any(lowered.startswith(prefix) for prefix in ARTICLE_BODY_NOISE_PREFIXES):
        return True
    if lowered.startswith("the views expressed are personal"):
        return True
    if paragraph.count(" / ") >= 4:
        return True
    return False


def extract_article_text(soup: BeautifulSoup) -> str:
    selectors = [
        "div.articlebodycontent p",
        "div#content-body p",
        "div#article-body p",
        "article p",
        "main p",
    ]
    for selector in selectors:
        paragraphs = []
        for item in soup.select(selector):
            text = item.get_text(" ", strip=True)
            if len(text) >= 40 and not is_article_body_noise(text):
                paragraphs.append(text)
        if paragraphs:
            deduped: list[str] = []
            seen: set[str] = set()
            for paragraph in paragraphs:
                if paragraph in seen:
                    continue
                seen.add(paragraph)
                deduped.append(paragraph)
            return "\n\n".join(deduped)
    return ""


def extract_print_view_url(soup: BeautifulSoup, base_url: str) -> str | None:
    candidates: list[str] = []
    amp_tag = soup.select_one("link[rel='amphtml']")
    if amp_tag and amp_tag.get("href"):
        candidates.append(urljoin(base_url, amp_tag["href"]))

    for anchor in soup.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        text = anchor.get_text(" ", strip=True).lower()
        href_lower = href.lower()
        if "print" in href_lower or "print" in text:
            candidates.append(urljoin(base_url, href))

    seen: set[str] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        parsed = urlparse(candidate)
        host = (parsed.netloc or "").lower()
        if parsed.scheme in {"http", "https"} and (
            host.endswith("thehindu.com") or host.endswith("indianexpress.com")
        ):
            return candidate
    return None


def prefer_print_view_for_url(url: str, config: PipelineConfig) -> bool:
    host = (urlparse(url).netloc or "").lower()
    if host.endswith("thehindu.com"):
        return config.hindu_prefer_print_view
    if host.endswith("indianexpress.com"):
        return config.indian_express_prefer_print_view
    return False


def fetch_editorial_article(
    session: requests.Session, article_url: str, timezone_name: str, prefer_print_view: bool
) -> Editorial | None:
    response = session.get(
        article_url,
        headers={"User-Agent": "Mozilla/5.0 (EditorialPipeline/1.0)"},
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    soup = BeautifulSoup(decode_response_text(response), "html.parser")

    title = ""
    published_at = ""
    body = ""
    canonical_url = article_url
    author_names: list[str] = []
    author_lines: list[str] = []
    publisher_names: list[str] = []

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw_json = script.string or script.get_text()
        if not raw_json:
            continue
        try:
            payload = json.loads(raw_json)
        except json.JSONDecodeError:
            continue
        for node in parse_json_ld_nodes(payload):
            node_type = node.get("@type")
            node_types = [node_type] if isinstance(node_type, str) else node_type or []
            if "NewsArticle" not in node_types and "Article" not in node_types:
                continue
            title = first_non_empty(title, node.get("headline"))
            body = first_non_empty(body, node.get("articleBody"))
            published_at = first_non_empty(published_at, node.get("datePublished"))
            canonical_url = first_non_empty(canonical_url, node.get("url"))
            author_names.extend(flatten_json_ld_names(node.get("author")))
            author_names.extend(flatten_json_ld_names(node.get("creator")))
            author_lines.extend(extract_author_lines(node.get("author")))
            author_lines.extend(extract_author_lines(node.get("creator")))
            publisher_names.extend(flatten_json_ld_names(node.get("publisher")))

    canonical_tag = soup.select_one("link[rel='canonical']")
    if canonical_tag and canonical_tag.get("href"):
        canonical_url = canonical_tag["href"].strip()
    canonical_url = normalize_editorial_url(canonical_url) or normalize_editorial_url(article_url)
    if not canonical_url:
        logging.info("Skipping non-target opinion URL: %s", article_url)
        return None

    meta_title = ""
    meta_title_tag = soup.select_one("meta[property='og:title']")
    if meta_title_tag and meta_title_tag.get("content"):
        meta_title = meta_title_tag["content"].strip()
    h1 = soup.select_one("h1")
    h1_text = h1.get_text(" ", strip=True) if h1 else ""

    meta_published = ""
    meta_published_tag = soup.select_one("meta[property='article:published_time']")
    if meta_published_tag and meta_published_tag.get("content"):
        meta_published = meta_published_tag["content"].strip()

    meta_author_candidates = [
        soup.select_one("meta[name='author']"),
        soup.select_one("meta[property='article:author']"),
        soup.select_one("meta[name='article:author']"),
    ]
    for tag in meta_author_candidates:
        if tag and tag.get("content"):
            raw_author = tag["content"].strip()
            author_names.append(raw_author)
            author_lines.extend(extract_author_lines(raw_author))

    meta_publisher_candidates = [
        soup.select_one("meta[property='og:site_name']"),
        soup.select_one("meta[name='publisher']"),
    ]
    for tag in meta_publisher_candidates:
        if tag and tag.get("content"):
            publisher_names.append(tag["content"].strip())

    for author_anchor in soup.select("a[rel='author'], [class*='author' i] a, [itemprop='author']"):
        author_text = author_anchor.get_text(" ", strip=True)
        if author_text:
            author_names.append(author_text)
            author_lines.extend(extract_author_lines(author_text))
    header_author_lines = extract_header_author_lines(soup)
    author_lines.extend(header_author_lines)
    author_names.extend(
        [extract_name_from_author_line(line) for line in header_author_lines if extract_name_from_author_line(line)]
    )
    author_names = [
        normalized_name
        for normalized_name in unique_person_names(author_names)
        if is_probable_person_name(normalized_name)
    ]
    author_lines = unique_author_lines(author_lines)

    title = normalize_article_title(first_non_empty(h1_text, title, meta_title), canonical_url or article_url)
    published_at = first_non_empty(published_at, meta_published)
    body = first_non_empty(body, extract_article_text(soup))

    if prefer_print_view:
        print_url = extract_print_view_url(soup, article_url)
        if print_url and print_url != article_url:
            try:
                print_response = session.get(
                    print_url,
                    headers={"User-Agent": "Mozilla/5.0 (EditorialPipeline/1.0)"},
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                print_response.raise_for_status()
                print_soup = BeautifulSoup(decode_response_text(print_response), "html.parser")
                print_body = extract_article_text(print_soup)
                if len(print_body) > len(body) + 120:
                    body = print_body
                    logging.info("Using print view content for %s", article_url)
            except Exception as exc:
                logging.debug("Print view fetch failed for %s: %s", article_url, exc)

    if not title or not body:
        return None

    title = clean_text(title)
    body = clean_text(body)
    body, trailing_author_lines = extract_author_bios_from_body_tail(body, author_names)
    moderator_line = extract_moderator_line_from_body_intro(body)

    if trailing_author_lines:
        final_author_lines = unique_author_lines(
            [*trailing_author_lines, *([moderator_line] if moderator_line else [])]
        )[:4]
    else:
        final_author_lines = unique_author_lines(
            [*author_lines, *([moderator_line] if moderator_line else [])]
        )[:4]

    final_author_names = unique_person_names(
        [extract_name_from_author_line(line) for line in final_author_lines if extract_name_from_author_line(line)]
    )

    parsed_dt = parse_datetime(published_at, timezone_name)
    editorial_date = parsed_dt.date().isoformat() if parsed_dt else extract_date_from_url(canonical_url) or ""
    published_at = parsed_dt.isoformat() if parsed_dt else published_at

    digest = hashlib.sha1(canonical_url.encode("utf-8")).hexdigest()[:12]
    return Editorial(
        id=digest,
        title=title,
        url=canonical_url,
        published_at=published_at,
        editorial_date=editorial_date,
        body=body,
        key_points=[],
        author=", ".join(final_author_names[:4]),
        publisher=", ".join(unique_non_empty(publisher_names)[:2]),
        author_lines=final_author_lines,
    )


def fallback_summary(text: str, max_sentences: int = 2, max_words: int = SUMMARY_WORD_LIMIT) -> str:
    cleaned_text = clean_text(text)
    sentences = re.split(r"(?<=[.!?])\s+", cleaned_text.strip())
    selected = [sentence.strip() for sentence in sentences if sentence.strip()]
    return truncate_words(" ".join(selected[:max_sentences])[:600], max_words)


def parse_llm_json(content: str) -> dict[str, Any]:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(content[start : end + 1])
        raise


def extract_gemini_text(payload: dict[str, Any]) -> str:
    candidates = payload.get("candidates") or []
    for candidate in candidates:
        content = candidate.get("content") or {}
        parts = content.get("parts") or []
        text_parts = [str(part.get("text", "")).strip() for part in parts if part.get("text")]
        combined = "\n".join(part for part in text_parts if part)
        if combined:
            return combined
    raise ValueError(f"Gemini response did not contain text content: {json.dumps(payload)[:500]}")


def gemini_generate_json(
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    response_schema: dict[str, Any],
    temperature: float = 0.2,
) -> dict[str, Any]:
    response = requests.post(
        GEMINI_API_URL_TEMPLATE.format(model=model),
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
        },
        json={
            "systemInstruction": {
                "parts": [{"text": system_prompt}],
            },
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": user_prompt}],
                }
            ],
            "generationConfig": {
                "temperature": temperature,
                "responseMimeType": "application/json",
                "responseSchema": response_schema,
            },
        },
        timeout=REQUEST_TIMEOUT_SECONDS * 2,
    )
    response.raise_for_status()
    return parse_llm_json(extract_gemini_text(response.json()))


def summarize_editorials(editorials: list[Editorial], config: PipelineConfig) -> tuple[str, list[str]]:
    if not editorials:
        return "", []

    takeaway_count = target_daily_takeaway_count(editorials)

    if not config.gemini_api_key:
        logging.warning("GEMINI_API_KEY not set. Using fallback extractive summaries.")
        for editorial in editorials:
            editorial.summary = fallback_summary(editorial.body)
            editorial.key_points = []
            editorial.tone = "N/A"
            editorial.implications = ""
            editorial.highlight_phrases = None
            editorial.summary_markup = sanitize_pdf_markup(editorial.summary)
            editorial.key_takeaway_markup = sanitize_pdf_markup(build_key_takeaway(editorial.summary, editorial.key_points))
            editorial.implications_markup = sanitize_pdf_markup(editorial.implications)
            editorial.tone_markup = sanitize_pdf_markup(editorial.tone)
        takeaways = build_daily_takeaways(editorials, takeaway_count)
        return " ".join(takeaways), takeaways

    for editorial in editorials:
        candidate_author_lines = unique_author_lines(editorial.author_lines or [])[:6]
        candidate_author_lines_text = (
            "\n".join(f"- {line}" for line in candidate_author_lines)
            if candidate_author_lines
            else "- None extracted yet"
        )
        prompt = (
            "Summarize this Indian newspaper editorial. Return strict JSON with keys: "
            f"summary (string, max {SUMMARY_WORD_LIMIT} words, executive-level and consultant-style), "
            "key_points (array containing exactly 1 concise one-line takeaway, max 1 sentence), "
            f"tone (string), implications (string, max {IMPLICATIONS_WORD_LIMIT} words, very concise), "
            "author_lines (array of 0-4 strings formatted as 'Name - detail' or 'Name'), "
            "summary_markup (string), key_takeaway_markup (string), implications_markup (string), tone_markup (string).\n\n"
            "Write like a political consultant preparing a sharp internal briefing: analytical, strategic, "
            "consequence-oriented, and focused on policy stakes, institutional weaknesses, political signalling, "
            "and what matters most for decision-makers. The summary should add a little more context and read like an "
            "executive-level note highlighting the major arguments in the editorial. Mention important numbers, named actors, "
            "institutions, policy debates, and political context when the article provides them. Prioritise the most decision-relevant "
            "facts and clearly frame what the editorial is really warning about, endorsing, or contesting. "
            "Use author_lines as the final judged byline list. Compare the candidate author strings, decide when differently formatted names refer to the same person, and collapse duplicates. "
            "If two strings refer to the same person, keep only one line and prefer the richer line with role or affiliation details. "
            "Do not add any author name that is not already present in the candidate author lines. "
            "If the article explicitly states author bios, roles, affiliations, or moderator details, capture them in author_lines and do not invent anything that is not stated. Do not treat quoted people, political actors, or subjects mentioned in the article as authors. "
            "For the four markup fields, use the same wording as the plain text fields with no highlighting and no emphasis. "
            "Do not use <b> tags or any other HTML. Return plain text only. Avoid unsupported symbols like the rupee sign; write currency as Rs. "
            "Do not rewrite, shorten, or embellish the article title.\n\n"
            f"Title: {editorial.title}\n"
            f"URL: {editorial.url}\n\n"
            f"Candidate author lines already extracted:\n{candidate_author_lines_text}\n\n"
            f"Body:\n{editorial.body[:12000]}"
        )
        try:
            result = gemini_generate_json(
                api_key=config.gemini_api_key,
                model=config.gemini_model,
                system_prompt=(
                    "You produce accurate editorial summaries as structured JSON for a political consulting brief. "
                    "Your writing should be crisp, analytical, professionally neutral, and useful to senior decision-makers. "
                    "You are also the final judge for author-line deduplication: merge punctuation-only or formatting-only variants of the same person's name and keep the most informative byline. "
                    "You also prepare PDF-ready plain text fields with no highlighting markup."
                ),
                user_prompt=prompt,
                response_schema={
                    "type": "OBJECT",
                    "properties": {
                        "summary": {"type": "STRING"},
                        "key_points": {
                            "type": "ARRAY",
                            "items": {"type": "STRING"},
                        },
                        "tone": {"type": "STRING"},
                        "implications": {"type": "STRING"},
                        "author_lines": {
                            "type": "ARRAY",
                            "items": {"type": "STRING"},
                        },
                        "summary_markup": {"type": "STRING"},
                        "key_takeaway_markup": {"type": "STRING"},
                        "implications_markup": {"type": "STRING"},
                        "tone_markup": {"type": "STRING"},
                    },
                    "required": [
                        "summary",
                        "key_points",
                        "tone",
                        "implications",
                        "author_lines",
                        "summary_markup",
                        "key_takeaway_markup",
                        "implications_markup",
                        "tone_markup",
                    ],
                },
            )
            editorial.summary = truncate_words(
                clean_text(str(result.get("summary", "")).strip()) or fallback_summary(editorial.body),
                SUMMARY_WORD_LIMIT,
            )
            editorial.key_points = [clean_text(str(item).strip()) for item in result.get("key_points", []) if str(item).strip()][:1]
            editorial.tone = clean_text(str(result.get("tone", "")).strip()).rstrip(". ")
            editorial.implications = truncate_words(
                clean_text(str(result.get("implications", "")).strip()),
                IMPLICATIONS_WORD_LIMIT,
            )
            article_takeaway = build_key_takeaway(editorial.summary, editorial.key_points)
            llm_author_lines = filter_author_lines_to_candidates(
                [clean_text(str(item).strip()) for item in result.get("author_lines", []) if clean_text(str(item).strip())],
                candidate_author_lines,
            )[:4]
            if llm_author_lines:
                editorial.author_lines = llm_author_lines
                editorial.author = ", ".join(
                    unique_person_names([extract_name_from_author_line(line) for line in editorial.author_lines])[:4]
                )
            elif candidate_author_lines:
                editorial.author_lines = candidate_author_lines[:4]
                editorial.author = ", ".join(
                    unique_person_names([extract_name_from_author_line(line) for line in editorial.author_lines])[:4]
                )
            editorial.highlight_phrases = None
            editorial.summary_markup = sanitize_pdf_markup(str(result.get("summary_markup", "")) or editorial.summary)
            editorial.key_takeaway_markup = sanitize_pdf_markup(
                str(result.get("key_takeaway_markup", "")) or article_takeaway
            )
            editorial.implications_markup = sanitize_pdf_markup(
                str(result.get("implications_markup", "")) or editorial.implications
            )
            editorial.tone_markup = sanitize_pdf_markup(str(result.get("tone_markup", "")) or editorial.tone)
        except Exception as exc:
            logging.warning("Gemini summary failed for %s: %s", editorial.url, exc)
            editorial.summary = fallback_summary(editorial.body)
            editorial.key_points = []
            editorial.tone = "N/A"
            editorial.implications = ""
            editorial.highlight_phrases = None
            editorial.summary_markup = sanitize_pdf_markup(editorial.summary)
            editorial.key_takeaway_markup = sanitize_pdf_markup(build_key_takeaway(editorial.summary, editorial.key_points))
            editorial.implications_markup = sanitize_pdf_markup(editorial.implications)
            editorial.tone_markup = sanitize_pdf_markup(editorial.tone)

    combined = "\n".join(
        (
            f"- Title: {item.title}\n"
            f"  Summary: {item.summary}\n"
            f"  Implications: {item.implications or 'N/A'}\n"
            f"  Takeaway hints: {' | '.join(item.key_points or []) or 'N/A'}"
        )
        for item in editorials
    )[:16000]
    try:
        result = gemini_generate_json(
            api_key=config.gemini_api_key,
            model=config.gemini_model,
            system_prompt="Write concise daily editorial briefing takeaways as JSON.",
            user_prompt=(
                "From these editorial summaries, return strict JSON with key `takeaways`. "
                f"Return exactly {takeaway_count} concise takeaway points. Merge related or overlapping articles into shared themes, "
                "avoid near-duplicate points, and focus only on the strongest themes of the day. "
                "You do not need to cover every summary individually. "
                "Each point must be one or two short sentences.\n\n"
                f"{combined}"
            ),
            response_schema={
                "type": "OBJECT",
                "properties": {
                    "takeaways": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                    },
                },
                "required": ["takeaways"],
            },
        )
        takeaways = clean_takeaway_points([str(item) for item in result.get("takeaways", [])])[:MAX_DAILY_TAKEAWAYS]
        if len(takeaways) < MIN_DAILY_TAKEAWAYS:
            raise ValueError("Too few daily takeaways returned by LLM.")
        return " ".join(takeaways), takeaways
    except Exception as exc:
        logging.warning("Failed to build daily takeaways with Gemini: %s", exc)
        takeaways = build_daily_takeaways(editorials, takeaway_count)
        return " ".join(takeaways), takeaways


def write_pdf_report(
    output_path: Path,
    target_date: str,
    daily_takeaways: list[str],
    editorials: list[Editorial],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        leftMargin=0.5 * inch,
        rightMargin=0.5 * inch,
        topMargin=0.42 * inch,
        bottomMargin=0.42 * inch,
    )
    styles = getSampleStyleSheet()
    accent_blue = colors.HexColor("#3C78D8")
    title_style = ParagraphStyle(
        "BriefTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        alignment=TA_CENTER,
        textColor=accent_blue,
        spaceAfter=1,
    )
    centered_date_style = ParagraphStyle(
        "BriefDate",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=11.5,
        leading=13,
        alignment=TA_CENTER,
        textColor=colors.HexColor("#222222"),
    )
    header_note_style = ParagraphStyle(
        "HeaderNote",
        parent=styles["Normal"],
        fontName="Helvetica-Oblique",
        fontSize=9.3,
        leading=10,
        textColor=colors.HexColor("#7A7A7A"),
    )
    section_heading_style = ParagraphStyle(
        "SourceHeading",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=14.5,
        leading=18,
        textColor=accent_blue,
        spaceAfter=2,
    )
    article_title_style = ParagraphStyle(
        "ArticleTitle",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=12.2,
        leading=14,
        textColor=colors.HexColor("#111111"),
        leftIndent=18,
        firstLineIndent=-14,
        spaceAfter=0,
    )
    metadata_style = ParagraphStyle(
        "ArticleMeta",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=10.2,
        leading=12,
        textColor=colors.HexColor("#333333"),
        leftIndent=18,
        spaceAfter=0,
    )
    body_style = ParagraphStyle(
        "BodyJustified",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=11.2,
        leading=13.6,
        alignment=TA_JUSTIFY,
        textColor=colors.HexColor("#222222"),
        leftIndent=18,
        spaceAfter=0,
    )
    takeaway_heading_style = ParagraphStyle(
        "TakeawayHeading",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=14,
        leading=17,
        textColor=accent_blue,
    )
    takeaway_list_style = ParagraphStyle(
        "TakeawayList",
        parent=body_style,
        leftIndent=16,
        firstLineIndent=-10,
    )
    story: list[Any] = []
    cleaned_daily_takeaways = clean_takeaway_points(daily_takeaways) or build_daily_takeaways(editorials)
    formatted_date = format_report_date(target_date)
    grouped_editorials = group_editorials_by_source(editorials)

    def section_divider() -> HRFlowable:
        return HRFlowable(width="100%", thickness=0.7, color=colors.HexColor("#D0D7E2"), spaceBefore=2, spaceAfter=1)

    story.append(Spacer(1, 1))
    story.append(Paragraph("Editorial and Opinion Summary", title_style))
    story.append(Paragraph(html.escape(formatted_date, quote=False), centered_date_style))
    story.append(Spacer(1, 5))
    story.append(Paragraph("Note: Clicking on an editorial title will redirect you to the original article.", header_note_style))
    story.append(Spacer(1, 5))

    editorial_index = 1
    for source_name, source_editorials in grouped_editorials:
        story.append(Paragraph(html.escape(source_name.upper(), quote=False), section_heading_style))
        story.append(section_divider())
        story.append(Spacer(1, 3))

        for editorial in source_editorials:
            clean_title = sanitize_pdf_plain_text(editorial.title)
            clean_summary = sanitize_pdf_plain_text(editorial.summary)
            article_takeaway = build_key_takeaway(clean_summary, editorial.key_points)
            author_details = format_author_details(editorial)
            emphasized_summary = sanitize_pdf_markup(editorial.summary_markup or clean_summary, max_bold_segments=0)
            emphasized_takeaway = sanitize_pdf_markup(editorial.key_takeaway_markup or article_takeaway, max_bold_segments=0)

            heading = f"{editorial_index}. <b>{html.escape(clean_title, quote=False)}</b>"
            if editorial.url:
                safe_url = html.escape(editorial.url, quote=True)
                heading = f'{editorial_index}. <link href="{safe_url}"><font color="#111111"><b>{html.escape(clean_title, quote=False)}</b></font></link>'
            story.append(Paragraph(heading, article_title_style))
            for author_line in author_details:
                story.append(Spacer(1, 2))
                story.append(Paragraph(html.escape(sanitize_pdf_plain_text(author_line), quote=False), metadata_style))
            story.append(Spacer(1, 2))
            story.append(Paragraph(f"<b>Summary:</b> {emphasized_summary}", body_style))
            if article_takeaway:
                story.append(Spacer(1, 2))
                story.append(Paragraph(f"<b>Key Takeaway:</b> {emphasized_takeaway}", body_style))
            story.append(Spacer(1, 5))
            story.append(section_divider())
            story.append(Spacer(1, 2))
            editorial_index += 1

    story.append(Spacer(1, 4))
    story.append(Paragraph("Major Key Takeaways", takeaway_heading_style))
    story.append(section_divider())
    story.append(Spacer(1, 4))
    for takeaway_point in cleaned_daily_takeaways:
        story.append(Paragraph(f"&bull; {html.escape(takeaway_point, quote=False)}", takeaway_list_style))
        story.append(Spacer(1, 4))
    story.append(Spacer(1, 6))

    def draw_page_chrome(canvas, pdf_doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 9)
        canvas.drawRightString(A4[0] - pdf_doc.rightMargin, 20, f"Page {canvas.getPageNumber()}")
        canvas.restoreState()

    doc.build(story, onFirstPage=draw_page_chrome, onLaterPages=draw_page_chrome)


def upload_artifacts_to_s3(
    config: PipelineConfig, target_date: str, json_path: Path, pdf_path: Path
) -> dict[str, str]:
    if not config.s3_bucket:
        logging.info("S3_BUCKET not set. Skipping S3 upload.")
        return {}

    s3 = boto3.client("s3", region_name=config.aws_region)
    prefix = config.s3_prefix.strip("/")
    if prefix:
        json_key = f"{prefix}/{target_date}/editorials.json"
        pdf_key = f"{prefix}/{target_date}/daily-report.pdf"
    else:
        json_key = f"{target_date}/editorials.json"
        pdf_key = f"{target_date}/daily-report.pdf"

    s3.upload_file(str(json_path), config.s3_bucket, json_key, ExtraArgs={"ContentType": "application/json"})
    s3.upload_file(str(pdf_path), config.s3_bucket, pdf_key, ExtraArgs={"ContentType": "application/pdf"})
    logging.info("Uploaded artifacts to s3://%s/%s and s3://%s/%s", config.s3_bucket, json_key, config.s3_bucket, pdf_key)

    return {
        "json_s3_uri": f"s3://{config.s3_bucket}/{json_key}",
        "pdf_s3_uri": f"s3://{config.s3_bucket}/{pdf_key}",
    }


def build_slack_fallback_pdf_reference(config: PipelineConfig, artifact_info: dict[str, str]) -> str:
    pdf_s3_uri = artifact_info.get("pdf_s3_uri", "").strip()
    if not pdf_s3_uri:
        return ""

    bucket, key = parse_s3_uri(pdf_s3_uri)
    if not bucket or not key:
        return pdf_s3_uri

    try:
        s3 = boto3.client("s3", region_name=config.aws_region)
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=config.slack_fallback_link_expiry_seconds,
        )
    except Exception as exc:
        logging.warning("Could not create presigned S3 URL for Slack fallback: %s", exc)
        return pdf_s3_uri


def compute_slack_retry_delay_seconds(config: PipelineConfig, attempt_number: int, error: SlackApiError | None = None) -> int:
    if error is not None:
        headers = getattr(error.response, "headers", {}) or {}
        retry_after = headers.get("Retry-After") or headers.get("retry-after")
        if retry_after:
            try:
                return max(config.slack_retry_backoff_seconds, int(retry_after))
            except (TypeError, ValueError):
                pass
    return config.slack_retry_backoff_seconds * attempt_number


def post_slack_fallback_message(
    config: PipelineConfig,
    client: WebClient | None,
    message: str,
) -> bool:
    if client is not None and config.slack_channel_id:
        try:
            client.chat_postMessage(channel=config.slack_channel_id, text=message)
            logging.info("Posted Slack fallback message with PDF link.")
            return True
        except SlackApiError as exc:
            logging.warning(
                "Slack fallback chat message failed (%s).",
                exc.response.get("error", "unknown_error"),
            )
        except Exception as exc:
            logging.warning("Slack fallback chat message failed: %s", exc)

    if config.slack_webhook_url:
        try:
            response = requests.post(
                config.slack_webhook_url,
                json={"text": message},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            logging.info("Posted Slack fallback message via webhook.")
            return True
        except Exception as exc:
            logging.warning("Slack webhook fallback failed: %s", exc)

    return False


def post_to_slack(
    config: PipelineConfig,
    target_date: str,
    editorial_count: int,
    pdf_path: Path,
    artifact_info: dict[str, str] | None = None,
) -> None:
    if editorial_count == 0:
        return

    artifact_info = artifact_info or {}
    fallback_pdf_reference = build_slack_fallback_pdf_reference(config, artifact_info)
    summary_title = f"Editorial and Opinion Summary {target_date}"
    initial_comment = f"Editorial and Opinion Summary ({target_date})"

    if config.slack_bot_token and config.slack_channel_id:
        client = WebClient(token=config.slack_bot_token)
        joined_channel = False
        for attempt in range(1, config.slack_upload_max_retries + 1):
            try:
                client.files_upload_v2(
                    channel=config.slack_channel_id,
                    file=str(pdf_path),
                    filename=pdf_path.name,
                    title=summary_title,
                    initial_comment=initial_comment,
                )
                logging.info("Posted PDF file to Slack channel.")
                return
            except SlackApiError as exc:
                error_code = exc.response.get("error", "unknown_error")
                if error_code == "not_in_channel" and not joined_channel:
                    try:
                        client.conversations_join(channel=config.slack_channel_id)
                        joined_channel = True
                        logging.info("Joined Slack channel and retrying PDF upload.")
                        continue
                    except SlackApiError as join_exc:
                        logging.warning(
                            "Could not join Slack channel before retry (%s).",
                            join_exc.response.get("error", "unknown_error"),
                        )
                if error_code in SLACK_RETRYABLE_ERRORS and attempt < config.slack_upload_max_retries:
                    delay_seconds = compute_slack_retry_delay_seconds(config, attempt, exc)
                    logging.warning(
                        "Slack upload attempt %d/%d failed (%s). Retrying in %d seconds.",
                        attempt,
                        config.slack_upload_max_retries,
                        error_code,
                        delay_seconds,
                    )
                    time.sleep(delay_seconds)
                    continue
                logging.warning(
                    "Slack upload failed (%s). Falling back to Slack message notification if possible.",
                    error_code,
                )
                break
            except Exception as exc:
                if attempt < config.slack_upload_max_retries:
                    delay_seconds = compute_slack_retry_delay_seconds(config, attempt)
                    logging.warning(
                        "Slack upload attempt %d/%d failed (%s). Retrying in %d seconds.",
                        attempt,
                        config.slack_upload_max_retries,
                        exc,
                        delay_seconds,
                    )
                    time.sleep(delay_seconds)
                    continue
                logging.warning("Slack upload failed after retries: %s", exc)
                break

        if fallback_pdf_reference:
            fallback_message = (
                f"{initial_comment}\n"
                f"Slack file upload did not complete, so here is the PDF download link instead:\n"
                f"{fallback_pdf_reference}"
            )
        else:
            fallback_message = (
                f"{initial_comment}\n"
                "Slack file upload did not complete and no S3 fallback link is available."
            )
        if post_slack_fallback_message(config, client, fallback_message):
            return
        logging.warning("Slack delivery did not succeed, and fallback notification also failed.")
        return

    if config.slack_webhook_url:
        if fallback_pdf_reference:
            fallback_message = (
                f"{initial_comment}\n"
                f"PDF download link:\n{fallback_pdf_reference}"
            )
            if post_slack_fallback_message(config, None, fallback_message):
                return
        logging.warning(
            "SLACK_WEBHOOK_URL is configured, but webhook delivery cannot upload files. "
            "Configure S3 plus bot-token upload or S3 fallback links for reliable PDF delivery."
        )
        return

    logging.info("Slack not configured. Skipping Slack delivery.")


def run_pipeline(config: PipelineConfig) -> None:
    output_dir = Path(config.output_dir).resolve() / config.target_date
    output_dir.mkdir(parents=True, exist_ok=True)
    window_start, window_end = build_editorial_window(config.target_date, config.timezone)
    previously_sent_urls = load_sent_article_urls(Path(config.output_dir).resolve(), config.target_date)

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (EditorialPipeline/1.0)"})
    source_logins_confirmed = 0
    if authenticate_hindu(session, config):
        source_logins_confirmed += 1
    elif config.hindu_username and config.hindu_password:
        logging.warning("The Hindu login was not confirmed; subscriber-only opinion pages may fail.")

    if authenticate_indian_express(session, config):
        source_logins_confirmed += 1
    elif config.indian_express_username and config.indian_express_password:
        logging.warning("Indian Express login was not confirmed; subscriber-only opinion pages may fail.")

    if source_logins_confirmed == 0:
        logging.warning("No source login was confirmed; subscriber-only opinion pages may fail.")

    links = fetch_editorial_links(session, config.editorial_urls)
    logging.info(
        "Found %d candidate opinion article link(s) across %d source section(s).",
        len(links),
        len(config.editorial_urls),
    )
    logging.info(
        "Filtering opinion articles within %s <= published_at < %s",
        window_start.isoformat(),
        window_end.isoformat(),
    )
    logging.info(
        "Loaded %d previously sent article URL(s) from %s for duplicate suppression.",
        len(previously_sent_urls),
        previous_calendar_date(config.target_date),
    )

    editorials: list[Editorial] = []
    seen_urls: set[str] = set()
    for link in links:
        try:
            editorial = fetch_editorial_article(
                session,
                link,
                config.timezone,
                prefer_print_view_for_url(link, config),
            )
        except Exception as exc:
            logging.warning("Failed to parse %s: %s", link, exc)
            continue
        if not editorial:
            continue
        if not normalize_editorial_url(editorial.url):
            logging.info("Skipping article outside allowed opinion sections: %s", editorial.url)
            continue
        if editorial.url in seen_urls:
            continue
        if editorial.url in previously_sent_urls:
            logging.info("Skipping article already sent in previous day's Slack payload: %s", editorial.url)
            continue
        if not is_editorial_in_window(editorial, config.target_date, config.timezone):
            continue
        seen_urls.add(editorial.url)
        editorials.append(editorial)

    if not editorials:
        logging.info("No opinion article found for %s. Skipping editorial PDF/S3/Slack.", config.target_date)
    else:
        generated_at = datetime.now(get_tzinfo(config.timezone)).isoformat()
        daily_overview, daily_takeaways = summarize_editorials(editorials, config)
        payload = {
            "target_date": config.target_date,
            "generated_at": generated_at,
            "source_section": "multiple" if len(config.editorial_urls) > 1 else config.editorial_url,
            "source_sections": config.editorial_urls,
            "editorial_count": len(editorials),
            "daily_overview": clean_text(daily_overview),
            "daily_takeaways": daily_takeaways,
            "items": [editorial_to_payload(item) for item in editorials],
        }

        json_path = output_dir / "editorials.json"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logging.info("Wrote editorial JSON to %s", json_path)

        pdf_path = output_dir / "daily-report.pdf"
        write_pdf_report(pdf_path, config.target_date, daily_takeaways, editorials)
        logging.info("Wrote daily PDF report to %s", pdf_path)

        artifact_info = upload_artifacts_to_s3(config, config.target_date, json_path, pdf_path)
        post_to_slack(config, config.target_date, len(editorials), pdf_path, artifact_info)


def main() -> None:
    load_env_file()
    args = parse_args()
    config = build_config(args)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    logging.info(
        "Starting editorial pipeline for target_date=%s timezone=%s",
        config.target_date,
        config.timezone,
    )
    run_pipeline(config)


if __name__ == "__main__":
    main()
