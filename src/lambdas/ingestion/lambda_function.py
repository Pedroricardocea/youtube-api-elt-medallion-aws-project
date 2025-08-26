import os
import json
import time
import logging
import boto3
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlencode
import io
import csv


# ---------- Config via Environment Variables ----------
COUNTRY_CODES = [c.strip() for c in os.environ["COUNTRY_CODES"].split(",")]
EXPECTED_BUCKET_OWNER = os.environ["EXPECTED_BUCKET_OWNER"]
RAW_BUCKET = os.environ["RAW_BUCKET"]
RAW_STATS_PREFIX = os.environ["RAW_STATS_PREFIX"].rstrip("/") + "/"
RAW_STATS_REF_PREFIX = os.environ["RAW_STATS_REF_PREFIX"].rstrip("/") + "/"
YOUTUBE_API_KEY = os.environ["YOUTUBE_API_KEY"]

# ---------- Logging ----------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------- AWS ----------
s3 = boto3.client("s3")

# ---------- YouTube Data API v3 ----------
YT_BASE = "https://www.googleapis.com/youtube/v3"


def yt_get(path: str, params: dict):
    params = {**params, "key": YOUTUBE_API_KEY}
    url = f"{YT_BASE}/{path}?{urlencode(params)}"
    with urllib.request.urlopen(url, timeout=30) as resp:
        if resp.status != 200:
            raise RuntimeError(f"YouTube API error {resp.status}: {resp.read()}")
        return json.loads(resp.read().decode("utf-8"))


def put_json_s3(bucket: str, key: str, data: dict):
    body = json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        ExpectedBucketOwner=EXPECTED_BUCKET_OWNER,
    )
    logger.info(f"PUT s3://{bucket}/{key} ({len(body)} bytes)")


def put_csv_s3(bucket: str, key: str, csv_text: str):
    body = csv_text.encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="text/csv",
        ExpectedBucketOwner=EXPECTED_BUCKET_OWNER,
    )
    logger.info(f"PUT s3://{bucket}/{key} ({len(body)} bytes)")


# ---------- Reference Data ----------


def fetch_categories_for_country(cc: str):
    """videoCategories per region code."""
    data = yt_get("videoCategories", {"part": "snippet", "regionCode": cc})
    put_json_s3(RAW_BUCKET, f"{RAW_STATS_REF_PREFIX}{cc}_category_id.json", data)


def _get(d, path, default=""):
    cur = d
    for p in path.split("."):
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return default
    return cur if cur is not None else default


def _to_str(v):
    if v is None:
        return ""
    if isinstance(v, list):
        return "|".join(map(str, v))
    return str(v)


# Only columns that are still available in YouTube Data API v3
CSV_COLUMNS = [
    "video_id",
    "published_at",
    "channel_id",
    "channel_title",
    "title",
    "category_id",
    "tags",
    "duration",  # ISO8601 (PT#M#S)
    "dimension",  # 2d/3d (when present)
    "definition",  # hd/sd
    "licensed_content",  # true/false
    "view_count",
    "like_count",
    "comment_count",
]


# ---------- Videos (Most Popular) ----------
def fetch_most_popular_for_country(cc: str, ts_folder: str):
    """
    Fetch only the FIRST page (50 videos max) for a region
    and write ONE CSV (API limitations):
      s3://.../{RAW_STATS_PREFIX}region=CC/{ts_folder}/videos.csv
    """
    params = {
        "part": "snippet,statistics,contentDetails",
        "chart": "mostPopular",
        "regionCode": cc,
        "maxResults": 50,
    }

    data = yt_get("videos", params)
    items = data.get("items", [])

    rows = []
    for it in items:
        rows.append(
            {
                "video_id": _get(it, "id"),
                "published_at": _get(it, "snippet.publishedAt"),
                "channel_id": _get(it, "snippet.channelId"),
                "channel_title": _get(it, "snippet.channelTitle"),
                "title": _get(it, "snippet.title"),
                "category_id": _get(it, "snippet.categoryId"),
                "tags": _to_str(_get(it, "snippet.tags", [])),
                "duration": _get(it, "contentDetails.duration"),
                "dimension": _get(it, "contentDetails.dimension"),
                "definition": _get(it, "contentDetails.definition"),
                "licensed_content": str(_get(it, "contentDetails.licensedContent", "")),
                "view_count": _get(it, "statistics.viewCount"),
                "like_count": _get(it, "statistics.likeCount"),
                "comment_count": _get(it, "statistics.commentCount"),
            }
        )

    # Write single CSV for the region
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow(r)

    key = f"{RAW_STATS_PREFIX}region={cc}/{ts_folder}/{cc}videos.csv"
    put_csv_s3(RAW_BUCKET, key, out.getvalue())


def current_ts_folder() -> str:
    # S3-friendly timestamp segment, e.g., ts=2025-08-24T23-10-05Z
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    return f"ts={now}"


# ---------- Lambda handler ----------
def lambda_handler(event, context):
    """
    Minimal, dependable flow:
      1) Write regions reference JSON
      2) Write video categories per region
      3) Write paged most-popular videos per region (JSON)
    All output goes to s3://RAW_BUCKET under RAW_STATS_REF_PREFIX and RAW_STATS_PREFIX.
    """
    logger.info(f"Starting YouTube scrape for regions: {COUNTRY_CODES}")
    ts_folder = current_ts_folder()

    # 1) Categories + 3) Videos for each country
    for cc in COUNTRY_CODES:
        try:
            fetch_categories_for_country(cc)
        except Exception:
            logger.exception(f"Failed to fetch categories for {cc} (continuing).")

        try:
            fetch_most_popular_for_country(cc, ts_folder)
        except Exception:
            logger.exception(
                f"Failed to fetch most popular videos for {cc} (continuing)."
            )

    msg = {
        "status": "ok",
        "countries": COUNTRY_CODES,
        "output_bucket": RAW_BUCKET,
        "refs_prefix": RAW_STATS_REF_PREFIX,
        "stats_prefix": RAW_STATS_PREFIX,
        "timestamp_folder": ts_folder,
    }
    logger.info(msg)
    return msg
