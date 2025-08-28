import re
import csv
import json
import subprocess
import webvtt
import tempfile
import nltk
import matplotlib.pyplot as plt

from nltk.corpus import stopwords
from typing import List, Dict
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# Internal libraries
from utils.logger import setup_logger

logger = setup_logger(__name__)
nltk.download('stopwords')
STOPWORDS = set(stopwords.words('english'))


def id_to_url(vid: str) -> str:
    return f"https://www.youtube.com/watch?v={vid}"

def extract_transcript(video_id: str, lang: str = 'en') -> str | None:
    """
    Extract auto-generated captions from a YouTube video using yt-dlp,
    returning plain transcript text. Returns None if unavailable.
    """
    video_url = id_to_url(vid=video_id)
    logger.debug(f"Extracting transcript from {video_url} in language '{lang}'")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        output_template = tmp_path / "%(id)s.%(ext)s"

        # Run yt-dlp command
        try:
            subprocess.run(
                [
                    'yt-dlp',
                    '--write-auto-sub',
                    '--sub-lang', lang,
                    '--skip-download',
                    '--output', str(output_template),
                    video_url
                ],
                check=True,
                capture_output=True
            )
        except subprocess.CalledProcessError:
            logger.error(f"❌ yt-dlp failed for {video_url}")
            return None

        # Construct expected subtitle file path
        vtt_path = tmp_path / f"{video_id}.{lang}.vtt"
        if not vtt_path.exists():
            logger.error(f"⚠️ No subtitles found for {video_url}")
            return None

        try:
            return convert_vtt_to_text(vtt_path=str(vtt_path))
        except Exception as e:
            logger.error(f"⚠️ Failed to parse VTT for {video_url}: {e}")
            return None

def normalize_line(line):
    return re.sub(r'\s+', ' ', line.lower().strip())

def clean_doc_text(text):
    text = re.sub(r"http\S+|www\S+|https\S+", '', text, flags=re.MULTILINE)
    text = re.sub(r'\W+', ' ', text)
    text = text.lower()
    return text

def convert_vtt_to_text(vtt_path: str) -> str:
    """
    Parses a .vtt subtitle file and returns a de-duplicated transcript string.
    Removes exact repeated lines while preserving order.
    """
    seen = set()
    lines = []

    for caption in webvtt.read(vtt_path):
        # Normalize each line in the caption block
        for line in caption.text.strip().splitlines():
            clean_line = normalize_line(line)
            if clean_line and clean_line not in seen:
                seen.add(clean_line)
                lines.append(clean_line)

    return ' '.join(lines)

def extract_video_id_from_url(url: str) -> str | None:
    """
    Try to extract a YouTube video id from a URL or query string.
    Returns the 11-char video id or None.
    """
    if not url:
        return None
    # common patterns: v=VIDEOID, youtu.be/VIDEOID, /watch?v=VIDEOID
    m = re.search(r'(?:v=|youtu\.be/|/embed/)([A-Za-z0-9_-]{11})', url)
    if m:
        return m.group(1)
    # sometimes url is just the id
    if re.fullmatch(r'[A-Za-z0-9_-]{11}', url):
        return url
    # try parsing query params
    try:
        p = urlparse(url)
        qs = parse_qs(p.query)
        if 'v' in qs and qs['v']:
            vid = qs['v'][0]
            if re.fullmatch(r'[A-Za-z0-9_-]{11}', vid):
                return vid
    except Exception:
        pass
    return None
