import re
import subprocess
import webvtt
import tempfile

from pathlib import Path

# Internal libraries
from utils.logger import setup_logger
logger = setup_logger(__name__)


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
