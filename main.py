from src.yt_api_manager import YoutubeManager
from utils.logger import setup_logger
from utils.helpers import extract_transcript, convert_vtt_to_text


if __name__ == "__main__":
    logger = setup_logger(__name__)
    # authenticate credentials for youtube api v3
    yt_manager = YoutubeManager(secrets_file='auth/client_secrets.json', token_file='auth/token.json')
    logger.info("Authenticated to YouTube API")
    

    # '''
    # Playlist and channel summaries
    # Use get_all_playlists and get_playlist_items to collect every video in your playlists, then fetch per-video metadata with get_video_info for stats and durations.

    # Topic / theme extraction
    # Aggregate titles, descriptions, and tags from get_video_info and get_video_tags and run keyword extraction or topic modeling (LDA, BERTopic).

    # Channel network and relationships
    # For each video, use get_channel_info to collect channel IDs; build a graph where nodes are channels and edges indicate co-occurrence in your playlists or shared tags.

    # Popular creators and engagement
    # Use statistics from get_video_info to rank videos/channels by views, likes, comment counts.

    # Comment analysis
    # Fetch comments with get_comments to run sentiment analysis, identify recurring topics or feedback patterns.

    # Transcript / content analysis

    # Extract video transcript (captions)
    # video_id = "EBFyrgRN4N4"  # replace with your video
    # # transcript
    # transcript = extract_transcript(video_id=video_id, lang='en')
    # transcript = convert_vtt_to_text(vtt_path='outputs/EBFyrgRN4N4.vtt.en.vtt')
    # if transcript:
    #     logger.info(f"Transcript for video {video_id}:\n{transcript[:500]}...")

    # When available, download transcripts via get_transcript and run NLP to extract topics, named entities, or compute watch-time topic distributions.
    # Add a wrapper for subscriptions.list to get subscribed channels.
    # Add category and NLP pipelines to extract themes from snippet.title, snippet.description, and tags from get_video_info.
    # Add export utilities to write CSV/JSON outputs under outputs for downstream analysis.
    # '''