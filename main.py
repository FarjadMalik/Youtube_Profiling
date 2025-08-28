# Internal imports
from src.yt_api_manager import YoutubeManager
from src.yt_data_manager import DataManager
from src.yt_analysis import plot_word_cloud
from utils.helpers import (
    extract_transcript, 
    convert_vtt_to_text
)
from utils.logger import setup_logger

logger = setup_logger(__name__)


def main():
    logger.info("Starting YouTube Profiling Tool")
    
    # Instantiate data manager 
    data_manager = DataManager(client_secrets='auth/client_secrets.json', token='auth/token.json', 
                               inputs='data', outputs='outputs')
    
    # playlist_doc = data_manager.get_playlist_doc(playlist_id='PLFzojsQltjOBtoz-_Hu0c2xu26aIIW0Ws')
    # Assuming `data` is your List[str]
    # blob = ' '.join(user_playlist_docs)
        
    
    user_playlist_docs = data_manager.get_user_playlist_docs()
    # Assuming `data` is your List[List[str]]
    blob = ' '.join(word for sublist in user_playlist_docs for word in sublist)
    data_manager.save_pickle(blob=blob, filename='user_playlist_docs')
    # Create a word cloud from playlist docs
    plot_word_cloud(blob, title='User Playlist Content')
    
if __name__ == "__main__":
    main()

    # TODO: Perform various analyses and profiling tasks

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