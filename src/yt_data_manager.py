import os
import csv
import json
import pickle

from typing import List, Dict
from pathlib import Path

# Internal libraries
from src.yt_api_manager import YoutubeManager
from utils.helpers import extract_video_id_from_url, extract_transcript, clean_doc_text
from utils.logger import setup_logger


class DataManager:
    def __init__(self, client_secrets: str, token: str, inputs: str, outputs: str) -> None:
        """

        """
        self.logger = setup_logger(__name__)
        
        if not client_secrets and not token:
            self.logger.error(f"Please provide a clients_secret file (currently: {client_secrets}) " \
                              f"or an already authorized token file (currently: {token}) to access youtube api.")
            return
               
        self.yt_api = self._auth(client_secrets, token)
        self.logger.info("Authenticated to YouTube API")
        
        self.data_folder = inputs
        self.outputs_folder = outputs
        self._verify_folders(self.data_folder, self.outputs_folder)


    def _auth(self, client_secrets: str, token_file: str) -> YoutubeManager:
        # Authenticate credentials for youtube api v3
        return YoutubeManager(secrets_file=client_secrets, token_file=token_file)
    
    def _verify_folders(self, input: str, output: str):
        if not os.path.exists(input):
            self.logger.warning(f"Input folder ({input}) does not exist")
    
        if not os.path.exists(output):
            os.makedirs(output)

    def get_take_out_history(self, filepath: str = 'watch-history.json', max_entries: int = 100) -> List[Dict] | None:
        """
        Read YouTube watch-history.json from Google Takeout and return a list of watched video IDs
        in chronological order (as they appear in the file). Returns None if file missing.
        Args:
            filepath: path to the watch-history.json file.
        Returns:
            List[Dict]: list of video IDs or None if file not found.
        """
        path = Path(filepath)
        if not path.exists():
            self.logger.warning(f"watch-history file not found at {path}")
            return None

        items = []
        try:
            with path.open(encoding='utf-8') as f:
                data = json.load(f)
                # data is generally a list of dicts
                for entry in data:
                    if not isinstance(entry, dict):
                        continue
                    items.append({
                        'title': entry.get('title', ''),
                        'url': entry.get('titleUrl', ''),
                        'video_id': extract_video_id_from_url(entry.get('titleUrl', '')),
                        'channel_title': entry.get('subtitles', [{}])[0].get('name', '') if entry.get('subtitles') else '',
                        'channel_url': entry.get('subtitles', [{}])[0].get('url', '') if entry.get('subtitles') else '',
                        'time': entry.get('time', '')
                    })
                    if len(items) >= max_entries:
                        self.logger.debug(f"Truncating watch history to first {max_entries} entries")
                        break
        except Exception as e:
            self.logger.error(f"Failed to parse watch-history {path}: {e}")
            return None

        return items

    def get_take_out_csv(self, filename: str = 'subscriptions', max_entries: int = 100) -> List[Dict] | None:
        """
        Read CSV from Takeout and return a list of dict containing corresponding info.
        """
        path = Path(f"data/{filename.split('.')[0]}.csv")
        if not path.exists():
            self.logger.warning(f"subscriptions file not found at {path}")
            return None

        items = []
        try:
            with path.open(newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
                if not rows:
                    return []
                header = rows[0]
                body = rows[1:]  # skip header if present
                for row in body:
                    if not any((cell or '').strip() for cell in row):
                        continue
                    items.append(
                        {k.lower().replace(' ', '_').strip(): (row[i] if i < len(row) else '') for i, k in enumerate(header)}
                    )
                    if len(items) >= max_entries:
                        self.logger.debug(f"Truncating subscriptions to {max_entries} entries")
                        break
        except Exception as e:
            self.logger.error(f"Failed to read subscriptions file {path}: {e}")
            return None

        return items

    def get_take_out_playlist(self, playlist_name: str = 'Watch later', max_entries: int = 100) -> List[str] | None:
        """
        Read playlist CSV from Google Take out and return a list of info dict.
        By default, reads 'data/playlists/Watch later-videos.csv', playlist_name = 'Watch Later'
        """
        path = Path(f"data/playlists/{playlist_name}-videos.csv")
        if not path.exists():
            self.logger.warning(f"watch later file not found at {path}")
            return None

        items = []
        try:
            with path.open(newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                rows = list(reader)
                if not rows:
                    return []
                header = rows[0]
                body = rows[1:]  # skip header if present
                for row in body:
                    if not any((cell or '').strip() for cell in row):
                        continue
                    items.append(
                        {k.lower().replace(' ', '_').strip(): (row[i] if i < len(row) else '') for i, k in enumerate(header)}
                    )
                    if len(items) >= max_entries:
                        self.logger.debug(f"Truncating subscriptions to {max_entries} entries")
                        break
        except Exception as e:
            self.logger.error(f"Failed to read watch later file {path}: {e}")
            return None

        return items
    
    def get_user_playlist_docs(self) -> List[List[str]]:
        """
        
        """
        # get all of the user playlists
        u_playlists = self.yt_api.get_user_playlists(max_results=1)
        playlist_docs = []
        for pl in u_playlists:
            # for each playlist add a new doc to the list
            playlist_docs.append(
                self.get_playlist_doc(playlist_id=pl.get('playlistId', ''))
                )

        return playlist_docs
    
    def get_playlist_doc(self, playlist_id: str) -> List[str]:
        """
        """
        if not playlist_id:
            self.logger.warning(f"No playlist id provided.")
            return []
        
        # list all items in the given playlist
        items = self.yt_api.get_playlist_items(playlist_id=playlist_id)

        video_ids = [item.get('videoId', '') for item in items]
        video_titles = [item.get('title', '') for item in items]
        video_desc = [item.get('description', '') for item in items]
        # Get tags and topics for each video using for loop and api functions
        video_tags = []
        video_topics = [] 
        for vid in video_ids:
            video_tags.append(self.yt_api.get_video_tags(video_id=vid))
            # Get topic details actual value and strip the url part
            video_topics.append([topic.split('/')[-1] for topic in self.yt_api.get_video_topics(video_id=vid)])

        # Create a joined string using all the video details
        doc = [f"{t} {d} {' '.join(tg if isinstance(tg, list) else [tg])} {' '.join(tp if isinstance(tp, list) else [tp])}" 
               for t, d, tg, tp in zip(video_titles, video_desc, video_tags, video_topics)]
        
        return [clean_doc_text(d) for d in doc]
        
    def save_pickle(self, blob: str, filename: str) -> None:
        with open(f'{self.data_folder}/{filename}.pkl', 'wb') as fl:
            pickle.dump(blob, fl)
        
    def load_pickle(self, filename: str):
        with open(f'{self.data_folder}/{filename}.pkl', 'rb') as f:
            blob = pickle.load(f)
        return blob
