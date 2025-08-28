# External libraries
import os

from typing import List, Dict
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# Internal libraries
from utils.logger import setup_logger
from utils.retry import retry_on_exception

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]


class YoutubeManager:
    def __init__(self, secrets_file: str = 'client_secrets.json', token_file: str = 'token.json'):
        """ Initialize, Authenticate and return a YouTube Data API service."""
        self.credentials = None
        self.logger = setup_logger(__name__)
        self.max_retries = 3

        if not secrets_file and not token_file:
            self.logger.error(f"Please provide a clients_secret file (currently: {secrets_file}) " \
                              f"or an already authorized token file (currently: {token_file}) to access youtube api.")
            return

        if os.path.exists(token_file):
            self.credentials = Credentials.from_authorized_user_file(token_file, SCOPES)

        if not self.credentials or not self.credentials.valid:
            if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                self.credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(secrets_file, SCOPES)
                self.credentials = flow.run_local_server(port=0)

            # Save the credentials for next time
            with open(token_file, 'w') as token:
                token.write(self.credentials.to_json())

        self.yt_service = build('youtube', 'v3', credentials=self.credentials)

    # Get playlist, video or channel information
    def get_playlist_info(self, playlist_id: str) -> Dict:
        """
        Get playlist resource for a single playlist id.
        Returns a playlist resource dictionary or None on error/empty.
        Args:
            playlist_id (str): The ID of the playlist to retrieve.
        Returns:
            Dict: A playlist resource dictionary.
        """
        items = []

        @retry_on_exception()
        def _call(pid: str):
            return self.yt_service.playlists().list(
                part="snippet,contentDetails",
                id=pid
            ).execute()
        
        try:
            response = _call(playlist_id)
            items.extend(response.get("items", []))
        except Exception:
            self.logger.error("[ERROR] Could not fetch user channel", exc_info=True)
            return {}
        # parse items to return only relevant details
        playlist_info = [
            {
                'playlistId': item.get('id'),
                'Title': item.get('snippet', {}).get('title'),
                'Description': item.get('snippet', {}).get('description'),
                'PublishedAt': item.get('snippet', {}).get('publishedAt'),
                'channelId': item.get('snippet', {}).get('channelId'),
                'channelTitle': item.get('snippet', {}).get('channelTitle'),
                'itemCount': item.get('contentDetails', {}).get('itemCount')
            }
            for item in items or []
        ]
        return playlist_info[0] if len(playlist_info) else {}
    
    def get_user_channel(self, part: str = "snippet,contentDetails,statistics,topicDetails") -> Dict:
        """
        Fetches the channel resource for the authenticated user.
        Returns a channel resource dictionary or None on error/empty.
        Args:
            part (str): Comma-separated list of resource parts to retrieve.
        Returns:
            Dict: A channel resource dictionary.
        """
        items = []
        @retry_on_exception()
        def _call(part: str):
            return self.yt_service.channels().list(
                part=part,
                mine=True
            ).execute()

        try:
            response = _call(part)
            items.extend(response.get("items", []))
        except Exception:
            self.logger.error("[ERROR] Could not fetch user channel", exc_info=True)
            return {}
        # parse items to return only relevant details
        channel_info = [
            {
                'channelId': item.get('id'),
                'Title': item.get('snippet', {}).get('title'),
                'Description': item.get('snippet', {}).get('description'),
                'PublishedAt': item.get('snippet', {}).get('publishedAt'),
                'customUrl': item.get('snippet', {}).get('customUrl'),
                'uploadPlaylist': item.get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads'),
                'likedPlaylist': item.get('contentDetails', {}).get('relatedPlaylists', {}).get('likes'),
                'viewCount': item.get('statistics', {}).get('viewCount'),
                'subscriberCount': item.get('statistics', {}).get('subscriberCount'),
                'videoCount': item.get('statistics', {}).get('videoCount'),
                'topicDetails': item.get('topicDetails', {}).get('topicCategories', []),
            }
            for item in items or []
        ]
        return channel_info[0] if len(channel_info) else {}
    
    def get_channel_info(self, channel_id: str) -> Dict:
        """
        Get channel resource for a single channel id.
        Args:
            channel_id (str): The ID of the channel to retrieve.
        Returns:
            Dict: A channel resource dictionary.
        """
        items = []
        @retry_on_exception()
        def _call(cid: str):
            return self.yt_service.channels().list(
                part="snippet,contentDetails,statistics,topicDetails",
                id=cid
            ).execute()
        
        try:
            response = _call(channel_id)
            items.extend(response.get("items", []))
        except Exception:
            self.logger.error("[ERROR] Could not fetch user channel", exc_info=True)
            return {}
        # parse items to return only relevant details
        channel_info = [
            {
                'channelId': item.get('id'),
                'Title': item.get('snippet', {}).get('title'),
                'Description': item.get('snippet', {}).get('description'),
                'country': item.get('snippet', {}).get('country'),
                'publishedAt': item.get('snippet', {}).get('publishedAt'),
                'customUrl': item.get('snippet', {}).get('customUrl'),
                'uploadPlaylist': item.get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads'),
                'likedPlaylist': item.get('contentDetails', {}).get('relatedPlaylists', {}).get('likes'),
                'viewCount': item.get('statistics', {}).get('viewCount'),
                'subscriberCount': item.get('statistics', {}).get('subscriberCount'),
                'videoCount': item.get('statistics', {}).get('videoCount'),
                'topicDetails': item.get('topicDetails', {}).get('topicCategories', []),
            }
            for item in items or []
        ]
        return channel_info[0]  if len(channel_info) else {}
        
    def get_video_info(self, video_id: str) -> Dict:
        """
        Get video resource for a single video id.
        Args:
            video_id (str): The ID of the video to retrieve.
        Returns:
            Dict: A video resource dictionary.
        """
        items = []
        @retry_on_exception()
        def _call(vid: str):
            return self.yt_service.videos().list(
                part="snippet,contentDetails,statistics,topicDetails",
                id=vid
            ).execute()
        
        try:
            response = _call(video_id)
            items.extend(response.get("items", []))
        except Exception:
            self.logger.error("[ERROR] Could not fetch user channel", exc_info=True)
            return {}
        # parse items to return only relevant details
        video_info = [
            {
                'videoId': item.get('id'),
                'channelId': item.get('snippet', {}).get('channelId'),
                'channelTitle': item.get('snippet', {}).get('channelTitle'),
                'Title': item.get('snippet', {}).get('title'),
                'Description': item.get('snippet', {}).get('description'),
                'publishedAt': item.get('snippet', {}).get('publishedAt'),
                'categoryId': item.get('snippet', {}).get('categoryId'),
                'defaultLanguage': item.get('snippet', {}).get('defaultLanguage'),
                'defaultAudioLanguage': item.get('snippet', {}).get('defaultAudioLanguage'),
                'tags': item.get('snippet', {}).get('tags', []),
                'duration': item.get('contentDetails', {}).get('duration'),
                'hasCaption': item.get('contentDetails', {}).get('caption'),
                'viewCount': item.get('statistics', {}).get('viewCount'),
                'likeCount': item.get('statistics', {}).get('likeCount'),
                'commentCount': item.get('statistics', {}).get('commentCount'),
                'topicDetails': item.get('topicDetails', {}).get('topicCategories', [])
            }
            for item in items or []
        ]
        return video_info[0]  if len(video_info) else {}
    
    # Manage playlists
    def get_user_playlists(self, max_results: int = 50, part: str = "snippet") -> List[Dict]:
        """
        Fetches up to `max_results` playlists for the authenticated user.

        Args:
            max_results (int): Maximum number of playlists to retrieve.
            part (str): Comma-separated list of resource parts to retrieve.

        Returns:
            List[Dict]: A list of playlist resource dictionaries.
        """
        if max_results <= 0:
            return []

        items = []
        next_page_token = None

        @retry_on_exception()
        def _fetch_page(page_token: str, limit: int):
            return self.yt_service.playlists().list(
                part=part,
                mine=True,
                maxResults=limit,
                pageToken=page_token
            ).execute()

        try:
            while len(items) < max_results:
                remaining = max_results - len(items)
                request_limit = min(remaining, 50)

                response = _fetch_page(next_page_token, request_limit)

                items.extend(response.get("items", []))
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

        except Exception:
            self.logger.error("[ERROR] Could not list playlists", exc_info=True)
        # parse items to return only relevant details
        playlists = [
            {
                'playlistId': item.get('id'),
                'publishedAt': item.get('snippet', {}).get('publishedAt'),
                'channelId': item.get('snippet', {}).get('channelId'),
                'title': item.get('snippet', {}).get('title'),
                'description': item.get('snippet', {}).get('description')
            }
            for item in items or []
        ]
        return playlists
    
    def create_playlist(self, title: str, description: str = "", privacy_status: str = "private") -> Dict:
        """
        Create a new playlist in the authenticated user's channel.

        Args:
            title (str): Title of the new playlist.
            description (str): Description of the new playlist.
            privacy_status (str): Privacy status of the playlist ('private', 'public', 'unlisted').
        Returns:
            Dict: The created playlist resource.
        """
        body = {
            "snippet": {
                "title": title,
                "description": description
            },
            "status": {
                "privacyStatus": privacy_status
            }
        }
        @retry_on_exception()
        def _call(bdy: dict):
            return self.yt_service.playlists().insert(
                part="snippet,status",
                body=bdy
            ).execute()

        try:
            response = _call(body)
        except Exception:
            self.logger.error("[ERROR] Could not create playlist", exc_info=True)
            return {}
        
        playlist_info = {
                'playlistId': response.get('id'),
                'publishedAt': response.get('snippet', {}).get('publishedAt'),
                'channelId': response.get('snippet', {}).get('channelId'),
                'channelTitle': response.get('snippet', {}).get('channelTitle'),
                'title': response.get('snippet', {}).get('title'),
                'description': response.get('snippet', {}).get('description'),
                'status': response.get('status', {}).get('privacyStatus')
        } if response else {}

        return playlist_info
    
    def update_playlist(self, playlist_id: str, title: str, description: str = ""):
        body = {
            "id": playlist_id,
            "snippet": {
                "title": title,
                "description": description
            }
        }
        @retry_on_exception()
        def _call(bdy: dict):
            return self.yt_service.playlists().update(
                part="snippet",
                body=bdy
            ).execute()
        try:
            response = _call(body)
        except Exception:
            self.logger.error(f"[ERROR] Could not update playlist {playlist_id}, check ownership", exc_info=True)
            return {}
        
        playlist_info = {
                'playlistId': response.get('id'),
                'publishedAt': response.get('snippet', {}).get('publishedAt'),
                'channelId': response.get('snippet', {}).get('channelId'),
                'channelTitle': response.get('snippet', {}).get('channelTitle'),
                'title': response.get('snippet', {}).get('title'),
                'description': response.get('snippet', {}).get('description'),
                'status': response.get('status', {}).get('privacyStatus')
        } if response else {}

        return playlist_info
    
    def delete_playlist(self, playlist_id: str) -> bool:
        """
        Delete a playlist by its ID if its owned by the authenticated user.
        Args:
            playlist_id (str): The ID of the playlist to delete.
        Returns:
            bool: True if deletion was successful, False otherwise.
        """
        @retry_on_exception()
        def _call(pid: str):
            return self.yt_service.playlists().delete(id=pid).execute()
        try:
            _call(playlist_id)
        except Exception:
            self.logger.error(f"[ERROR] Could not delete playlist {playlist_id}, check ownership", exc_info=True)
            return False
        return True

    def get_playlist_items(self, playlist_id: str) -> List[Dict]:
        """
        List all videos in a playlist (handles pagination).
        Returns a list of video IDs or None on error/empty.
        Args:
            playlist_id (str): The ID of the playlist to retrieve items from.
        Returns:
            List[str]: A list of video IDs in the playlist.
        """
        items = []
        next_page_token = None

        @retry_on_exception()
        def _fetch_page(page_token: str):
            return self.yt_service.playlistItems().list(
                part='snippet',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=page_token
            ).execute()

        try:
            while True:
                response = _fetch_page(next_page_token)
                items.extend(response.get("items", []))
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

        except Exception:
            self.logger.error("[ERROR] Could not list playlists", exc_info=True)
        
        playlist_items_info = [
            {
                'playlistItemId': item.get('id'),
                'publishedAt': item.get('snippet', {}).get('publishedAt'),
                'channelId': item.get('snippet', {}).get('channelId'),
                'channelTitle': item.get('snippet', {}).get('channelTitle'),
                'title': item.get('snippet', {}).get('title'),
                'description': item.get('snippet', {}).get('description'),
                'playlistId': item.get('snippet', {}).get('playlistId'),
                'videoId': item.get('snippet', {}).get('resourceId', {}).get('videoId'),
                'position': item.get('snippet', {}).get('position'),
                'videoOwnerChannelId': item.get('snippet', {}).get('videoOwnerChannelId'),
                'videoOwnerChannelTitle': item.get('snippet', {}).get('videoOwnerChannelTitle')
            } for item in items or []
        ]

        return playlist_items_info
    
    # Add/remove videos to/from playlists
    def add_video_to_playlist(self, video_id: str, playlist_id: str) -> Dict:
        """
        Add a video to a playlist of authenticated user.
        Args:
            video_id (str): The ID of the video to add to the playlist.
            playlist_id (str): The ID of the playlist to add the video to.
        Returns:
            Dict: The created playlist item resource.
        """
        body = {
            "snippet": {
                "playlistId": playlist_id,
                "resourceId": {
                    "kind": "youtube#video",
                    "videoId": video_id
                }
            }
        }

        @retry_on_exception()
        def _call(bdy: dict):
            return self.yt_service.playlistItems().insert(
                part="snippet",
                body=bdy
            ).execute()
        
        try:
            response = _call(body)
        except Exception:
            self.logger.error(f"[ERROR] Could not add video {video_id} to playlist {playlist_id}", exc_info=True)
            return {}
        
        playlist_item_info = {
                'playlistItemId': response.get('id'),
                'publishedAt': response.get('snippet', {}).get('publishedAt'),
                'channelId': response.get('snippet', {}).get('channelId'),
                'channelTitle': response.get('snippet', {}).get('channelTitle'),
                'title': response.get('snippet', {}).get('title'),
                'description': response.get('snippet', {}).get('description'),
                'playlistId': response.get('snippet', {}).get('playlistId'),
                'videoId': response.get('snippet', {}).get('resourceId', {}).get('videoId'),
                'position': response.get('snippet', {}).get('position'),
                'videoOwnerChannelId': response.get('snippet', {}).get('videoOwnerChannelId'),
                'videoOwnerChannelTitle': response.get('snippet', {}).get('videoOwnerChannelTitle')
        } if response else {}
        
        return playlist_item_info

    def get_playlist_item_id(self, video_id: str, playlist_id: str) -> str | None:
        """
        Get the playlist item ID for a given video ID in a specific playlist.
        Args:
            video_id (str): The ID of the video.
            playlist_id (str): The ID of the playlist.
        Returns:
            str | None: The playlist item ID if found, None otherwise.
        """
        playlist_items = self.get_playlist_items(playlist_id)
        if not playlist_items:
            return None
        for item in playlist_items:
            try:
                if item.get('videoId') == video_id:
                    return item.get('playlistItemId')
            except KeyError:
                continue                
        return None

    def remove_video_from_playlist(self, video_id: str, playlist_id: str) -> bool:
        """
        Convenience wrapper: remove video from a playlist using video id and the corresponding playlist id.
        Finds the playlist item id and deletes it. Returns True if delete is successful or False.
        Args:
            video_id (str): The ID of the video to remove from the playlist.
            playlist_id (str): The ID of the playlist to remove the video from.
        Returns:
            bool: True if removal was successful, False otherwise.
        """
        playlist_item_id = self.get_playlist_item_id(video_id, playlist_id)
        if not playlist_item_id:
            self.logger.warning(f"Video {video_id} not found in playlist {playlist_id}")
            return False
        return self.delete_playlist_item(playlist_item_id)

    def delete_playlist_item(self, playlist_item_id: str) -> bool:
        """
        Remove a video from a playlist by its playlist item ID if it's owned by the authenticated user.
        Args:
            playlist_item_id (str): The ID of the playlist item to remove. Be specific with your playlist item id and not playlist id.
        Returns:
            bool: True if removal was successful, False otherwise.
        """
        @retry_on_exception()
        def _call(item_id: str):
            return self.yt_service.playlistItems().delete(id=item_id).execute()
        try:
            _call(playlist_item_id)
        except Exception:
            self.logger.error(f"[ERROR] Could not remove playlist item {playlist_item_id}, check ownership", exc_info=True)
            return False

        return True
    
    # Comments
    def get_comments(self, video_id: str, max_results: int = 50) -> List[Dict]:
        """
        Fetches up to `max_results` comments for a given video ID.
        Args:
            video_id (str): The ID of the video to retrieve comments for.
            max_results (int): Maximum number of comments to retrieve.
        Returns:
            List[Dict]: A list of comment resource dictionaries.
        """
        items = []
        next_page_token = None

        @retry_on_exception()
        def _fetch_page(vid: str, page_token: str | None, mr: int):
            return self.yt_service.commentThreads().list(
                part="snippet",
                videoId=vid,
                maxResults=mr,
                pageToken=page_token,
                textFormat="plainText"
            ).execute()

        try:
            while len(items) < max_results:
                remaining = max_results - len(items)
                request_limit = min(remaining, 50)

                response = _fetch_page(video_id, next_page_token, request_limit)
                items.extend(response.get('items', []))
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
        except Exception:
            self.logger.error(f"[ERROR] Could not fetch comments for video: '{video_id}'", exc_info=True)
            return []
        
        # parse items to return only relevant details
        comments = [
            {
                'commentId': item.get('id'),
                'videoId': item.get('snippet', {}).get('videoId'),
                'channelId': item.get('snippet', {}).get('channelId'),
                'totalReplyCount': item.get('snippet', {}).get('totalReplyCount'),
                'textDisplay': item.get('snippet', {}).get('topLevelComment', {}).get('snippet', {}).get('textDisplay'),
                'textOriginal': item.get('snippet', {}).get('topLevelComment', {}).get('snippet', {}).get('textOriginal'),
                'authorChannelId': item.get('snippet', {}).get('topLevelComment', {}).get('snippet', {}).get('authorChannelId', {}).get('value'),
                'likeCount': item.get('snippet', {}).get('topLevelComment', {}).get('snippet', {}).get('likeCount'),
                'publishedAt': item.get('snippet', {}).get('topLevelComment', {}).get('snippet', {}).get('publishedAt'),
                'updatedAt': item.get('snippet', {}).get('topLevelComment', {}).get('snippet', {}).get('updatedAt')
            }
            for item in items or []
        ]
        return comments
    
    def post_comment(self, video_id: str, text: str) -> Dict:
        """
        Post a comment on a video.
        Args:
            video_id (str): The ID of the video to comment on.
            text (str): The text of the comment.
        Returns:
            Dict: The created comment resource.
        """
        body = {
            "snippet": {
                "videoId": video_id,
                "topLevelComment": {
                    "snippet": {
                        "textOriginal": text
                    }
                }
            }
        }
        @retry_on_exception()
        def _call(bdy: dict):
            return self.yt_service.commentThreads().insert(
                part="snippet",
                body=bdy
            ).execute()

        return _call(body)
    
    # Get video tags and transcript (captions)
    def get_video_tags(self, video_id: str) -> List[str]:
        """
        Get tags for a single video id.
        Args:
            video_id (str): The ID of the video to retrieve tags for.
        Returns:
            List[str] | None: A list of tags or empty list if no tags are found.
        """        
        return self.get_video_info(video_id).get('tags', [])
    
    # Get video topics
    def get_video_topics(self, video_id: str) -> List[str]:
        """
        Get topics for a single video id.
        Args:
            video_id (str): The ID of the video to retrieve topics for.
        Returns:
            List[str] | None: A list of tags or  empty list if no topics are found.
        """        
        return self.get_video_info(video_id).get('topicDetails', [])
    
    def fetch_transcript(self, video_id: str, languages=['en']) -> str | None:
        """
        Fetch the transcript for a video if available.
        Args:
            video_id (str): The ID of the video to retrieve the transcript for.
        Returns:
            str | None: The transcript text if available, None otherwise.
        """

        # Using youtube-transcript-api to fetch transcripts
        # Install with: pip install youtube-transcript-api
        from youtube_transcript_api import YouTubeTranscriptApi
        ytt_api = YouTubeTranscriptApi()
        # returns a FetchedTranscript object 
        # FetchedTranscript(
        #     snippets=[
        #         FetchedTranscriptSnippet(
        #             text="Hey there",
        #             start=0.0,
        #             duration=1.54,
        #         ),
        #         FetchedTranscriptSnippet(
        #             text="how are you",
        #             start=1.54,
        #             duration=4.16,
        #         ),
        #         # ...
        #     ],
        #     video_id="12345",
        #     language="English",
        #     language_code="en",
        #     is_generated=False,
        # )
        # fetched_transcript.to_raw_data(), which will return a list of dictionaries:
        # [
        #     {
        #         'text': 'Hey there',
        #         'start': 0.0,
        #         'duration': 1.54
        #     },
        #     {
        #         'text': 'how are you',
        #         'start': 1.54
        #         'duration': 4.16
        #     },
        #     # ...
        # ]

        fetched_transcript = ytt_api.fetch(video_id=video_id, languages=languages)

        return " ".join([snippet.text for snippet in fetched_transcript]) if fetched_transcript else ""

    def get_captions(self, video_id: str):
        """
        Get the captions for a video if available.
        Args:
            video_id (str): The ID of the video to retrieve the transcript for.
        Returns:
            str | None: The transcript text if available, None otherwise.
        """
        try:
            @retry_on_exception()
            def _list(vid: str):
                return self.yt_service.captions().list(
                    part="snippet",
                    videoId=vid
                ).execute()

            response = _list(video_id)
            items = response.get("items", [])

            # TODO: Implement caption download and parsing
            # captions = []
            # for item in items:
            #     if item.get('snippet', {}).get('language') == 'en':
            #         item_id = item.get('id')

                    # @retry_on_exception()
                    # def _download(cid: str):
                    #     return self.yt_service.captions().download(id=cid).execute()
            
                    # captions.append(_download(item_id))

        except Exception:
            self.logger.debug(f"No transcript available for {video_id}", exc_info=True)
            return None
        caption_info = [
            {
                'captionId': item.get('id'),
                'language': item.get('snippet', {}).get('language'),
                'videoId': item.get('snippet', {}).get('videoId'),
                'trackKind': item.get('snippet', {}).get('trackKind'),
                'lastUpdated': item.get('snippet', {}).get('lastUpdated'),
                'name': item.get('snippet', {}).get('name'),
                'language': item.get('snippet', {}).get('language')
            } for item in items or []
        ]
        return caption_info
    
    # Like/dislike video
    def get_liked_videos(self, max_results: int = 50) -> List[Dict]:
        """
        Fetches up to `max_results` liked videos for the authenticated user.

        Args:
            max_results (int): Maximum number of liked videos to retrieve.

        Returns:
            List[str]: A list of liked video IDs.
        """
        if max_results <= 0:
            return []

        items = []
        next_page_token = None

        # Ensure we have the user's channel info to get the liked playlist ID
        channels = self.get_user_channel()
        if not channels:
            return []

        liked_playlist_id = channels[0].get('likedPlaylist')

        @retry_on_exception()
        def _fetch_page(pid: str, page_token: str | None, limit: int):
            return self.yt_service.playlistItems().list(
                part="id,snippet,contentDetails",
                playlistId=pid,
                maxResults=limit,
                pageToken=page_token
            ).execute()

        try:
            while len(items) < max_results:
                remaining = max_results - len(items)
                request_limit = min(remaining, 50)

                response = _fetch_page(liked_playlist_id, next_page_token, request_limit)

                items.extend(response.get("items", []))
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

        except Exception:
            self.logger.error("[ERROR] Could not list liked videos", exc_info=True)
            return []
        # parse items to return only relevant details
        liked_videos = [
             {
                'playlistItemId': item.get('id'),
                'videoId': item.get('snippet', {}).get('resourceId', {}).get('videoId'),
                'videoChannelId': item.get('snippet', {}).get('videoOwnerChannelId'),
                'videoChannelTitle': item.get('snippet', {}).get('videoOwnerChannelTitle'),
                'videoTitle': item.get('snippet', {}).get('title'),
                'videoDescription': item.get('snippet', {}).get('description'),
                'videoPublishedAt': item.get('contentDetails', {}).get('videoPublishedAt'),
                'userId': item.get('snippet', {}).get('channelId'),
                'userTitle': item.get('snippet', {}).get('channelTitle'),
                'userLikedAt': item.get('snippet', {}).get('publishedAt'),
                'playlistId': item.get('snippet', {}).get('playlistId')
            }
            for item in items or []
        ]

        return liked_videos
    
    def like_video(self, video_id: str) -> bool:
        """
        Like a video on behalf of the authenticated user.
        Args:
            video_id (str): The ID of the video to like.
        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        try:
            self.yt_service.videos().rate(
                id=video_id,
                rating="like"
            ).execute()
        except Exception:
            self.logger.error(f"[ERROR] Could not like video {video_id}", exc_info=True)
            return False
        
        return True

    def dislike_video(self, video_id: str) -> bool:
        """
        Dislike a video on behalf of the authenticated user.
        Args:
            video_id (str): The ID of the video to dislike.
        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        try:
            self.yt_service.videos().rate(
                id=video_id,
                rating="dislike"
            ).execute()
        except Exception:
            self.logger.error(f"[ERROR] Could not like video {video_id}", exc_info=True)
            return False
        
        return True
    
    def unrate_video(self, video_id: str) -> bool:
        """
        Unrate a video on behalf of the authenticated user basically neutral rating, neither like nor dislike.
        Args:
            video_id (str): The ID of the video to dislike.
        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        try:
            self.yt_service.videos().rate(
                id=video_id,
                rating="none"
            ).execute()
        except Exception:
            self.logger.error(f"[ERROR] Could not like video {video_id}", exc_info=True)
            return False
        
        return True

    # Manage subscriptions
    def subscribe_to_channel(self, channel_id: str) -> Dict:
        """
            Subscribe to a channel by its channel ID.
            Returns the subscription resource on success or None on error.
            Args:
                channel_id (str): The ID of the channel to subscribe to.
            Returns:
                Dict: The created subscription resource.
        """
        body = {
            "snippet": {
                "resourceId": {
                    "kind": "youtube#channel",
                    "channelId": channel_id
                }
            }
        }
        @retry_on_exception()
        def _call(bdy: dict):
            return self.yt_service.subscriptions().insert(
                part="snippet",
                body=bdy
            ).execute()

        try:
            response = _call(body)
        except Exception:
            self.logger.error(f"[ERROR] Could not subscribe to channel {channel_id}", exc_info=True)
            return {}

        subscription_info = {
            'subscriptionId': response.get('id'),
            'channelId': response.get('snippet', {}).get('resourceId', {}).get('channelId'),
            'title': response.get('snippet', {}).get('title'),
            'description': response.get('snippet', {}).get('description'),
            'publishedAt': response.get('snippet', {}).get('publishedAt')
        } if response else {}

        return subscription_info
    
    def list_subscriptions(self, max_results: int = 50):
        """
        List all subscriptions for the authenticated user (handles pagination).
        Returns a list of subscription resources or None on error/empty.
        """
        if max_results <= 0:
            return []

        items = []
        next_page_token = None

        @retry_on_exception()
        def _fetch_page(page_token: str, limit: int):
            return self.yt_service.subscriptions().list(
                part="id,snippet,contentDetails",
                mine=True,
                maxResults=limit,
                pageToken=page_token
            ).execute()

        try:
            while len(items) < max_results:
                remaining = max_results - len(items)
                request_limit = min(remaining, 50)

                response = _fetch_page(next_page_token, request_limit)

                items.extend(response.get("items", []))
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

        except Exception:
            self.logger.error("[ERROR] Could not list playlists", exc_info=True)
        # parse items to return only relevant details
        subscriptions = [
            {
                'subscriptionId': item.get('id'),
                'channelId': item.get('snippet', {}).get('resourceId', {}).get('channelId'),
                'title': item.get('snippet', {}).get('title'),
                'description': item.get('snippet', {}).get('description'),
                'publishedAt': item.get('snippet', {}).get('publishedAt'),
                'totalItemCount': item.get('contentDetails', {}).get('totalItemCount')
            }
            for item in items or []
        ]
        return subscriptions

    def get_subscription_id_for_channel(self, channel_id: str):
        """
        Find the subscription resource id for a given channel_id (if the authenticated user is subscribed).
        Returns subscription id (string) or None if not found.
        """
        subs = self.list_subscriptions()
        if not subs:
            return None
        for s in subs:
            try:
                if s.get('channelId') == channel_id:
                    return s.get('subscriptionId')
            except KeyError:
                continue
        return None

    def delete_subscription(self, subscription_id: str) -> bool:
        """
        Delete a subscription by its subscription resource id.
        Returns API response on success or None on error.
        """
        @retry_on_exception()
        def _call(sub_id: str):
            return self.yt_service.subscriptions().delete(id=sub_id).execute()

        try:
            _call(subscription_id)
        except Exception:
            self.logger.error(f"[ERROR] Could not delete subscription {subscription_id}", exc_info=True)
            return False
        
        return True

    def unsubscribe_from_channel(self, channel_id: str) -> bool:
        """
        Convenience wrapper: unsubscribe from a channel by channel id.
        Finds the subscription id and deletes it. Returns delete response or None.
        """
        sub_id = self.get_subscription_id_for_channel(channel_id)
        if not sub_id:
            self.logger.warning(f"No subscription found for channel {channel_id}")
            return False
        return self.delete_subscription(sub_id)
    
    # Search content
    def search_videos(self, query: str, max_results: int = 5) -> List[Dict]:
        """
        Search for videos by query string.
        Args:
            query (str): The search query string.
            max_results (int): Maximum number of results to return.
        Returns:
            List[Dict]: A list of video resource dictionaries.
        """        
        if max_results <= 0:
            return []

        items = []
        next_page_token = None

        @retry_on_exception()
        def _fetch_page(q: str, mr: int):
            return self.yt_service.search().list(
                q=q,
                part="snippet",
                maxResults=mr,
                type="id,video"
            ).execute()
        
        try:
            while len(items) < max_results:
                remaining = max_results - len(items)
                request_limit = min(remaining, 50)

                response = _fetch_page(query, request_limit)

                items.extend(response.get("items", []))
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

        except Exception:
            self.logger.error("Could not list videos", exc_info=True)
            return []
        
        videos = [
            {
                'videoId': item.get('id', {}).get('videoId'),
                'channelId': item.get('snippet', {}).get('channelId'),
                'channelTitle': item.get('snippet', {}).get('channelTitle'),
                'title': item.get('snippet', {}).get('title'),
                'description': item.get('snippet', {}).get('description'),
                'publishedAt': item.get('snippet', {}).get('publishedAt')
            } for item in items or []
        ]

        return videos
    
    def search_playlist(self, playlist_name, channel_id=None, max_results=50) -> List[Dict]:
        """
        Search for playlists by name, optionally filtering by channel ID.
        Args:
            playlist_name (str): The name of the playlist to search for.
            max_results (int): Maximum number of results to return.
            channel_id (str | None): Optional channel ID to filter the search.
        Returns:
            List[Dict]: A list of playlist resource dictionaries.
        """
        if max_results <= 0:
            return []

        items = []
        next_page_token = None

        @retry_on_exception()
        def _fetch_page(name: str, mr: int, cid: str | None):
            return self.yt_service.search().list(
                part="snippet",
                type="id,playlist",
                q=name,
                channelId=cid,
                maxResults=mr
            ).execute()
        
        try:
            while len(items) < max_results:
                remaining = max_results - len(items)
                request_limit = min(remaining, 50)

                response = _fetch_page(playlist_name, request_limit, channel_id)

                items.extend(response.get("items", []))
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

        except Exception:
            self.logger.error("Could not list playlists", exc_info=True)
            return []
        
        # parse items to return only relevant details
        playlists = [
            {
                'playlistId': item.get('id', {}).get('playlistId'),
                'channelId': item.get('snippet', {}).get('channelId'),
                'channelTitle': item.get('snippet', {}).get('channelTitle'),
                'title': item.get('snippet', {}).get('title'),
                'description': item.get('snippet', {}).get('description'),
                'publishedAt': item.get('snippet', {}).get('publishedAt')
            }
            for item in items or []
        ]
        return playlists
    
    # Additional methods can be added as needed
