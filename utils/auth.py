from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

def get_authenticated_service():
    """Authenticate and return a YouTube Data API service."""
    # Use the InstalledAppFlow to manage OAuth2 authentication
    flow = InstalledAppFlow.from_client_secrets_file(
        'client_secrets.json', SCOPES
    )
    # Use run_local_server to authenticate
    credentials = flow.run_local_server(port=0)
    return build('youtube', 'v3', credentials=credentials)
