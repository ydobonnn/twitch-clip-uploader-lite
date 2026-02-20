import httplib2
import os
import random
import time
from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow
from config import DESCRIPTION_FILE, TAGS_FILE, YOUTUBE_CREDS_FILE, YOUTUBE_TOKEN_FILE

YOUTUBE_UPLOAD_SCOPE = "https://www.googleapis.com/auth/youtube.upload https://www.googleapis.com/auth/youtube.force-ssl"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

MISSING_CLIENT_SECRETS_MESSAGE = f"""
WARNING: Please configure OAuth 2.0

To make this sample run you will need to populate the client_secrets.json file
found at:

   {os.path.abspath(os.path.join(os.path.dirname(__file__), YOUTUBE_CREDS_FILE))}

with information from the API Console
https://console.cloud.google.com/

For more information about the client_secrets.json file format, please visit:
https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
"""


# Explicitly tell the underlying HTTP transport library not to retry, since we handle retry logic.
httplib2.RETRIES = 1
MAX_RETRIES = 10
# Always retry on these exceptions.
RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError)
RETRIABLE_STATUS_CODES = [500, 502, 503, 504]

def get_authenticated_service():
    flow = flow_from_clientsecrets(
        str(YOUTUBE_CREDS_FILE),
        scope=YOUTUBE_UPLOAD_SCOPE,
        message=MISSING_CLIENT_SECRETS_MESSAGE
    )

    storage = Storage(str(YOUTUBE_TOKEN_FILE))  # Use fixed file path
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage)

    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, http=credentials.authorize(httplib2.Http()))

from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import time
import random
import datetime

# Add the new parameter for scheduled upload time
def initialize_upload(youtube, file, title, description, category, keywords, privacyStatus, 
                      thumbnail_path, playlist_name, scheduled_time=None):
    tags = keywords.split(",") if keywords else None

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags,
            "categoryId": category,
            "defaultLanguage": "en"
        },
        "status": {
            "privacyStatus": privacyStatus,
            "selfDeclaredMadeForKids": False
        }
    }

    # Only set 'publishAt' if a scheduled time is provided
    if scheduled_time:
        # Ensure the time is in ISO 8601 format and at least 1 hour in the future
        publish_time = datetime.datetime.strptime(scheduled_time, "%Y-%m-%dT%H:%M:%SZ")
        print(publish_time, datetime.datetime.utcnow())
        if publish_time > datetime.datetime.utcnow():
            print("publishAt", publish_time.isoformat() + "Z")
            body["status"]["publishAt"] = publish_time.isoformat() + "Z"
        else:
            print("Scheduled time has already passed, publishing video now.")

    insert_request = youtube.videos().insert(
        part=",".join(body.keys()),
        body=body,
        media_body=MediaFileUpload(file, chunksize=-1, resumable=True)
    )

    resumable_upload(youtube, insert_request, thumbnail_path, playlist_name)


def resumable_upload(youtube, insert_request, thumbnail_path, playlist_name=None):
    response = None
    error = None
    retry = 0
    while response is None:
        try:
            print("Uploading file...")
            status, response = insert_request.next_chunk()
            if response is not None:
                if "id" in response:
                    video_id = response["id"]
                    print(f"Video id '{video_id}' was successfully uploaded.")

                    # Set thumbnail after successful upload
                    if thumbnail_path:
                        set_thumbnail(youtube, video_id, thumbnail_path)

                    # Add video to playlist if a playlist name is provided
                    if playlist_name:
                        ensure_playlist_and_add_video(youtube, playlist_name, video_id)

                else:
                    exit(f"The upload failed with an unexpected response: {response}")
        except HttpError as e:
            if e.resp.status in RETRIABLE_STATUS_CODES:
                error = f"A retriable HTTP error {e.resp.status} occurred:\n{e.content}"
            else:
                raise
        except RETRIABLE_EXCEPTIONS as e:
            error = f"A retriable error occurred: {e}"

        if error is not None:
            print(error)
            retry += 1
            if retry > MAX_RETRIES:
                exit("No longer attempting to retry.")

            max_sleep = 2**retry
            sleep_seconds = random.random() * max_sleep
            print(f"Sleeping {sleep_seconds} seconds and then retrying...")
            time.sleep(sleep_seconds)

# Main upload function now with scheduled_upload_time
def upload_video(youtube, file, title="Test Title", description="Test Description", category="22", keywords="", 
                 privacyStatus="public", thumbnail_path=None, playlist_name=None, scheduled_upload_time=None):
    
    # If scheduled_upload_time is provided, ensure it is in the correct format (UTC string)
    if scheduled_upload_time:
        if isinstance(scheduled_upload_time, datetime.datetime):
            scheduled_upload_time = scheduled_upload_time.strftime('%Y-%m-%dT%H:%M:%SZ')  # Convert to UTC string
        elif isinstance(scheduled_upload_time, str):
            pass  # Already in the correct format
    
    try:
        initialize_upload(youtube, file, title, description, category, keywords, privacyStatus, 
                          thumbnail_path, playlist_name, scheduled_upload_time)
    except HttpError as e:
        print(f"An HTTP error {e.resp.status} occurred:\n{e.content}")


def set_thumbnail(youtube, video_id, thumbnail_path):
    try:
        youtube.thumbnails().set(
            videoId=video_id,
            media_body=MediaFileUpload(thumbnail_path)
        ).execute()
        print(f"Thumbnail '{thumbnail_path}' set for video ID {video_id}")
    except HttpError as e:
        print(f"An error occurred while setting thumbnail: {e}")

def create_playlist(youtube, title, description, privacyStatus="private"):
    request = youtube.playlists().insert(
        part="snippet,status",
        body={
            "snippet": {
                "title": title,
                "description": description
            },
            "status": {
                "privacyStatus": privacyStatus
            }
        }
    )
    
    response = request.execute()
    print(f"Playlist '{title}' created successfully! Playlist ID: {response['id']}")
    return response["id"]  # Returns the playlist ID

def add_video_to_playlist(youtube, playlist_id, video_id):
    request = youtube.playlistItems().insert(
        part="snippet",
        body={
            "snippet": {
                "playlistId": playlist_id,
                "resourceId": {
                    "kind": "youtube#video",
                    "videoId": video_id
                }
            }
        }
    )
    
    response = request.execute()
    print(f"Video '{video_id}' added to Playlist '{playlist_id}' successfully!")
    return response

def get_playlist_id_by_name(youtube, playlist_name):
    request = youtube.playlists().list(
        part="snippet",
        mine=True,  # Retrieves only playlists owned by the authenticated user
        maxResults=50  # Adjust if needed
    )
    response = request.execute()

    for playlist in response.get("items", []):
        if playlist["snippet"]["title"] == playlist_name:
            return playlist["id"]  # Return the first matching playlist ID

    print(f"No playlist found with the name '{playlist_name}'.")
    return None  # Return None if no matching playlist is found

def ensure_playlist_and_add_video(youtube, playlist_name, video_id):
    description = f"Complete playlist of {playlist_name} videos"

    """Checks if a playlist exists, creates it if needed, and adds a video to it."""
    
    # Check if the playlist exists
    playlist_id = get_playlist_id_by_name(youtube, playlist_name)

    # If it doesn't exist, create it
    if not playlist_id:
        print(f"Playlist '{playlist_name}' not found. Creating it now...")
        playlist_id = create_playlist(youtube, playlist_name, description, privacyStatus="public")

    # Add video to the found or newly created playlist
    add_video_to_playlist(youtube, playlist_id, video_id)


def create_description(game_name, episode_number, streamer_names, desc_path=DESCRIPTION_FILE):
    desc_template = ""
    with open(desc_path, "r") as desc_file:
        desc_template = desc_file.read().strip() 
    # Remove duplicates while keeping order
    
    # Create the formatted source lines
    streamer_sources = "\n".join([f"twitch.tv/{streamer}" for streamer in streamer_names])

    # Replace the placeholders with actual values
    desc = desc_template.format(game_name=game_name, episode_number=episode_number, streamer_sources=streamer_sources)

    return desc

def create_tags(game_name, streamer_names, tags_file=TAGS_FILE):
    """
    Generates a YouTube-friendly tags string using default tags, game name, and streamer names.

    :param game_name: (str) The name of the game being streamed.
    :param streamer_names: (list) A list of streamer names.
    :param default_tags_file: (str) Path to the file containing default tags.
    :return: (str) A comma-separated tags string for YouTube.
    """
    tags_list = []

    with open(tags_file, "r", encoding="utf-8") as f:
        default_tags = f.read().strip().split(",")
        tags_list.extend([tag.strip() for tag in default_tags if tag.strip()])

    # Add game name and streamer names
    if game_name:
        tags_list.append(game_name)
        tags_list.append(game_name+" Twitch")
        tags_list.append(game_name+" Twitch Clips")
        tags_list.append(game_name+" Clips")
        tags_list.append(game_name+" Highlights")
        tags_list.append(game_name+" Best Of")
        tags_list.append(game_name+" Funny Moments")
        tags_list.append(game_name+" Best Plays")
        tags_list.append(game_name+" Most Viewed Clips")
    # if streamer_names:
    #     tags_list.extend(streamer_names)

    # Remove duplicates while maintaining order
    unique_tags = list(dict.fromkeys(tags_list))

    # Limit total character count to 500
    final_tags = []
    total_length = 0

    for tag in unique_tags:
        if total_length + len(tag) + 1 <= 500:  # +1 for commas
            final_tags.append(tag)
            total_length += len(tag) + 1
        else:
            break  # Stop adding tags once the limit is reached

    return ",".join(final_tags)

def video_exists(youtube, title_to_check):
    """
    Checks if a video with the given title already exists on the channel.
    This includes scheduled, private, unlisted, and public videos.
    """
    try:
        # Step 1: Get the uploads playlist ID
        channels_response = youtube.channels().list(
            part="contentDetails",
            mine=True
        ).execute()

        uploads_playlist_id = channels_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        # Step 2: Fetch videos from uploads playlist
        next_page_token = None
        while True:
            playlist_response = youtube.playlistItems().list(
                playlistId=uploads_playlist_id,
                part="snippet",
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            for item in playlist_response["items"]:
                video_title = item["snippet"]["title"].strip().lower()
                if video_title == title_to_check.strip().lower():
                    # print(f"Video with title '{title_to_check}' already exists (including scheduled/unlisted/private).")
                    return True

            next_page_token = playlist_response.get("nextPageToken")
            if not next_page_token:
                break

        return False
    except HttpError as e:
        print(f"Error checking video existence: {e}")
        return False
