from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import requests
import yt_dlp
import os

# Function to get access token (using your existing code)
def get_access_token(client_id, client_secret):
    url = 'https://id.twitch.tv/oauth2/token'
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    return response.json()['access_token']

# Function to get the game ID for Marvel Rivals
def get_game_id(game_name):
    url = "https://api.twitch.tv/helix/games"
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }
    params = {"name": game_name}
    response = requests.get(url, headers=headers, params=params)
    data = response.json().get("data", [])
    return data[0]["id"] if data else None

# Function to fetch top 10 most viewed clips in the last 7 days
def get_top_clips(game_id, days=7, limit=100, cursor=None):
    url = "https://api.twitch.tv/helix/clips"
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=days)
    params = {
        "game_id": game_id,
        "first": limit,
        "started_at": start_time.isoformat() + "Z",
        "ended_at": end_time.isoformat() + "Z",
    }

    # Only include cursor if it's provided (for pagination)
    if cursor:
        params["after"] = cursor  # Twitch API uses 'after' for pagination

    response = requests.get(url, headers=headers, params=params)
    return response.json() 

def get_top_clips_last_week(game_id, limit=100, cursor=None, end_time=None):
    """
    Fetches top clips for a given game from the previous week (Monday 00:00 to Sunday 23:59 UTC).
    
    Parameters:
      - game_id (str): The ID of the game.
      - limit (int): The maximum number of clips to fetch in one request.
      - cursor (str): The pagination cursor for the API request.

    Returns:
      - A response object containing clips from the last week.
    """
    if end_time is None:
        # Get the current time (UTC)
        end_time = datetime.now(timezone.utc)
        print("Using current time as end time:", end_time)

    # Calculate the start and end time for the previous week (Monday 00:00 to Sunday 23:59 UTC)
    # Find the most recent Monday
    start_of_week = end_time - timedelta(days=end_time.weekday())  # Monday of this week
    start_of_last_week = start_of_week - timedelta(weeks=1)  # Monday of last week
    end_of_last_week = start_of_last_week + timedelta(days=6)  # Sunday of last week

    # Set both times to 00:00 UTC for the start (Monday) and 23:59 for the end (Sunday)
    start_of_last_week = start_of_last_week.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_last_week = end_of_last_week.replace(hour=23, minute=59, second=59, microsecond=999999)

     # Convert to ISO 8601 format (Twitch expects this format with offset, not "Z")
    start_time_str = start_of_last_week.isoformat()
    end_time_str = end_of_last_week.isoformat()
    print(start_time_str, end_time_str)
    # Set the parameters for the request
    params = {
        "game_id": game_id,
        "first": limit,
        "started_at": start_time_str,
        "ended_at": end_time_str,
        "after": cursor  # Handle pagination
    }

    # Make the API call to get top clips for the given game and time range
    url = "https://api.twitch.tv/helix/clips"
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }

    response = requests.get(url, headers=headers, params=params)
    return response.json()  # Return the API response as JSON

def get_english_clips(game_id, desired_count=10, today=None):
    """
    Fetches up to `desired_count` English clips for a given game by paginating through results.

    Parameters:
      - game_id (str): The ID of the game.
      - days (int): Number of past days to fetch clips from.
      - desired_count (int): Number of English clips you want.
      - batch_size (int): Number of clips per API request (typically 100).

    Returns:
      - A list of clip dictionaries (English clips) with a length of up to desired_count.
    """
    batch_size=100
    all_english_clips = []
    cursor = None  # The pagination cursor provided by the API

    while len(all_english_clips) < desired_count:
        # Call the API to get a batch of clips.
        # The get_top_clips function must be adapted to accept a 'cursor' parameter.
        response = get_top_clips_last_week(game_id, limit=batch_size, cursor=cursor, end_time=today)

        # Assume the API response has a structure like:
        # { "data": [<clip1>, <clip2>, ...], "pagination": {"cursor": "..." } }
        if not response or "data" not in response:
            break  # No response or unexpected response structure

        clips_batch = response["data"]
        # Filter for English clips
        english_clips = [clip for clip in clips_batch if clip.get("language") == "en"]
        all_english_clips.extend(english_clips)

        # Check for a pagination cursor for the next batch
        pagination = response.get("pagination", {})
        if "cursor" in pagination:
            cursor = pagination["cursor"]
        else:
            break  # No more pages available

    # Return only the desired number of English clips, or as many as were available.
    return all_english_clips[:desired_count]

# Function to download a single clip
def download_clip(clip_id, save_path):
    clip_url = f"https://www.twitch.tv/clip/{clip_id}"
    ydl_opts = {
        'format': 'best',  # Best quality
        'outtmpl': save_path  # Save file to path
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([clip_url])
    print(f"Downloaded: {save_path}")

def download_clips(df, save_dir):
    os.makedirs(save_dir, exist_ok=True)  # Ensure save directory exists

    for index, row in df.iterrows():
        clip_id = row["clip_id"]
        save_path = os.path.join(save_dir, row["clip_filename"])
        # Check if the clip already exists
        if os.path.isfile(save_path):
            print(f"Clip '{row['clip_filename']}' already exists, skipping download.")
        else:
            download_clip(clip_id, save_path)

def get_clip_counts_for_length(clips, min_length=600, max_length=1200):
    """
    Returns the number of clips needed to reach at least min_length,
    and the number of clips before surpassing max_length.
    """
    cumulative_duration = 0
    min_clip_count = None
    max_clip_count = None

    for i, clip in enumerate(clips):
        cumulative_duration += clip["duration"]

        # First time cumulative duration reaches or exceeds min_length
        if min_clip_count is None and cumulative_duration >= min_length:
            min_clip_count = i + 1  # Convert index to count

        # Last clip before cumulative duration surpasses max_length
        if cumulative_duration < max_length:
            max_clip_count = i + 1  # Convert index to count

    return min_clip_count, max_clip_count

# Scaling function
def sigmoid_scaling(x, C=10000, D=20000, L=250):
    """Sigmoid-like scaling with a lower bound L"""
    return L + ((C - L) * x) / (x + D)

# Select clip count
def get_clip_count(clips):
    """
    Returns the number of clips where the view count is above a computed lower bound.

    :param clips: List of dictionaries, each containing "view_count" and "duration".
    :param lower_bound_factor: Factor to determine the lower bound for views.
    :return: The number of clips with views above the lower bound.
    """
    min_clips, max_clips = get_clip_counts_for_length(clips)

    # Get the views threshold for the clip that exceeds 600s cumulative length
    views_threshold = clips[9]["view_count"]

    # Compute the lower bound for view counts
    views_lower_bound = sigmoid_scaling(views_threshold)
    # print("10th clip:",views_threshold,"Lower bound:", views_lower_bound)
    # Count clips with views above the lower bound
    clip_count = sum(1 for clip in clips if clip["view_count"] > views_lower_bound)
    # print(min_clips, clip_count, max_clips)
    clip_count = max(min_clips, min(clip_count, max_clips))
    

    return clip_count

# Load environment variables from a .env file if present (for local use)
load_dotenv()

# Use environment variables (they will be pulled from GitHub Actions secrets in CI)
CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    raise ValueError("Missing TWITCH_CLIENT_ID or TWITCH_CLIENT_SECRET environment variable")

# Get access token
ACCESS_TOKEN = get_access_token(CLIENT_ID, CLIENT_SECRET)
print(f"Access Token: {ACCESS_TOKEN}")

# # Get game ID for "Marvel Rivals"
# GAME_NAME = "League of Legends"
# GAME_ID = get_game_id(GAME_NAME)
# print(f"Game ID for '{GAME_NAME}': {GAME_ID}")

# # # Get and display clips
# clips = get_english_clips(GAME_ID, desired_count=100)
# print(f"Found {len(clips)} Clips")


# # Download all clips to "clips" folder
# download_clips(clips, GAME_NAME)