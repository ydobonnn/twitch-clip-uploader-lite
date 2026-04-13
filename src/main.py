from create_video import create_thumbnail, create_video, get_clips_df
from youtube_manager import get_authenticated_service, upload_video, create_description, create_tags, video_exists, get_uploaded_titles
from config import CATEGORIES, GAMES_FILE
from datetime import datetime, timedelta, time, timezone
import argparse
import json
from multiprocessing import freeze_support
import traceback

SLOTS = [time(17, 0), time(21, 0)]  # 17:00 UTC and 21:00 UTC
EPISODE_START_DATE = datetime(2025, 3, 24, tzinfo=timezone.utc) # Monday, March 24, 2025
OUTCOME_UPLOADED = "uploaded"
OUTCOME_SKIPPED_NO_CLIPS = "skipped_no_clips"
OUTCOME_SKIPPED_EXISTS = "skipped_exists"

def get_episode_by_week(override_date=None):
    today = override_date or datetime.now(timezone.utc)
    weeks_elapsed = (today - EPISODE_START_DATE).days // 7
    return weeks_elapsed + 1

def get_scheduled_datetime(order_id, ref_date):
    week_start = ref_date - timedelta(days=ref_date.weekday())
    week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
    day_offset, slot_index = divmod(order_id, 2)
    scheduled_date = week_start + timedelta(days=day_offset)
    return datetime.combine(scheduled_date, SLOTS[slot_index], tzinfo=timezone.utc)

def create_and_upload_video(ref_date, game_name, game_id, scheduled_time, youtube=None, existing_titles=None, privacyStatus="private"):
    episode_number = get_episode_by_week(ref_date)
    game_folder = CATEGORIES / game_name
    video_path = game_folder / "final_video.mp4"
    thumbnail_path = game_folder / "final_thumbnail.jpg"

    print(f"Current Game: '{game_name}', ID: {game_id}, Episode Number: {episode_number}, Scheduled Time: {scheduled_time}")

    clips_df = get_clips_df(game_id, ref_date)
    if clips_df.empty:
        print(f"Skipping '{game_name}' because not enough clips were found.")
        return OUTCOME_SKIPPED_NO_CLIPS
    streamer_names = list(dict.fromkeys(clips_df["streamer_name"].tolist()))
    title = f"{game_name} MOST VIEWED Twitch Clips of The Week! #{episode_number}"
    desc  = create_description(game_name, episode_number, streamer_names)
    tags = create_tags(game_name, streamer_names)

    print(f"Title: {title}\nTags: {tags}\nDescription: {desc}")

    if youtube is None:
        youtube = get_authenticated_service()

    if video_exists(youtube, title, cached_titles=existing_titles):
        print(f"Video already exists: {title}")
        return OUTCOME_SKIPPED_EXISTS

    create_video(game_name, clips_df, episode_number)
    create_thumbnail(video_path, thumbnail_path)

    uploaded_video_id = upload_video(youtube=youtube, file=video_path, title=title, description=desc, category="24", keywords=tags, privacyStatus=privacyStatus, thumbnail_path=thumbnail_path, playlist_name=game_name, scheduled_upload_time=scheduled_time)
    if not uploaded_video_id:
        raise RuntimeError(f"Upload failed for '{game_name}': no video ID was returned.")

    print(f"Video upload successful! ID: {uploaded_video_id}")
    if existing_titles is not None:
        existing_titles.add(title.strip().lower())
    return OUTCOME_UPLOADED

def create_and_upload_videos_for_games(ref_date, games, start_index=0, end_index=14):
    youtube = get_authenticated_service()
    existing_titles = get_uploaded_titles(youtube)
    selected_games = sorted(games, key=lambda x: x["order_id"])[start_index:end_index]
    if not selected_games:
        raise RuntimeError(f"No games selected for range start={start_index}, end={end_index}.")

    outcomes = {
        OUTCOME_UPLOADED: 0,
        OUTCOME_SKIPPED_NO_CLIPS: 0,
        OUTCOME_SKIPPED_EXISTS: 0,
    }
    failures = []

    for game in selected_games:
        try:
            scheduled_datetime = get_scheduled_datetime(game["order_id"], ref_date)
            outcome = create_and_upload_video(ref_date, game["name"], game["id"], scheduled_datetime, youtube=youtube, existing_titles=existing_titles)
            outcomes[outcome] = outcomes.get(outcome, 0) + 1
        except Exception as e:
            print(f"Error processing '{game['name']}' (ID: {game['id']}): {e}")
            traceback.print_exc()
            failures.append(f"{game['name']} ({game['id']}): {e}")

    print(
        "Batch summary: "
        f"{outcomes[OUTCOME_UPLOADED]} uploaded, "
        f"{outcomes[OUTCOME_SKIPPED_EXISTS]} skipped because they already exist, "
        f"{outcomes[OUTCOME_SKIPPED_NO_CLIPS]} skipped because not enough clips were found, "
        f"{len(failures)} failed."
    )

    if failures:
        raise RuntimeError("One or more games failed:\n" + "\n".join(failures))

if __name__ == "__main__":
    freeze_support()
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=7)
    parser.add_argument("--weeks-ago", type=int, default=0)
    args = parser.parse_args()

    with open(GAMES_FILE) as f:
        games = json.load(f)

    ref_date = datetime.now(timezone.utc) - timedelta(weeks=args.weeks_ago)
    create_and_upload_videos_for_games(ref_date, games, start_index=args.start, end_index=args.end)
