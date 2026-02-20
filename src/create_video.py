from process_clips import  create_intro, process_all_clips_one_command, process_all_clips, process_all_clips_multiprocessing
from twitch_manager import download_clips, get_english_clips, get_clip_count
from PIL import Image
import pandas as pd
import subprocess
import os
import tempfile
from pathlib import Path
from config import CATEGORIES, OVERLAY_PATH

def get_clips_df(game_id, ref_date):
    columns = ["clip_filename", "clip_name", "duration_sec", "views", "streamer_name", "clip_id"]
    all_clips = get_english_clips(game_id, desired_count=100, today=ref_date)
    print(f"Found {len(all_clips)} English Clips")
    clip_count = get_clip_count(all_clips)
    print(f"Clip Count: {clip_count}")
    if clip_count == 0:
        return pd.DataFrame(columns=columns)

    clips = all_clips[:clip_count]

    df = pd.DataFrame([
        {
            "clip_filename": f"{i+1}.mp4",
            "clip_name": clip["title"],
            "duration_sec": clip["duration"],
            "views": clip["view_count"],
            "streamer_name": clip["broadcaster_name"],
            "clip_id": clip["id"]
        } 
        for i, clip in enumerate(clips)
    ])
    return df

import time

def create_video(game_name, df, episode_number):
    if df.empty:
        print(f"No clips available for {game_name}; skipping video creation.")
        return
    start_time = time.time()  # Start timer
    folder_path = CATEGORIES / game_name
    # Download all clips to "clips" folder
    download_clips(df, folder_path)

    # Create intro and outro files
    create_intro(df, folder_path, game_name, episode_number)

    # Add text overlays to all clips
    process_all_clips_one_command(df, folder_path, remove_temp=True)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"✅ Video creation completed in {elapsed_time:.2f} seconds")

# Function to extract a frame from the video at a specific timestamp
def extract_frame(video_path, timestamp, output_image_path):
    """
    Extracts a frame from the video at the specified timestamp.
    :param video_path: The path to the video file
    :param timestamp: The timestamp to extract the frame (e.g., '00:00:05' for 5 seconds)
    :param output_image_path: The path to save the extracted frame
    """
    # FFmpeg command to extract a frame at the given timestamp
    command = [
        'ffmpeg', '-y', '-i', video_path,  # Input video file, overwrite if target exists
        '-ss', timestamp,  # Specify the timestamp (e.g., 5 seconds)
        '-vframes', '1',  # Extract a single frame
        output_image_path  # Output path for the frame
    ]
    
    subprocess.run(command, check=True)  # Execute the command

# Function to overlay the logo on the frame
def overlay_logo(frame_path, output_image_path, logo_path=OVERLAY_PATH):
    """
    Overlays the logo onto the extracted frame and saves the result.
    :param frame_path: The path to the extracted frame image
    :param logo_path: The path to the logo image
    :param output_image_path: The path to save the final image with logo
    """
    # Open the extracted frame and logo images
    frame = Image.open(frame_path)
    logo = Image.open(logo_path)

    # Resize the logo to a reasonable size based on the frame size (e.g., 10% of the frame's width)
    logo_width = int(frame.width * 0.35)  # Resize logo to 20% of the frame width
    logo_height = int(logo.height * (logo_width / logo.width))  # Maintain the aspect ratio
    logo = logo.resize((logo_width, logo_height))

    # Calculate position to place the logo (center of the frame)
    position = ((frame.width - logo.width) // 2, (frame.height - logo.height) // 2)
    # print(frame.width, frame.height, logo.width, logo.height, position)
    # Paste the logo on the frame with transparency (if the logo has an alpha channel)
    frame.paste(logo, position, logo)

    # Save the final image with the logo
    frame.save(output_image_path)

# Function to create a thumbnail (extract frame and overlay logo)
def create_thumbnail(video_path, output_image_path, timestamp='00:00:15'):
    """
    Creates a thumbnail by extracting a frame from the video and overlaying a logo.
    :param video_path: The path to the video file
    :param logo_path: The path to the logo image
    :param output_image_path: The path to save the final thumbnail
    """
    thumb_dir = Path(output_image_path).parent
    thumb_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix=".jpg", dir=thumb_dir, delete=False) as tmp:
        temp_frame_path = tmp.name

    # Extract a frame at timestamp from the video
    extract_frame(video_path, timestamp, temp_frame_path)

    # Overlay the logo on the extracted frame and save the final image
    overlay_logo(temp_frame_path, output_image_path)
    
    # Optionally remove the temporary extracted frame image
    try:
        os.remove(temp_frame_path)
    except OSError:
        pass