from concurrent.futures import ProcessPoolExecutor
from functools import partial
import json
import os
import re
import ffmpeg
import subprocess
import pandas as pd
from pathlib import Path
from config import ASSETS, INTRO_FONT_PATH, FONT_PATH, INTRO_MUSIC_PATH


def process_text(text):
    """Manually escapes special characters for safe usage in ffmpeg."""
    text = text.upper()
    text = re.sub(r"[^a-zA-Z0-9\s.,!?;:'\"()\[\]{}\-_/\\]", "", text)
    text = text.replace("'", "'\\\\\\''").replace(":", "\:")
    return text

def create_intro_intern(input_file, output_file, category_name, category_episode):
    """
    Creates an intro by extracting the first 'duration' seconds of the input video, reversing it,
    blurring it, adding a title text overlay, and adding background music with a fade-out effect.
    """
    duration = 5
    blur_strength = 10
    title_font_size = 90
    text_duration = duration - 0.75

    # Escape title text, splitting it into three lines
    title_line1 = process_text(f"{category_name}")
    title_line2 = "CLIPS OF THE WEEK"
    title_line3 = f"#{category_episode}"
    escaped_font_path = str(INTRO_FONT_PATH).replace('\\', '/').replace(':', r'\:')
    print("FONT PATH:", escaped_font_path)

    # Define the complex video filter chain with three lines of centered text
    vf_filter = (
        f"[0:v]trim=duration={duration},reverse,boxblur=luma_radius={blur_strength}:luma_power=1,"
        f"drawtext=text='{title_line1}':fontfile='{escaped_font_path}':fontcolor=white:fontsize={title_font_size}:"
        f"x=(w-text_w)/2:y=(h-text_h)/2-120:alpha='if(lt(t,{text_duration}),1,1-(t-{text_duration})/0.5)', "  # First line (centered vertically and horizontally)
        f"drawtext=text='{title_line2}':fontfile='{escaped_font_path}':fontcolor=white:fontsize={title_font_size}:" 
        f"x=(w-text_w)/2:y=(h-text_h)/2:alpha='if(lt(t,{text_duration}),1,1-(t-{text_duration})/0.5)', "  # Second line (centered horizontally)
        f"drawtext=text='{title_line3}':fontfile='{escaped_font_path}':fontcolor=white:fontsize={title_font_size}:" 
        f"x=(w-text_w)/2:y=(h-text_h)/2+120:alpha='if(lt(t,{text_duration}),1,1-(t-{text_duration})/0.5)'[v]"  # Third line (centered horizontally and below)
    )


    # Input the video and music, and apply the necessary filters
    video_input = ffmpeg.input(input_file, t=duration)
    audio_input = ffmpeg.input(INTRO_MUSIC_PATH, t=duration)  # Music for intro

    # Apply fade-out to audio
    audio_fade = audio_input.filter('afade', type='out', start_time=duration - 1, duration=1)

    # Output the combined video and audio (with the fade-out effect on the music)
    ffmpeg.output(
        video_input,  # Video stream (muted intro)
        audio_fade,  # Audio stream (background music with fade-out)
        output_file,  # Final output path
        vf=vf_filter,  # Apply the video filter (blur, reverse, etc.)
        vcodec="libx264",  # Video codec (same as process_clip_overlay)
        acodec="aac",  # Audio codec (same as process_clip_overlay)
        movflags="faststart",  # Optimize for web playback (same as process_clip_overlay)
        audio_bitrate="192k",  # Set audio bitrate for music (same as process_clip_overlay)
        ar="48000",  # Set audio sample rate (same as process_clip_overlay)
        r="60",  # Set video frame rate (same as process_clip_overlay)
        s="1920x1080",  # Set video resolution (same as process_clip_overlay)
        crf="23",  # Set constant rate factor for video quality (same as process_clip_overlay)
        preset="fast",  # Set encoding preset (same as process_clip_overlay)
        an=None  # Audio stream is already included (no need to add a separate one)
    ).run(overwrite_output=True)

def create_intro(df, folder_path, game_name, episode_number):
    """Create an intro.mp4 file in the folder from the first clip's file."""
    first_clip = df.iloc[0]["clip_filename"]
    first_file_path = Path(folder_path) / first_clip
    intro_output_path = Path(folder_path) / "intro.mp4"

    # Convert paths to forward-slash strings for ffmpeg compatibility
    first_file_path = str(first_file_path.resolve()).replace('\\', '/')
    intro_output_path = str(intro_output_path.resolve()).replace('\\', '/')

    create_intro_intern(first_file_path, intro_output_path, game_name, episode_number)

def build_ffmpeg_command(df, folder_path, intro_path, outro_path, output_filename="final_video.mp4", duration=8):
    """
    Build an FFmpeg command that uses a filter_complex script file and
    returns both the command list and the path to the filter file.
    """
    filter_parts = []
    input_map = ["[0:v][0:a]"]  # intro is input 0

    font_path_escaped = str(Path(FONT_PATH).resolve()).replace('\\', '/').replace(':', r'\:')

    for pos, (_, row) in enumerate(df.iterrows()):
        input_idx = pos + 1  # inputs start after intro (0)
        name_text = "| " + process_text(row['streamer_name'])
        audio_filter = "acompressor=threshold=-20dB:ratio=7:attack=5:release=50"

        video_filter = (
            f"[{input_idx}:v]scale=1920:1080,setsar=1,fps=60,format=yuv420p,drawtext="
            f"text='{name_text}':fontfile='{font_path_escaped}':"
            f"fontcolor=white:fontsize=70:x=-10:y=h-th-175:"
            f"box=1:boxcolor=black@0.75:boxborderw=5|20|8|20:"
            f"alpha='if(lt(t,1),t,if(lt(t,{duration}),1,1-(t-{duration})/0.5))'"
            f"[v{input_idx}]"
        )
        audio_chain = f"[{input_idx}:a]{audio_filter}[a{input_idx}]"

        filter_parts.extend([video_filter, audio_chain])
        input_map.append(f"[v{input_idx}][a{input_idx}]")

    outro_idx = len(df) + 1
    input_map.append(f"[{outro_idx}:v][{outro_idx}:a]")  # outro input

    concat_filter = f"{' '.join(input_map)}concat=n={len(df)+2}:v=1:a=1[outv][outa]"
    filter_parts.append(concat_filter)

    filter_text = ";\n".join(filter_parts)

    filter_file = Path(folder_path) / "filter_complex.txt"
    filter_file.write_text(filter_text, encoding="utf-8")

    cmd = [
        "ffmpeg", "-y",
        "-i", str(intro_path)
    ]

    for _, row in df.iterrows():
        cmd.extend(["-i", str(folder_path / row["clip_filename"])])

    cmd.extend(["-i", str(outro_path)])
    cmd.extend([
        "-filter_complex_script", str(filter_file),
        "-map", "[outv]",
        "-map", "[outa]",
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-c:a", "aac", "-b:a", "192k", "-ar", "48000",
        "-movflags", "+faststart",
        str(folder_path / output_filename)
    ])

    return cmd, str(filter_file)


def filter_clips_with_audio(df, folder_path):
    folder_path = Path(folder_path)
    filtered_rows = []
    for _, row in df.iterrows():
        clip_path = folder_path / row['clip_filename']
        if has_audio(str(clip_path)):
            filtered_rows.append(row)
        else:
            print(f"❌ Skipping muted or silent clip: {clip_path}")
    return pd.DataFrame(filtered_rows)


def process_all_clips_one_command(df, folder_path, remove_temp=True):
    folder_path = Path(folder_path)
    df = filter_clips_with_audio(df, folder_path)

    intro_path = folder_path / "intro.mp4"
    outro_path = ASSETS / "outro.mp4"

    command, filter_file = build_ffmpeg_command(df, folder_path, intro_path, outro_path)

    print("Executing FFmpeg command:")
    print(" ".join(command))
    print("Filter file:", filter_file)

    try:
        subprocess.run(command, check=True)
        print("FFmpeg finished successfully.")
    finally:
        # Best effort removal of filter file
        try:
            if filter_file and os.path.exists(filter_file):
                os.remove(filter_file)
                print(f"Removed filter file: {filter_file}")
        except Exception:
            pass

    if remove_temp:
        keep = {"final_video.mp4", "final_thumbnail.jpg"}
        for f in folder_path.iterdir():
            if f.is_file() and f.name not in keep:
                try:
                    f.unlink()
                    print(f"Removed {f.name}")
                except Exception as e:
                    print(f"⚠️ Could not remove {f.name}: {e}")


def process_clip_overlay(input_clip, streamer_name, output_clip):
    duration = 8
    display_name = "| " + streamer_name
    input_clip = f'"{input_clip}"'
    output_clip = f'"{output_clip}"'

    font_path_escaped = str(Path(FONT_PATH).resolve()).replace('\\', '/').replace(':', r'\:')
    audio_compressor = "acompressor=threshold=-20dB:ratio=7:attack=5:release=50"

    command = (
        f'ffmpeg -y -threads 1 -i {input_clip} '
        f'-vf "scale=1920:1080,fps=60,format=yuv420p,'
        f'drawtext=text=\'{display_name}\':fontfile=\'{font_path_escaped}\':fontcolor=white:fontsize=70:x=-10:y=h-th-175:'
        f'box=1:boxcolor=black@0.75:boxborderw=5|20|8|20:'
        f'alpha=\'if(lt(t,1),t,if(lt(t,{duration}),1,1-(t-{duration})/0.5))\'" '
        f'-af "{audio_compressor}" '
        f'-c:v libx264 -preset fast -crf 23 -c:a aac -b:a 192k -ar 48000 -ac 2 -movflags +faststart {output_clip}'
    )

    print(f"Running command: {command}")
    subprocess.run(command, shell=True, check=True)


def has_audio(file_path):
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "a:0",
        "-show_entries", "stream=bit_rate",
        "-of", "json",
        file_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    try:
        data = json.loads(result.stdout)
        streams = data.get("streams", [])
        if not streams:
            print(f"DEBUG: No audio stream found in {file_path}")
            return False

        bit_rate = streams[0].get("bit_rate")
        return bit_rate is not None and int(bit_rate) > 10000
    except json.JSONDecodeError:
        print("DEBUG: Failed to parse ffprobe output")
        return False

def write_concat_file(clips, folder_path, intro_path, outro_path):
    concat_file = folder_path / "concat_list.txt"
    with concat_file.open("w", encoding="utf-8") as f:
        for clip in clips:
            if clip == "intro.mp4":
                path = intro_path.resolve()
            elif clip == "outro.mp4":
                path = outro_path.resolve()
            else:
                path = (folder_path / clip).resolve()
            f.write(f"file '{path}'\n")
    return concat_file

def process_single_clip(row, folder_path):
    folder_path = Path(folder_path)
    raw_clip = folder_path / row['clip_filename']
    match = re.search(r'(\d+)', row['clip_filename'])
    num = match.group(1) if match else "0"
    output_clip = folder_path / f"processed_{num}.mp4"

    if not output_clip.is_file():
        process_clip_overlay(str(raw_clip), process_text(row['streamer_name']), str(output_clip))

    if has_audio(str(output_clip)):
        return output_clip.name
    print(f"❌ Skipping muted or silent clip: {output_clip}")
    return None

def process_all_clips(df, folder_path, remove_temp=False, reencode_video=False):
    folder_path = Path(folder_path)
    intro_path = folder_path / "intro.mp4"
    outro_path = ASSETS / "outro.mp4"
    processed_clips = []

    if intro_path.is_file():
        processed_clips.append(intro_path.name)

    for _, row in df.iterrows():
        clip_name = process_single_clip(row, folder_path)
        if clip_name:
            processed_clips.append(clip_name)

    if outro_path.is_file():
        processed_clips.append(outro_path.name)

    concat_file = write_concat_file(processed_clips, folder_path, intro_path, outro_path)

    final_output = folder_path / "final_video.mp4"
    if reencode_video:
        concat_cmd = (
            f'ffmpeg -y -f concat -safe 0 -i "{concat_file}" '
            f'-vf "format=yuv420p" -c:v libx264 -preset fast -crf 23 '
            f'-c:a aac -b:a 192k -ar 48000 -movflags +faststart "{final_output}"'
        )
    else:
        concat_cmd = f'ffmpeg -y -f concat -safe 0 -i "{concat_file}" -c copy "{final_output}"'

    subprocess.run(concat_cmd, shell=True, check=True)
    print(f"✅ Final video created at: {final_output}")

    if remove_temp:
        keep_files = {"final_video.mp4", "final_thumbnail.jpg"}
        for file in folder_path.iterdir():
            if file.is_file() and file.name not in keep_files:
                try:
                    file.unlink()
                    print(f"🗑️ Removed: {file.name}")
                except Exception as e:
                    print(f"⚠️ Could not remove {file.name}: {e}")

def process_all_clips_multiprocessing(df, folder_path, remove_temp=False, reencode_video=False):
    folder_path = Path(folder_path)
    intro_path = folder_path / "intro.mp4"
    outro_path = ASSETS / "outro.mp4"

    processed_clips = []
    if intro_path.is_file():
        processed_clips.append(intro_path.name)

    clip_rows = df.to_dict(orient='records')
    with ProcessPoolExecutor(max_workers=2) as executor:
        clip_names = list(executor.map(partial(process_single_clip, folder_path=folder_path), clip_rows))

    processed_clips.extend([clip for clip in clip_names if clip])

    if outro_path.is_file():
        processed_clips.append(outro_path.name)

    concat_file = write_concat_file(processed_clips, folder_path, intro_path, outro_path)

    final_output = folder_path / "final_video.mp4"
    if reencode_video:
        concat_cmd = (
            f'ffmpeg -y -f concat -safe 0 -i "{concat_file}" '
            f'-vf "format=yuv420p" -c:v libx264 -preset fast -crf 23 '
            f'-c:a aac -b:a 192k -ar 48000 -movflags +faststart "{final_output}"'
        )
    else:
        concat_cmd = f'ffmpeg -y -f concat -safe 0 -i "{concat_file}" -c copy "{final_output}"'

    subprocess.run(concat_cmd, shell=True, check=True)
    print(f"✅ Final video created at: {final_output}")

    if remove_temp:
        keep_files = {"final_video.mp4", "final_thumbnail.jpg"}
        for file in folder_path.iterdir():
            if file.is_file() and file.name not in keep_files:
                try:
                    file.unlink()
                except Exception as e:
                    print(f"⚠️ Could not remove {file.name}: {e}")