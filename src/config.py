from pathlib import Path

# Project Root (2 Ebenen hoch vom aktuellen File)
ROOT = Path(__file__).resolve().parent.parent

# Folders
DATA = ROOT / "data"
ASSETS = ROOT / "assets"
SECRETS = ROOT / ".secrets"
CATEGORIES = ROOT / "categories"

# Data files
DESCRIPTION_FILE = DATA / "description.txt"
TAGS_FILE = DATA / "tags.txt"
EPISODE_FILE = DATA / "episodes.json"
GAMES_FILE = DATA / "games.json"

# Assets files
OVERLAY_PATH = ASSETS / "thumbnail_overlay.png"
INTRO_FONT_PATH = ASSETS / "Boldonse-Regular.ttf"
FONT_PATH = ASSETS  / "Anton-Regular.ttf"
INTRO_MUSIC_PATH = ASSETS / "smooth-bossa-beats.mp3"
OUTRO_MUSIC_PATH = ASSETS / "gentle-bossa-nova.mp3"

# Secrets
YOUTUBE_TOKEN_FILE = SECRETS / "youtube_token.json"
YOUTUBE_CREDS_FILE = SECRETS / "youtube_creds.json"
