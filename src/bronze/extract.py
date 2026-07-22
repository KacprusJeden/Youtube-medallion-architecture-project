import os
from dotenv import load_dotenv
from src.bronze.extract_api import YoutubeData
from pathlib import Path
from typing import Literal


def process_channel(run_id: str, channel: str, date_of_data: str, mode: Literal["full", "reload", "reload_date", "incremental"] = "incremental"):
    try:
        base_dir = Path(__file__).resolve().parents[2]
        dotenv_path = base_dir / "config" / ".env"
        
        load_dotenv(dotenv_path=dotenv_path)
        
        yt = YoutubeData(os.getenv("YT_API_KEY"), channel)

        yt.extract_channel_info(run_id, channel, date_of_data, mode)

        video_ids = yt.get_videos_ids(channel)

        yt.extract_videos_info(video_ids, run_id, channel, date_of_data, mode)

        yt.extract_comments_info(video_ids, run_id, channel, date_of_data, mode)
    except Exception as e:
        raise e