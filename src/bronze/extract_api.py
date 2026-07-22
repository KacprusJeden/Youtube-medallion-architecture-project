from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
import calendar
from dags import watermark as wtm
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import traceback
from typing import List, Literal
import duckdb
import threading
import uuid

class ChannelError(Exception): pass
class VideoError(Exception): pass
class CommentError(Exception): pass

class YoutubeData:
    LAYER = "bronze"

    def __init__(self, api_key: str, *channels: str):
        self.api_key = api_key
        self.channels = channels
        self.duckdb_conn = duckdb.connect(database=':memory:')

    @property
    def youtube(self):
        if not hasattr(self, "_thread_local"):
            self._thread_local = threading.local()
            
        if not hasattr(self._thread_local, "client"):
            self._thread_local.client = build("youtube", "v3", developerKey=self.api_key)
            
        return self._thread_local.client

    # ==========================================
    # MODE 
    # ==========================================
    @classmethod
    def get_mode(self, date_of_data: str, reload: bool = False, reload_date: bool = False) -> Literal["full", "reload", "reload_date", "incremental"]:
        if reload_date:
            return "reload_date"
        if reload:
            return "reload"
        
        date_of_data_dt = datetime.strptime(date_of_data, "%Y-%m-%d").date()
        _, last_day = calendar.monthrange(date_of_data_dt.year, date_of_data_dt.month)
        return "full" if date_of_data_dt.day == last_day else "incremental"

    # ========================================= 
    # ETL METHODS & SAVING
    # =========================================
    def get_etl_metadata(self, channel: str) -> dict:
        return {
            "load_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"), 
            "source": "youtube_api", 
            "channel_handle": channel
        }

    def save_bronze(self, channel: str, dataset_type: str, data: List[dict], mode: str, status: str, date_of_data: str, run_id: str):
        if not data:
            return
        
        data_dir = wtm.get_data_dir_pattern(
            layer=self.LAYER,
            dataset_type=dataset_type,
            mode=mode,
            status=status,
            channel=channel,
            date_of_data=date_of_data,
            run_id=run_id
        )
        data_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = data_dir / f"{str(uuid.uuid4().hex)}.jsonl"
        with open(file_path, "w", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

    def extract_channel_info(self, run_id: str, channel: str, date_of_data: str, mode: str):
        try:
            request = self.youtube.channels().list(
                part="snippet,statistics,contentDetails",
                forUsername=channel.replace("@", "") if channel.startswith("@") else None,
                id=channel if not channel.startswith("@") else None
            )
            response = request.execute()
            
            if not response.get("items"):
                request = self.youtube.search().list(q=channel, type="channel", part="id")
                res = request.execute()
                if res.get("items"):
                    c_id = res["items"][0]["id"]["channelId"]
                    request = self.youtube.channels().list(part="snippet,statistics,contentDetails", id=c_id)
                    response = request.execute()

            items = response.get("items", [])
            if not items:
                raise ChannelError(f"Channel {channel} not found.")

            for item in items:
                item["etl_metadata"] = self.get_etl_metadata(channel) 

            self.save_bronze(channel, "channels", items, mode, "success", date_of_data, run_id)
            wtm.save_watermark(self.LAYER, channel, "channels", "run_id", run_id, run_id, date_of_data, mode, "success")
        
        except Exception as e:
            traceback.print_exc()
            wtm.save_watermark(self.LAYER, channel, "channels", "run_id", None, run_id, date_of_data, mode, "failed", e)
            raise e

    def get_videos_ids(self, channel: str) -> List[str]:
        try:
            request = self.youtube.channels().list(part="contentDetails", forUsername=channel.replace("@", ""))
            response = request.execute()
            if not response.get("items"):
                request = self.youtube.search().list(q=channel, type="channel", part="id")
                res = request.execute()
                c_id = res["items"][0]["id"]["channelId"]
                request = self.youtube.channels().list(part="contentDetails", id=c_id)
                response = request.execute()
            
            uploads_playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
            
            video_ids = []
            next_page_token = None
            
            while True:
                req = self.youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=uploads_playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                res = req.execute()
                video_ids.extend([item["contentDetails"]["videoId"] for item in res.get("items", [])])
                next_page_token = res.get("nextPageToken")
                if not next_page_token:
                    break
            return video_ids
        except Exception as e:
            print(f"Error getting video IDs: {e}")
            return []

    def extract_videos_info(self, video_ids: List[str], run_id: str, channel: str, date_of_data: str, mode: str):
        try:
            all_videos = []

            for i in range(0, len(video_ids), 50):
                batch_ids = video_ids[i:i+50]
                request = self.youtube.videos().list(
                    part="snippet,statistics,contentDetails",
                    id=",".join(batch_ids)
                )
                response = request.execute()
                all_videos.extend(response.get("items", []))

            for video in all_videos:
                video["etl_metadata"] = self.get_etl_metadata(channel)

            self.save_bronze(channel, "videos", all_videos, mode, "success", date_of_data, run_id)
            wtm.save_watermark(self.LAYER, channel, "videos", "run_id", run_id, run_id, date_of_data, mode, "success")
        except Exception as e:
            traceback.print_exc()
            wtm.save_watermark(self.LAYER, channel, "videos", "run_id", None, run_id, date_of_data, mode, "failed", e)
            raise e

    def get_comments_for_video(self, video_id: str, channel: str, last_watermark: str, mode: str) -> List[dict]:
        comments = []
        try:
            next_page_token = None
            while True:
                request = self.youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=next_page_token,
                    order="time"
                )
                response = request.execute()
                
                stop_pagination = False
                for item in response.get("items", []):
                    top_comment = item["snippet"]["topLevelComment"]["snippet"]
                    updated_at = top_comment.get("updatedAt") or top_comment.get("publishedAt")
                    
                    if mode == "incremental" and last_watermark and updated_at <= last_watermark:
                        stop_pagination = True
                        break
                    
                    comments.append({
                        "comment_id": item["id"],
                        "video_id": video_id,
                        "channel_id": channel,
                        "comment_author": top_comment.get("authorDisplayName"),
                        "comment_text": top_comment.get("textDisplay"),
                        "comment_like_count": top_comment.get("likeCount"),
                        "comment_publish_date": top_comment.get("publishedAt"),
                        "comment_update_date": top_comment.get("updatedAt"),
                        "comment_is_public": item["snippet"].get("isPublic"),
                        "comment_can_reply": item["snippet"].get("canReply"),
                        "comment_reply_count": item["snippet"].get("totalReplyCount"),
                        "watermark_date": updated_at,
                        "etl_metadata": self.get_etl_metadata(channel)
                    })
                
                next_page_token = response.get("nextPageToken")
                if not next_page_token or stop_pagination:
                    break
        except HttpError as e:
            if e.resp.status != 403:
                print(f"HTTP Error for video {video_id}: {e}")
        except Exception as e:
            print(f"General Error for video {video_id}: {e}")
        return comments

    def extract_comments_info(self, video_ids: List[str], run_id: str, channel: str, date_of_data: str, mode: str):
        last_watermark = None

        if mode == "incremental":
            try:
                wm_path = wtm.get_watermark_path(self.LAYER, channel, "comments")
                if wm_path.exists() and wm_path.stat().st_size > 0:
                    with open(wm_path, "r") as f:
                        lines = f.readlines()
                        if lines:
                            last_entry = json.loads(lines[-1])
                            if last_entry.get(f"youtube_comments", {}).get("status") == "success":
                                last_watermark = last_entry[f"youtube_comments"].get("watermark_value")
            except Exception as e:
                print(f"[WARN] Could not load last watermark for comments: {e}")

        all_comments = []
        all_dates = []
        batch_size = 500

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self.get_comments_for_video, vid, channel, last_watermark, mode): vid for vid in video_ids}
            
            for future in as_completed(futures):
                vid = futures[future]
                try:
                    result = future.result()
                    all_comments.extend(result)
                    all_dates.extend([c.get("watermark_date") for c in result if c.get("watermark_date")])
                    
                    if len(all_comments) >= batch_size:
                        self.save_bronze(channel, "comments", all_comments, mode, "success", date_of_data, run_id)
                        all_comments = []
                except Exception as e:
                    self.save_bronze(channel, "comments", [{"video_id": vid, "error": str(e)}], mode, "failed", date_of_data, run_id)
                    print(f"[WARN] Failed to get comments for video {vid}: {e}")
                    wtm.save_watermark(self.LAYER, channel, "comments", "watermark_date", None, run_id, date_of_data, mode, "failed", e)
        
            if all_comments:
                self.save_bronze(channel, "comments", all_comments, mode, "success", date_of_data, run_id)

        new_watermark = max(all_dates) if all_dates else last_watermark
        wtm.save_watermark(self.LAYER, channel, "comments", "watermark_date", new_watermark, run_id, date_of_data, mode, "success")