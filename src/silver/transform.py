from src.silver import channel as ch
from src.silver import video as vid
from src.silver.comment import Transformer
from dags import watermark as wtm
from typing import List, Literal
import traceback

def transform_channels(run_id: str, channels: List[str], date_of_data: str,  
    mode: Literal["full", "reload", "reload_date", "incremental"] = "incremental"
):

    for channel in channels:
        try:
            dataset_type = "channels"
            bronze_wtm_path = wtm.get_watermark_path("bronze", channel, dataset_type)
            silver_wtm_path = wtm.get_watermark_path("silver", channel, dataset_type)
            ch.load_silver(run_id, mode, channel, bronze_wtm_path, silver_wtm_path, date_of_data) 

            dataset_type = "videos"
            bronze_wtm_path = wtm.get_watermark_path("bronze", channel, dataset_type)
            silver_wtm_path = wtm.get_watermark_path("silver", channel, dataset_type)
            vid.load_silver(run_id, mode, channel, bronze_wtm_path, silver_wtm_path, date_of_data)

            dataset_type = "comments"
            bronze_wtm_path = wtm.get_watermark_path("bronze", channel, dataset_type)
            silver_wtm_path = wtm.get_watermark_path("silver", channel, dataset_type)
            com = Transformer()
            com.load_silver(run_id, mode, channel, bronze_wtm_path, silver_wtm_path, date_of_data)
        except Exception as e:
            print(e)
            print(traceback.format_exc())

