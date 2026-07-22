import duckdb
from typing import Literal, Tuple
from os import getenv
from dotenv import load_dotenv
from dags import watermark as wtm
from pathlib import Path
from datetime import datetime
import traceback
import platform

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / "config/.env")

def env(key):
    value = getenv(key)
    if not value:
        raise ValueError(f"Missing env: {key}")
    return value


def get_current_silver(mode: Literal["full", "reload", "reload_date", "incremental"]
        ) -> Tuple[Path, Path, Path, Path, Path, Path, Path, Path]:

    ch_hub_path, ch_sat_path, _ = wtm.get_data_dir_pattern(
        layer="silver", dataset_type="channels",
        mode=mode, status="success", load_gold=True
    )
    
    vid_hub_path, vid_sat_path, vid_link_path = wtm.get_data_dir_pattern(
        layer="silver", dataset_type="videos",
        mode=mode, status="success", load_gold=True
    )
    
    com_hub_path, com_sat_path, com_link_path = wtm.get_data_dir_pattern(
        layer="silver", dataset_type="comments",
        mode=mode, status="success", load_gold=True
    )
    
    return (ch_hub_path, ch_sat_path, vid_hub_path, vid_sat_path, vid_link_path,
            com_hub_path, com_sat_path, com_link_path)


def load_statistics(
    mode: Literal["full", "reload", "reload_date", "incremental"],
    date_of_data: str,
    run_id: str,
    set_mode_on_airflow: bool = True
):  
    _, ch_s, _, vid_s, vid_l, _, com_s, com_l = get_current_silver(mode)

    conn = duckdb.connect(":memory:")

    try:
        # =====================================
        # CREATE DUCKDB DBLINK TO POSTGRESQL IF NOT EXISTS 
        # =====================================
        conn.execute("INSTALL postgres; LOAD postgres;")

        host = (
            env("PG_HOST_FOR_WINDOWS")
            if platform.system() == "Windows"
            else env("PG_HOST_FOR_WSL")
        )

        conn.execute(f"""
            ATTACH 'host={host} user={env("PG_USER")} password={env("PG_PSWD")} port={env("PG_PORT")} dbname={env("PG_DB")}' 
            AS pg (TYPE POSTGRES);
        """)

        conn.execute("BEGIN TRANSACTION")

        # ========================================================================
        # CACHE SILVER 
        # READ DENORMALIZED SILVER VIEW Silvera TO TEMP TABLE ONLY ONE TIME.
        # ========================================================================
        conn.execute(f"""
            CREATE TEMP TABLE stg_silver_current AS 
            SELECT 
                ch.hk_channel, vid.hk_video, com.hk_comment, 
                ch.channel_name, ch.custom_url, ch.description, ch.channel_publish_date, 
                vid.video_publish_date, vid.video_title, vid.video_description, 
                vid.video_category_id, vid.video_duration, vid.video_caption, 
                vid.video_dflt_language, vid.video_dflt_audio_language,
                SUBSTRING(com.comment_text, 1, 2000) AS comment_text, 
                SUBSTRING(com.comment_original, 1, 2000) AS comment_original, com.comment_author, 
                SUBSTRING(com.comment_author_url, 1, 300) AS comment_author_url, 
                com.comment_publish_date, com.comment_update_date, com.is_public, com.can_reply,
                ch.channel_view_count, ch.channel_subscrb_count, ch.channel_hidden_subscrb_count, 
                ch.video_count, vid.video_view_count, vid.video_like_count, vid.video_favorite_count, 
                vid.video_comment_count, com.comment_like_count, com.comment_reply_count,
                md5(concat_ws('|', 
                    COALESCE(TRIM(UPPER(ch.channel_name)), ''), 
                    COALESCE(TRIM(UPPER(vid.video_title)), ''), 
                    COALESCE(TRIM(UPPER(com.comment_text)), '')
                )) as hash_diff
            FROM (
                SELECT s.hk_channel, s.title AS channel_name, s.custom_url, s.description, 
                       s.published_at AS channel_publish_date, s.view_count AS channel_view_count, 
                       s.subscriber_count AS channel_subscrb_count, s.hidden_subscriber_count AS channel_hidden_subscrb_count, 
                       s.video_count
                FROM read_parquet('{ch_s}', union_by_name=True) s
                WHERE CAST(s.valid_from AS DATE) <= CAST('{date_of_data}' AS DATE)
                    AND (s.valid_to IS NULL OR CAST(s.valid_to AS DATE) >= CAST('{date_of_data}' AS DATE))
                QUALIFY ROW_NUMBER() OVER (PARTITION BY s.hk_channel ORDER BY s.load_date DESC) = 1
            ) ch
            LEFT JOIN (
                SELECT s.hk_video, l.hk_channel, s.published_at AS video_publish_date, s.title AS video_title,
                       s.description AS video_description, s.category_id AS video_category_id, 
                       (s.duration / 1000000000 || ' seconds')::INTERVAL AS video_duration, 
                       s.caption AS video_caption, s.default_language AS video_dflt_language, 
                       s.default_audio_language AS video_dflt_audio_language,
                       s.view_count AS video_view_count, s.like_count AS video_like_count, 
                       s.favorite_count AS video_favorite_count, s.comment_count AS video_comment_count
                FROM read_parquet('{vid_s}', union_by_name=True) s
                INNER JOIN read_parquet('{vid_l}', union_by_name=True) l ON s.hk_video = l.hk_video
                WHERE CAST(s.valid_from AS DATE) <= CAST('{date_of_data}' AS DATE)
                    AND (s.valid_to IS NULL OR CAST(s.valid_to AS DATE) >= CAST('{date_of_data}' AS DATE))
                QUALIFY ROW_NUMBER() OVER (PARTITION BY s.hk_video ORDER BY s.load_date DESC) = 1
            ) vid ON ch.hk_channel = vid.hk_channel
            LEFT JOIN (
                SELECT s.hk_comment, l.hk_video, s.comment_text, s.comment_original_text AS comment_original, 
                       s.comment_author, s.comment_author_url, s.comment_publish_date, 
                       s.comment_update_date, s.comment_is_public as is_public, s.comment_can_reply as can_reply,
                       s.comment_like_count, s.comment_reply_count
                FROM read_parquet('{com_s}', union_by_name=True) s
                INNER JOIN read_parquet('{com_l}', union_by_name=True) l ON s.hk_comment = l.hk_comment
                WHERE CAST(s.valid_from AS DATE) <= CAST('{date_of_data}' AS DATE)
                    AND (s.valid_to IS NULL OR CAST(s.valid_to AS DATE) >= CAST('{date_of_data}' AS DATE))
                QUALIFY ROW_NUMBER() OVER (PARTITION BY s.hk_comment ORDER BY s.load_date DESC) = 1
            ) com ON vid.hk_video = com.hk_video
        """)

        conn.execute("DROP TABLE IF EXISTS pg.public.pg_stg_silver_current")
        conn.execute("CREATE TABLE pg.public.pg_stg_silver_current AS SELECT * FROM stg_silver_current")

        # ========================================================================
        # YOUTUBE_STATISTICS_OBT (SCD2)
        # ========================================================================

        # ========================================================================
        # YOUTUBE_STATISTICS_OBT - CHECK EXISTING DATA AND MAX DATE OF DATA ON TABLE
        # ========================================================================
        if not set_mode_on_airflow: 
            max_valid_from = conn.execute("""
                    SELECT MAX(valid_from) FROM pg.public.youtube_statistics_obt
                """).fetchone()[0]
            
            if max_valid_from is None:
                mode = "full"
            elif datetime.strptime(date_of_data, "%Y-%m-%d").date() > max_valid_from:
                mode = "incremental"
            else:
                mode = "reload_date"
        
        if mode in ["full", "reload"]:
            conn.execute(f"SELECT * FROM postgres_query('pg', 'SELECT public.manage_partition(''youtube_statistics_obt'', ''{date_of_data}'', ''{mode}'')');")
            conn.execute(f"""
                INSERT INTO pg.public.youtube_statistics_obt (
                    valid_from, valid_to, run_id,
                    hk_channel, hk_video, hk_comment, 
                    channel_name, custom_url, description, channel_publish_date, 
                    video_publish_date, video_title, video_description, video_category_id, 
                    video_duration, video_caption, video_dflt_language, video_dflt_audio_language, 
                    comment_text, comment_original, comment_author, comment_author_url, 
                    comment_publish_date, comment_update_date, is_public, can_reply, is_current, hash_diff
                )
                SELECT CAST('{date_of_data}' AS DATE) AS valid_from, NULL AS valid_to, '{run_id}' AS run_id, 
                    hk_channel, hk_video, hk_comment, 
                    channel_name, custom_url, description, channel_publish_date, 
                    video_publish_date, video_title, video_description, video_category_id, 
                    video_duration, video_caption, video_dflt_language, video_dflt_audio_language, 
                    comment_text, comment_original, comment_author, comment_author_url, 
                    comment_publish_date, comment_update_date, is_public, can_reply, TRUE AS is_current, hash_diff
                FROM stg_silver_current
            """)

        elif mode == "incremental":
            conn.execute(f"""
                MERGE INTO pg.public.youtube_statistics_obt t
                USING (
                    SELECT s.*, s.hk_channel AS join_key, 'INSERT' as action_type
                    FROM stg_silver_current s
                    UNION ALL
                    SELECT s.*, s.hk_channel AS join_key, 'UPDATE' as action_type
                    FROM stg_silver_current s
                    INNER JOIN pg.public.youtube_statistics_obt o 
                        ON s.hk_channel IS NOT DISTINCT FROM o.hk_channel
                        AND s.hk_video IS NOT DISTINCT FROM o.hk_video 
                        AND s.hk_comment IS NOT DISTINCT FROM o.hk_comment
                    WHERE o.is_current = TRUE AND s.hash_diff <> o.hash_diff
                ) s ON t.hk_channel IS NOT DISTINCT FROM s.hk_channel 
                    AND t.hk_video IS NOT DISTINCT FROM s.hk_video 
                    AND t.hk_comment IS NOT DISTINCT FROM s.hk_comment 
                    AND t.is_current = TRUE
                WHEN MATCHED AND s.action_type = 'UPDATE' THEN
                    UPDATE SET valid_to = CAST('{date_of_data}' AS DATE) - 1, is_current = FALSE
                WHEN NOT MATCHED AND s.action_type = 'INSERT' THEN
                    INSERT (valid_from, valid_to, run_id,
                        hk_channel, hk_video, hk_comment,
                        channel_name, custom_url, description,
                        channel_publish_date, video_publish_date, video_title, video_description,
                        video_category_id, video_duration, video_caption, video_dflt_language,
                        video_dflt_audio_language, comment_text, comment_original, comment_author,
                        comment_author_url, comment_publish_date, comment_update_date, is_public,
                        can_reply, is_current, hash_diff)
                    VALUES (CAST('{date_of_data}' AS DATE), NULL, '{run_id}', 
                        s.hk_channel, s.hk_video, s.hk_comment,
                        s.channel_name, s.custom_url, s.description,
                        s.channel_publish_date, s.video_publish_date, s.video_title, s.video_description,
                        s.video_category_id, s.video_duration, s.video_caption, s.video_dflt_language,
                        s.video_dflt_audio_language, s.comment_text, s.comment_original, s.comment_author,
                        s.comment_author_url, s.comment_publish_date, s.comment_update_date, s.is_public,
                        s.can_reply, TRUE, s.hash_diff)
            """)

        elif mode == "reload_date":
            conn.execute(f"""
                CREATE TEMP TABLE ref_record AS
                SELECT DISTINCT ON (hk_channel, hk_video, hk_comment)
                    hk_channel, hk_video, hk_comment, 
                    valid_from AS ref_valid_from, 
                    hash_diff AS ref_hash
                FROM pg.public.youtube_statistics_obt
                WHERE valid_from < CAST('{date_of_data}' AS DATE)
                AND (valid_to IS NULL OR valid_to >= CAST('{date_of_data}' AS DATE))
                ORDER BY hk_channel, hk_video, hk_comment, valid_from DESC
            """)


            conn.execute(f"""
                UPDATE pg.public.youtube_statistics_obt t
                SET valid_to = CAST('{date_of_data}' AS DATE) - 1, is_current = FALSE
                FROM ref_record r
                JOIN stg_silver_current s
                    ON s.hk_channel IS NOT DISTINCT FROM r.hk_channel
                AND s.hk_video   IS NOT DISTINCT FROM r.hk_video
                AND s.hk_comment IS NOT DISTINCT FROM r.hk_comment
                WHERE t.hk_channel IS NOT DISTINCT FROM r.hk_channel
                AND t.hk_video   IS NOT DISTINCT FROM r.hk_video
                AND t.hk_comment IS NOT DISTINCT FROM r.hk_comment
                AND t.valid_from = r.ref_valid_from
                AND s.hash_diff <> r.ref_hash
            """)

            conn.execute(f"""
                DELETE FROM pg.public.youtube_statistics_obt 
                WHERE valid_from = CAST('{date_of_data}' AS DATE)
            """)

            conn.execute(f"""
                INSERT INTO pg.public.youtube_statistics_obt (valid_from, valid_to, run_id,
                    hk_channel, hk_video, hk_comment,
                    channel_name, custom_url, description,
                    channel_publish_date, video_publish_date, video_title, video_description,
                    video_category_id, video_duration, video_caption, video_dflt_language,
                    video_dflt_audio_language, comment_text, comment_original, comment_author,
                    comment_author_url, comment_publish_date, comment_update_date, is_public,
                    can_reply, is_current, hash_diff
                )
                SELECT CAST('{date_of_data}' AS DATE) AS valid_from,
                    g.next_val - 1 AS valid_to,
                    '{run_id}' AS run_id,
                    s.hk_channel, s.hk_video, s.hk_comment,
                    s.channel_name, s.custom_url, s.description,
                    s.channel_publish_date, s.video_publish_date, s.video_title, s.video_description,
                    s.video_category_id, s.video_duration, s.video_caption, s.video_dflt_language,
                    s.video_dflt_audio_language, s.comment_text, s.comment_original, s.comment_author,
                    s.comment_author_url, s.comment_publish_date, s.comment_update_date, s.is_public,
                    s.can_reply, CASE WHEN g.next_val IS NULL THEN TRUE ELSE FALSE END AS is_current, s.hash_diff
                FROM stg_silver_current s
                LEFT JOIN ref_record r
                    ON s.hk_channel IS NOT DISTINCT FROM r.hk_channel
                    AND s.hk_video   IS NOT DISTINCT FROM r.hk_video
                    AND s.hk_comment IS NOT DISTINCT FROM r.hk_comment
                LEFT JOIN (
                    SELECT hk_channel, hk_video, hk_comment, MIN(valid_from) as next_val
                    FROM pg.public.youtube_statistics_obt
                    WHERE valid_from > CAST('{date_of_data}' AS DATE)
                    GROUP BY 1, 2, 3
                ) g ON s.hk_channel = g.hk_channel AND s.hk_video = g.hk_video AND s.hk_comment = g.hk_comment
                WHERE r.ref_hash IS NULL OR s.hash_diff <> r.ref_hash
            """)

            conn.execute("DROP TABLE IF EXISTS ref_record")


        # ========================================================================
        # youtube_num_statistics_obt (Snapshot / Fact)
        # ========================================================================
        conn.execute(f"SELECT * FROM postgres_query('pg', 'SELECT public.manage_partition(''youtube_num_statistics_obt'', ''{date_of_data}'', ''{mode}'')');")
        
        conn.execute(f"""
            INSERT INTO pg.public.youtube_num_statistics_obt (
                date_of_data, run_id, hk_channel, hk_video, hk_comment, 
                channel_view_count, channel_subscrb_count, channel_hidden_subscrb_count, 
                video_count, video_view_count, video_like_count, video_favorite_count, 
                video_comment_count, comment_like_count, comment_reply_count
            )
            SELECT 
                CAST('{date_of_data}' AS DATE), '{run_id}', hk_channel, hk_video, hk_comment,
                channel_view_count, channel_subscrb_count, channel_hidden_subscrb_count, 
                video_count, video_view_count, video_like_count, video_favorite_count, 
                video_comment_count, comment_like_count, comment_reply_count
            FROM stg_silver_current
        """)

        conn.execute("COMMIT")
        print(f"[{run_id}] Gold load SUCCESS — mode={mode}, date={date_of_data}")

    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass

        print(f"[{run_id}] Gold load FAILED: {e}")
        raise e

    finally:
        conn.execute("DROP TABLE IF EXISTS stg_silver_current")
        conn.close()


def load(run_id: str, date_of_data: str, mode: Literal["full", "reload", "reload_date", "incremental"] = "incremental",
        set_mode_on_airflow: bool = True     
    ):
    
    try:
        start_date = datetime.now()
        print(start_date)
        load_statistics(mode, date_of_data, run_id, set_mode_on_airflow)
        end_date = datetime.now()
        print(end_date)
    except Exception as e:
        traceback.print_exc()
        end_date = datetime.now()
        print(end_date)
        raise e
