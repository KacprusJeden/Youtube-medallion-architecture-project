import hashlib
import pandas as pd
import pandera.pandas as pdr
from typing import Literal, Tuple
from pathlib import Path
import shutil
import isodate
from datetime import datetime, timedelta
import traceback
import re
from dags import watermark as wtm
import duckdb

# =========================
# GET NEW BRONZE AND CURRENT SILVER
# =========================

def get_new_bronze(mode: Literal["full", "incremental", "reload", "reload_date"],
                   bronze_wm_path: Path, silver_wm_path: Path = None, date_of_data: str = None) -> pd.DataFrame:    
    
    if not bronze_wm_path.exists():
        print(f"Bronze watermark not found at {bronze_wm_path}")
        return None
    
    pending_runs = wtm.get_pending_runs(
        dataset_type="videos",
        mode=mode, 
        duckdb_conn=duckdb.connect(":memory:"),
        bronze_watermark_path=bronze_wm_path,
        silver_watermark_path=silver_wm_path,
        date_of_data=date_of_data
    )

    print(pending_runs)
    
    if not pending_runs:
        print(f"No pending runs for videos")
        return None
    
    all_dfs = []
    
    try:
        for item in pending_runs:
            data_dir = Path(item.get("data_dir"))
            
            if data_dir.exists():
                files = list(data_dir.glob("*.jsonl"))
                
                for f in files:
                    df = pd.read_json(f, lines=True)
                    df['run_id'] = item.get('run_id')
                    df['date_of_data'] = item.get('date_of_data')
                    all_dfs.append(df)

        if all_dfs:    
            return pd.concat(all_dfs, ignore_index=True)
    
    except Exception as e:
        print(f"Error while loading bronze data: {e}")
        print(traceback.format_exc())

    return None


def get_current_silver(hub_path: Path, sat_path: Path, link_path: Path = None) \
        -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    
    df_hub = pd.DataFrame()
    df_sat = pd.DataFrame()
    df_link = pd.DataFrame()

    if hub_path.exists():
        hub_files = list(hub_path.rglob("*.parquet"))
        if hub_files:
            df_hub = pd.concat((pd.read_parquet(f) for f in hub_files), ignore_index=True)

    if sat_path.exists():
        sat_files = list(sat_path.rglob("*.parquet"))
        if sat_files:
            df_sat = pd.concat((pd.read_parquet(f) for f in sat_files), ignore_index=True)
            if not df_sat.empty:
                df_sat = df_sat.sort_values("load_date")

    if link_path and link_path.exists():
        link_files = list(link_path.rglob("*.parquet"))
        if link_files:
            df_link = pd.concat((pd.read_parquet(f) for f in link_files), ignore_index=True)
            if not df_link.empty:
                df_link = df_link.sort_values("load_date")

    return df_hub, df_sat, df_link


def truncate_data(silver_path: Path):
    if silver_path.exists():
        shutil.rmtree(silver_path)
    silver_path.mkdir(parents=True, exist_ok=True)

# =========================
# HELPERS
# =========================

def generate_hash_key(business_key: str) -> str:
    if business_key is None:
        return None
    return hashlib.md5(str(business_key).strip().lower().encode()).hexdigest()


def safe_col(df: pd.DataFrame, col: str, default=None) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series([default] * len(df))


def parse_iso8601_duration(duration_str) -> pd.Timedelta:
    try:
        if pd.isna(duration_str):
            return pd.Timedelta(0)
        return pd.to_timedelta(isodate.parse_duration(duration_str))
    except Exception:
        return pd.Timedelta(0)


def safe_to_numeric(series, default=0):
    return pd.to_numeric(series, errors="coerce").fillna(default)


def safe_bool(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.lower()
        .map({"true": True, "false": False, "1": True, "0": False})
        .fillna(False)
    )


def clean_nulls(series: pd.Series) -> pd.Series:
    return series.replace(["", "None", "null", None, "none"], pd.NA)


# ===========================
# MAIN TRANSFORMATION
# ===========================

def transform_bronze(
    bronze: pd.DataFrame = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    
    try:

        if bronze is None or bronze.empty:
            return (pd.DataFrame(),)*5

        if "id" not in bronze.columns:
            raise ValueError("Missing required column: id")

        load_date = datetime.now()

        # =========================
        # SORT + DEDUP
        # =========================
        data = bronze.sort_values(
            by="etl_metadata",
            key=lambda col: col.apply(
                lambda x: x.get("load_date") if isinstance(x, dict) else x
            ),
        ).drop_duplicates(subset=["id"], keep="last")

        # =========================
        # NORMALIZE JSON
        # =========================
        for column in ["snippet", "contentDetails", "statistics", "etl_metadata"]:
            if column in data.columns and data[column].notna().any():
                norm = pd.json_normalize(data[column])
                data = pd.concat(
                    [data.drop(columns=[column]).reset_index(drop=True),
                    norm.reset_index(drop=True)],
                    axis=1,
                )

        # =========================
        # CLEAN COLUMNS
        # =========================
        data.columns = [
            re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", c).lower()
            for c in data.columns
        ]

        data = data.rename(columns={"id": "video_id"})

        # drop noisy columns
        drop_cols = [
            c for c in data.columns
            if re.search(r"(thumbnails|localized|tags)", c)
        ]
        data = data.drop(columns=drop_cols, errors="ignore")

        # remove null business keys
        data = data[data["video_id"].notna()].copy()

        # =========================
        # TRANSFORM
        # =========================
        data = data.assign(
            hk_video=data["video_id"].apply(generate_hash_key),
            hk_channel=data["channel_id"].apply(generate_hash_key),

            duration=safe_col(data, "duration").apply(parse_iso8601_duration),

            title=clean_nulls(safe_col(data, "title", "")).astype(str),
            description=clean_nulls(safe_col(data, "description", "")).astype(str).str[:4000],
            channel_title=clean_nulls(safe_col(data, "channel_title", "")).astype(str),

            default_language=safe_col(data, "default_language", "").astype(str).str.upper().str[:4],
            default_audio_language=safe_col(data, "default_audio_language", "").astype(str).str.upper().str[:4],

            dimension=safe_col(data, "dimension", "2D").fillna("2D").astype(str).str.upper(),
            definition=safe_col(data, "definition", "HD").fillna("HD").astype(str).str.upper(),

            caption=safe_bool(safe_col(data, "caption")),
            licensed_content=safe_bool(safe_col(data, "licensed_content")),

            view_count=safe_to_numeric(safe_col(data, "view_count")),
            like_count=safe_to_numeric(safe_col(data, "like_count")),
            favorite_count=safe_to_numeric(safe_col(data, "favorite_count")),
            comment_count=safe_to_numeric(safe_col(data, "comment_count")),

            live_broadcast_content=safe_col(data, "live_broadcast_content", "none").astype(str).str.lower(),
            category_id=safe_to_numeric(safe_col(data, "category_id"), 0).astype(int),
            channel_id=safe_col(data, "channel_id").astype(str),
            published_at=pd.to_datetime(safe_col(data, "published_at"), errors="coerce"),

            run_id=safe_col(data, "run_id").astype(str),
            date_of_data=pd.to_datetime(safe_col(data, "date_of_data"), errors="coerce"),
            load_date=load_date,
            source="youtube_api_videos"
        )

        # =========================
        # HASH DIFF
        # =========================
        data["hash_diff"] = (
            data["title"].fillna("N/A") + "|" +
            data["view_count"].astype(str) + "|" +
            data["like_count"].astype(str) + "|" +
            data["comment_count"].astype(str) + "|" +
            data["favorite_count"].astype(str)
        ).apply(generate_hash_key)

        # =========================
        # SCD2 META
        # =========================
        data = data.assign(
            valid_from=None,
            valid_to=None,
            is_current=None,
        )

        # =========================
        # VALIDATION
        # =========================
        video_schema = pdr.DataFrameSchema({ 
            "video_id": pdr.Column(str, coerce=True), 
            "run_id": pdr.Column(str, coerce=True), 
            "date_of_data": pdr.Column(pdr.DateTime, coerce=True), 
            "hk_video": pdr.Column(str, coerce=True), 
            "hk_channel": pdr.Column(str, coerce=True), 
            "published_at": pdr.Column(pd.Timestamp, coerce=True, nullable=True), 
            "channel_id": pdr.Column(str, coerce=True), 
            "title": pdr.Column(str, coerce=True), 
            "description": pdr.Column(str, checks=pdr.Check.str_length(max_value=4000), coerce=True, nullable=True), 
            "channel_title": pdr.Column(str, coerce=True), 
            "category_id": pdr.Column(int, coerce=True), 
            "live_broadcast_content": pdr.Column(str, coerce=True, checks=pdr.Check.str_length(max_value=100)), 
            "default_language": pdr.Column(str, coerce=True, checks=pdr.Check.str_length(max_value=4), default="EN"), 
            "default_audio_language": pdr.Column(str, coerce=True, checks=pdr.Check.str_length(max_value=4), default="EN"), 
            "duration": pdr.Column(pd.Timedelta, coerce=True),
            "dimension": pdr.Column(str, coerce=True, default="2D", checks=pdr.Check.str_length(max_value=10)),
            "definition": pdr.Column(str, coerce=True, default="HD", checks=pdr.Check.str_length(max_value=10)), 
            "caption": pdr.Column(bool, coerce=True), 
            "licensed_content": pdr.Column(bool, coerce=True), 
            "view_count": pdr.Column(int, coerce=True, checks=pdr.Check.ge(min_value=0)), 
            "like_count": pdr.Column(int, coerce=True, checks=pdr.Check.ge(min_value=0)), 
            "favorite_count": pdr.Column(int, coerce=True, checks=pdr.Check.ge(min_value=0)), 
            "comment_count": pdr.Column(int, coerce=True, checks=pdr.Check.ge(min_value=0)),
            "load_date": pdr.Column(pd.Timestamp, coerce=True, nullable=False), 
            "source": pdr.Column(str, coerce=True), 
            "hash_diff": pdr.Column(str, coerce=True, nullable=True), 
            "valid_from": pdr.Column(str, coerce=True, nullable=True), 
            "valid_to": pdr.Column(str, coerce=True, nullable=True), 
            "is_current": pdr.Column(bool, coerce=True, nullable=True) 
        })

        valid, invalid, report = validate_with_pandera_full(data, video_schema)

        if valid.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), invalid, report

        # =========================
        # HUB
        # =========================
        hub = valid[["video_id", "hk_video", "load_date", "source"]].drop_duplicates(subset=["hk_video"])

        # =========================
        # LINK
        # =========================
        link = valid[["hk_video", "hk_channel", "load_date", "source"]].drop_duplicates(subset=["hk_video", "hk_channel"])
        link["hk_link_video_channel"] = (link["hk_video"] + link["hk_channel"]).apply(generate_hash_key)

        # =========================
        # CLEAN HASHES FROM SATELLITE
        # =========================
        valid = valid.drop(columns=["channel_id", "hk_channel"], errors="ignore")

        return hub, valid, link, invalid, report

    except Exception as e:
        raise e


# =========================
# VALIDATION HANDLER
# =========================
def validate_with_pandera_full(df: pd.DataFrame, schema: pdr.DataFrameSchema):
    try:
        valid_df = schema.validate(df, lazy=True)
        return valid_df, pd.DataFrame(), pd.DataFrame()

    except pdr.errors.SchemaErrors as err:
        failure_report = err.failure_cases
        invalid_idx = failure_report["index"].dropna().unique()

        invalid = df.loc[invalid_idx].copy() if len(invalid_idx) else pd.DataFrame()
        valid_raw = df.drop(index=invalid_idx).copy()

        try:
            valid = schema.validate(valid_raw, lazy=True) if not valid_raw.empty else pd.DataFrame()
        except Exception:
            valid = pd.DataFrame()

        return valid, invalid, failure_report
    

# =====================
# COMPARE NEW BRONZE WITH CURRENT SILVER + SCD2
# =====================

def get_new_silver(
    date_of_data: str,
    mode: str,
    current_hub: pd.DataFrame = None,
    new_hub: pd.DataFrame = None,
    current_satelite: pd.DataFrame = None,
    new_satelite: pd.DataFrame = None,
    current_link: pd.DataFrame = None,
    new_link: pd.DataFrame = None
) -> Tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame
]:

    hub_delta = pd.DataFrame()
    sat_delta = pd.DataFrame()
    link_delta = pd.DataFrame()

    current_hub = current_hub.copy() if current_hub is not None else pd.DataFrame()
    current_satelite = current_satelite.copy() if current_satelite is not None else pd.DataFrame()
    current_link = current_link.copy() if current_link is not None else pd.DataFrame()

    try:

        reload_date = pd.to_datetime(date_of_data).normalize()
        prev_day = (reload_date - timedelta(days=1)).strftime("%Y-%m-%d")


        # ==========================================================
        # HUB
        # ==========================================================

        if new_hub is not None and not new_hub.empty:

            if current_hub.empty:
                hub_delta = new_hub.copy()

            else:
                existing = set(
                    current_hub["hk_video"]
                    .astype(str)
                    .unique()
                )

                hub_delta = new_hub[
                    ~new_hub["hk_video"].astype(str).isin(existing)
                ].copy()



        # ==========================================================
        # LINK
        # ==========================================================

        if new_link is not None and not new_link.empty:

            if current_link.empty:
                link_delta = new_link.copy()

            else:
                existing = set(
                    current_link["hk_link_video_channel"]
                    .astype(str)
                    .unique()
                )

                link_delta = new_link[
                    ~new_link["hk_link_video_channel"]
                    .astype(str)
                    .isin(existing)
                ].copy()



        # ==========================================================
        # RELOAD DATE - SCD2 REPAIR
        # ==========================================================

        if mode == "reload_date" and new_satelite is not None and not new_satelite.empty:


            # ------------------------------------------------------
            # REF RECORD
            # odpowiednik:
            #
            # SELECT DISTINCT ON(hk_video)
            # ORDER BY valid_from DESC
            # ------------------------------------------------------

            ref_record = current_satelite[
                (pd.to_datetime(current_satelite["valid_from"]) < reload_date)
                &
                (
                    current_satelite["valid_to"].isna()
                    |
                    (
                        pd.to_datetime(current_satelite["valid_to"])
                        >= reload_date
                    )
                )
            ].copy()


            if not ref_record.empty:

                ref_record = (
                    ref_record
                    .sort_values(
                        "valid_from",
                        ascending=False
                    )
                    .drop_duplicates(
                        subset=["hk_video"],
                        keep="first"
                    )
                )


            # ------------------------------------------------------
            # CLOSE OLD RECORDS
            # ------------------------------------------------------

            if not ref_record.empty:


                compare = ref_record[
                    [
                        "hk_video",
                        "hash_diff"
                    ]
                ].merge(
                    new_satelite[
                        [
                            "hk_video",
                            "hash_diff"
                        ]
                    ],
                    on="hk_video",
                    how="inner",
                    suffixes=("_old", "_new")
                )


                changed = compare[
                    compare["hash_diff_old"]
                    !=
                    compare["hash_diff_new"]
                ]["hk_video"].unique()



                if len(changed) > 0:

                    close_mask = (
                        current_satelite["hk_video"].isin(changed)
                        &
                        (
                            current_satelite["valid_from"]
                            ==
                            ref_record.set_index(
                                "hk_video"
                            )
                            .loc[
                                current_satelite.loc[
                                    current_satelite["hk_video"]
                                    .isin(changed),
                                    "hk_video"
                                ],
                                "valid_from"
                            ]
                            .values
                        )
                    )


                    current_satelite.loc[
                        close_mask,
                        "valid_to"
                    ] = prev_day


                    current_satelite.loc[
                        close_mask,
                        "is_current"
                    ] = False



            # ------------------------------------------------------
            # DELETE RELOAD DATE
            # ------------------------------------------------------

            current_satelite = current_satelite[
                pd.to_datetime(
                    current_satelite["valid_from"]
                )
                != reload_date
            ].copy()



            # ======================================================
            # NEXT VALID FROM
            # ======================================================

            future = current_satelite[
                pd.to_datetime(
                    current_satelite["valid_from"]
                )
                > reload_date
            ]


            if not future.empty:

                next_valid = (
                    future
                    .groupby(
                        "hk_video",
                        as_index=False
                    )
                    ["valid_from"]
                    .min()
                    .rename(
                        columns={
                            "valid_from":
                            "next_valid_from"
                        }
                    )
                )

            else:

                next_valid = pd.DataFrame(
                    columns=[
                        "hk_video",
                        "next_valid_from"
                    ]
                )



            # ======================================================
            # OPEN NEW RECORDS
            # ======================================================

            if ref_record.empty:

                ref_hash = pd.DataFrame(
                    columns=[
                        "hk_video",
                        "hash_diff"
                    ]
                )

            else:

                ref_hash = ref_record[
                    [
                        "hk_video",
                        "hash_diff"
                    ]
                ]

            sat_delta = (
                new_satelite
                .merge(
                    ref_hash,
                    on="hk_video",
                    how="left",
                    suffixes=("", "_old")
                )
            )

            sat_delta = sat_delta[
                sat_delta["hash_diff_old"].isna()
                |
                (
                    sat_delta["hash_diff"]
                    !=
                    sat_delta["hash_diff_old"]
                )
            ].copy()

            sat_delta = sat_delta.merge(
                next_valid,
                on="hk_video",
                how="left"
            )

            sat_delta["valid_from"] = date_of_data

            sat_delta["valid_to"] = None

            mask = (
                sat_delta["next_valid_from"]
                .notna()
            )

            sat_delta.loc[
                mask,
                "valid_to"
            ] = (
                pd.to_datetime(
                    sat_delta.loc[
                        mask,
                        "next_valid_from"
                    ]
                )
                -
                timedelta(days=1)
            ).dt.strftime("%Y-%m-%d")


            sat_delta["is_current"] = (
                sat_delta["next_valid_from"]
                .isna()
            )

            sat_delta.drop(
                columns=[
                    "hash_diff_old",
                    "next_valid_from"
                ],
                inplace=True,
                errors="ignore"
            )

            return (
                hub_delta,
                sat_delta,
                link_delta,
                current_hub,
                current_satelite,
                current_link
            )

        # ==========================================================
        # INCREMENTAL SCD2
        # ==========================================================
        if new_satelite is None or new_satelite.empty:

            return (
                hub_delta,
                sat_delta,
                link_delta,
                current_hub,
                current_satelite,
                current_link
            )

        if current_satelite.empty:

            sat_delta = new_satelite.copy()

            sat_delta["valid_from"] = date_of_data
            sat_delta["valid_to"] = None
            sat_delta["is_current"] = True


            return (
                hub_delta,
                sat_delta,
                link_delta,
                current_hub,
                current_satelite,
                current_link
            )

        current_open = current_satelite[
            current_satelite["valid_to"].isna()
        ].copy()

        current_open = (
            current_open
            .sort_values("valid_from")
            .drop_duplicates(
                "hk_video",
                keep="last"
            )
        )

        merged = new_satelite.merge(
            current_open[
                [
                    "hk_video",
                    "hash_diff"
                ]
            ],
            on="hk_video",
            how="left",
            suffixes=("", "_old")
        )

        changed = (
            merged["hash_diff_old"].isna()
            |
            (
                merged["hash_diff"]
                !=
                merged["hash_diff_old"]
            )
        )

        to_open = merged[changed].copy()

        to_open["valid_from"] = date_of_data
        to_open["valid_to"] = None
        to_open["is_current"] = True

        to_open.drop(
            columns=[
                "hash_diff_old"
            ],
            inplace=True,
            errors="ignore"
        )

        close_keys = merged[
            merged["hash_diff_old"].notna()
            &
            (
                merged["hash_diff"]
                !=
                merged["hash_diff_old"]
            )
        ]["hk_video"].unique()

        if len(close_keys):

            current_satelite.loc[
                (
                    current_satelite["hk_video"]
                    .isin(close_keys)
                    &
                    current_satelite["valid_to"]
                    .isna()
                ),
                "valid_to"
            ] = prev_day

            current_satelite.loc[
                (
                    current_satelite["hk_video"]
                    .isin(close_keys)
                    &
                    current_satelite["valid_to"]
                    .eq(prev_day)
                ),
                "is_current"
            ] = False

        sat_delta = to_open

        return (
            hub_delta,
            sat_delta,
            link_delta,
            current_hub,
            current_satelite,
            current_link
        )


    except Exception as e:

        print(f"Error in get_new_silver: {e}")
        traceback.print_exc()

        return (
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            current_hub,
            current_satelite,
            current_link
        )

def save_silver(
    run_id: str, hub: pd.DataFrame, satelite: pd.DataFrame, link: pd.DataFrame,
    invalid: pd.DataFrame = None, report: pd.DataFrame = None,
    hub_path: Path = None, sat_path: Path = None, link_path: Path = None,
    invalid_path: Path = None, report_path: Path = None,
):
    if hub_path and not hub.empty:
        if hub_path.is_file(): raise NotADirectoryError(f"Path '{hub_path}' is file")
        hub_path.mkdir(parents=True, exist_ok=True)
        hub.to_parquet(hub_path / f"hub_{run_id}.parquet", index=False)

    if sat_path and not satelite.empty:
        if sat_path.is_file(): raise NotADirectoryError(f"Path '{sat_path}' is file")
        sat_path.mkdir(parents=True, exist_ok=True)
        satelite.to_parquet(sat_path / f"sat_{run_id}.parquet", index=False)
        
    if link_path and not link.empty:
        if link_path.is_file(): raise NotADirectoryError(f"Path '{link_path}' is file")
        link_path.mkdir(parents=True, exist_ok=True)
        link.to_parquet(link_path / f"link_{run_id}.parquet", index=False)
        
    if invalid_path and not invalid.empty:
        if invalid_path.is_file(): raise NotADirectoryError(f"Path '{invalid_path}' is file")
        invalid_path.mkdir(parents=True, exist_ok=True)
        invalid.to_parquet(invalid_path / f"invalid_{run_id}.parquet", index=False)

    if report_path and not report.empty:
        if report_path.is_file(): raise NotADirectoryError(f"Path '{report_path}' is file")
        report_path.mkdir(parents=True, exist_ok=True)
        report.to_parquet(report_path / f"report_{run_id}.parquet", index=False)


def load_silver(run_id: str, mode: Literal["full", "incremental", "reload", "reload_date"],
                channel: str, bronze_wtm_path: Path, silver_wtm_path: Path | None = None,
                date_of_data: str = None) -> None:
    
    hub = pd.DataFrame()
    satelite = pd.DataFrame()
    link = pd.DataFrame()
    invalid = pd.DataFrame()
    failure_report = pd.DataFrame()

    try:
        hub_path, sat_path, link_path, invalid_path, report_path = wtm.get_data_dir_pattern(
            layer="silver", dataset_type="videos", mode=mode, status="success", channel=channel
        )

        bronze = get_new_bronze(mode, bronze_wtm_path, silver_wtm_path, date_of_data)
        hub_new, sat_new, link_new, invalid, failure_report = transform_bronze(bronze)
        
        if mode in ["incremental", "reload_date"]:
            if bronze is None or bronze.empty:
                print(f"[{run_id}] No new bronze data — skipping")
                return

            current_hub, current_sat, current_link = get_current_silver(hub_path, sat_path, link_path)

            hub_delta, sat_delta, link_delta, current_hub, current_sat, current_link = get_new_silver(
                date_of_data, mode,
                current_hub, hub_new,
                current_sat, sat_new,
                current_link, link_new
            )

            # ==============================================================
            # RELOAD_DATE
            # ==============================================================
            if mode == "reload_date":
                satelite = pd.concat([current_sat, sat_delta], ignore_index=True)
                
                hub = pd.concat([current_hub, hub_delta], ignore_index=True)
                hub = hub.drop_duplicates(subset=["hk_video"], keep="last")
                
                link = pd.concat([current_link, link_delta], ignore_index=True)
                link = link.drop_duplicates(subset=["hk_link_video_channel"], keep="last")
                
                # Ważne porządki po reload_date
                satelite = satelite.sort_values(["hk_video", "valid_from"]).reset_index(drop=True)
                satelite = satelite.drop_duplicates(subset=["hk_video", "valid_from"], keep="last")
                
                truncate_data(sat_path)
                truncate_data(hub_path)
                truncate_data(link_path)
            else:
                # ==============================================================
                # INCREMENTAL
                # ==============================================================
                hub = hub_delta
                satelite = sat_delta
                link = link_delta

        else:  
            # ==============================================================
            # FULL / RELOAD
            # ==============================================================
            truncate_data(hub_path)
            truncate_data(sat_path)
            truncate_data(link_path)

            hub, satelite, link = hub_new, sat_new, link_new
            satelite["valid_from"] = date_of_data
            satelite["valid_to"] = None
            satelite["is_current"] = True

        save_silver(
            run_id=run_id, hub=hub, satelite=satelite, link=link, 
            invalid=invalid, report=failure_report, 
            hub_path=hub_path, sat_path=sat_path, link_path=link_path,
            invalid_path=invalid_path, report_path=report_path
        )
        print(f"[{run_id}] Silver load SUCCESS")

    except Exception as e:
        traceback.print_exc()
        wtm.save_watermark(
            layer="silver", channel=channel, dataset_type="videos", watermark_column="run_id",
            watermark_value=run_id, run_id=run_id, date_of_data=date_of_data, mode=mode,
            status="failed", exception=str(e)
        )
        print(f"[{run_id}] Silver load FAILED")
        raise e