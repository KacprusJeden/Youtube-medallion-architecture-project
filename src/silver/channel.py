import hashlib
from typing import Literal, Tuple
import pandas as pd
from pathlib import Path 
import shutil
from datetime import datetime, timedelta
import traceback
import duckdb
from dags import watermark as wtm

def generate_hash_key(business_key: str) -> str:
    if not business_key:
        return None
    return hashlib.md5(str(business_key).strip().lower().encode()).hexdigest()


def get_new_bronze(mode: Literal["full", "incremental", "reload", "reload_date"],
                   bronze_wm_path: Path, silver_wm_path: Path = None, date_of_data: str = None) -> pd.DataFrame:    
    if not bronze_wm_path.exists():
        print(f"Bronze watermark not found at {bronze_wm_path}")
        return None
    
    pending_runs = wtm.get_pending_runs(
        dataset_type="channels",
        mode=mode, 
        duckdb_conn=duckdb.connect(":memory:"),
        bronze_watermark_path=bronze_wm_path,
        silver_watermark_path=silver_wm_path,
        date_of_data=date_of_data
    )
    
    if not pending_runs:
        print(f"No pending runs for channels")
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


def transform_bronze(bronze_channel_data: pd.DataFrame = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if bronze_channel_data is None or bronze_channel_data.empty:
        return pd.DataFrame(), pd.DataFrame()

    try:
        load_date = datetime.now()
        record_source = "youtube_api_channels"

        bronze_channel_data = bronze_channel_data.sort_values(
            by='etl_metadata', key=lambda col: col.apply(lambda x: x.get('load_date') if isinstance(x, dict) else None)
        ).drop_duplicates(subset=["id"], keep="last")

        # =========================
        # HUB
        # =========================
        hub = bronze_channel_data[["id"]].drop_duplicates(subset=["id"]).copy()
        hub.rename(columns={"id": "channel_id"}, inplace=True)
        
        hub["hk_channel"] = hub["channel_id"].apply(generate_hash_key)
        hub["load_date"] = load_date
        hub["record_source"] = record_source

        # =========================
        # SATELLITE
        # =========================
        satelite = bronze_channel_data.copy()
        satelite.rename(columns={"id": "channel_id"}, inplace=True)
        satelite["hk_channel"] = satelite["channel_id"].apply(generate_hash_key)

        columns_to_normalize = ["snippet", "contentDetails", "statistics", "etl_metadata"]
        for column in columns_to_normalize:
            if column in satelite.columns:
                normalized = pd.json_normalize(satelite[column])
                satelite = pd.concat([satelite.drop(columns=[column]).reset_index(drop=True), 
                                    normalized.reset_index(drop=True)], axis=1)

        thumb_cols = [f"thumbnails.{c}.url" for c in ["medium", "high"] if f"thumbnails.{c}.url" in satelite.columns]
        satelite = satelite.drop(columns=thumb_cols, errors='ignore')
        satelite = satelite.drop(columns=["localized.description", "localized.title"], errors="ignore")

        satelite = satelite.rename(columns={
            "viewCount": "view_count", 
            "subscriberCount": "subscriber_count",
            "hiddenSubscriberCount": "hidden_subscriber_count",
            "videoCount": "video_count",
            "customUrl": "custom_url", 
            "publishedAt": "published_at",
            "relatedPlaylists.likes": "likes", 
            "relatedPlaylists.uploads": "uploads",
            "thumbnails.default.url": "default_url"
        })

        satelite["relevant_data"] = (
            satelite.get("title").fillna("N/A").astype(str) + "|" +
            satelite.get("view_count").fillna(0).astype(str) + "|" +
            satelite.get("subscriber_count").fillna(0).astype(str) + "|" +
            satelite.get("video_count").fillna(0).astype(str) + "|" +
            satelite.get("hidden_subscriber_count").fillna(False).astype(str)
        )

        satelite = satelite.assign(
            hash_diff = satelite["relevant_data"].apply(generate_hash_key),
            view_count = pd.to_numeric(satelite.get("view_count", 0)).fillna(0).astype(int),
            subscriber_count = pd.to_numeric(satelite.get("subscriber_count", 0)).fillna(0).astype(int),
            default_dims = satelite.get("thumbnails.default.width", "0").astype(str) + "x" + satelite.get("thumbnails.default.height", "0").astype(str),
            medium_dims = satelite.get("thumbnails.medium.width", "0").astype(str) + "x" + satelite.get("thumbnails.medium.height", "0").astype(str),
            high_dims = satelite.get("thumbnails.high.width", "0").astype(str) + "x" + satelite.get("thumbnails.high.height", "0").astype(str),
            load_date = load_date
        )

        thumb_cols = [f"thumbnails.{c}.{dim}" for c in ["default", "medium", "high"] for dim in ["width", "height"] if f"thumbnails.{c}.url" in satelite.columns]
        satelite = satelite.drop(columns=thumb_cols, errors='ignore')
        satelite = satelite.drop(columns=["relevant_data"], errors='ignore')

        satelite = satelite.assign(
            valid_from = None,
            valid_to = None,
            is_current = None,
        )

        return hub, satelite
    
    except Exception as e:
        raise e 


def get_new_silver(date_of_data: str, mode: str,
                   current_hub: pd.DataFrame = None, new_hub: pd.DataFrame = None,
                   current_satelite: pd.DataFrame = None, new_satelite: pd.DataFrame = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    hub_delta = pd.DataFrame()
    sat_delta = pd.DataFrame()

    current_satelite = (
        current_satelite.copy()
        if current_satelite is not None
        else pd.DataFrame()
    )

    current_hub = (
        current_hub.copy()
        if current_hub is not None
        else pd.DataFrame()
    )

    try:

        # ==========================================================
        # RELOAD_DATE
        # ==========================================================
        if mode == "reload_date" and new_satelite is not None and not new_satelite.empty:

            reload_date = pd.to_datetime(date_of_data).normalize()
            prev_day = (
                reload_date - timedelta(days=1)
            ).strftime("%Y-%m-%d")


            # ------------------------------------------------------
            # REF RECORD
            # rekord obowiązujący w dniu reloadu
            # odpowiednik gold ref_record
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
                        subset=["hk_channel"],
                        keep="first"
                    )
                )


                # --------------------------------------------------
                # CLOSE CHANGED RECORDS
                # --------------------------------------------------

                compare = ref_record.merge(
                    new_satelite[
                        [
                            "hk_channel",
                            "hash_diff"
                        ]
                    ],
                    on="hk_channel",
                    how="inner",
                    suffixes=(
                        "_old",
                        "_new"
                    )
                )


                changed_keys = compare.loc[
                    compare["hash_diff_old"]
                    !=
                    compare["hash_diff_new"],
                    "hk_channel"
                ].unique()


                if len(changed_keys) > 0:

                    close_indexes = ref_record[
                        ref_record["hk_channel"].isin(changed_keys)
                    ].index


                    current_satelite.loc[
                        close_indexes,
                        "valid_to"
                    ] = prev_day


                    current_satelite.loc[
                        close_indexes,
                        "is_current"
                    ] = False



            # ------------------------------------------------------
            # DELETE EXISTING RELOAD DATE
            # ------------------------------------------------------

            current_satelite = current_satelite[
                pd.to_datetime(current_satelite["valid_from"])
                != reload_date
            ].copy()



            # ------------------------------------------------------
            # NEXT VALID FROM
            # odpowiednik gold g.next_val
            # ------------------------------------------------------

            future_records = current_satelite[
                pd.to_datetime(current_satelite["valid_from"])
                > reload_date
            ]


            if not future_records.empty:

                next_valid = (
                    future_records
                    .groupby(
                        "hk_channel",
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
                        "hk_channel",
                        "next_valid_from"
                    ]
                )


            # ------------------------------------------------------
            # INSERT RELOAD DATE
            #
            # UWAGA:
            # tutaj NIE filtrujemy hash_diff
            # bo dzień został usunięty
            # i musi zostać odtworzony
            # ------------------------------------------------------

            sat_delta = new_satelite.copy()


            sat_delta["valid_from"] = date_of_data


            sat_delta = sat_delta.merge(
                next_valid,
                on="hk_channel",
                how="left"
            )


            sat_delta["valid_to"] = None


            mask = sat_delta["next_valid_from"].notna()


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
                sat_delta["next_valid_from"].isna()
            )


            sat_delta.drop(
                columns=[
                    "next_valid_from"
                ],
                inplace=True,
                errors="ignore"
            )


            return (
                hub_delta,
                sat_delta,
                current_hub,
                current_satelite
            )


        # ==========================================================
        # HUB LOGIC
        # ==========================================================

        if new_hub is not None and not new_hub.empty:

            if not current_hub.empty:

                current_keys = set(
                    current_hub["hk_channel"]
                    .astype(str)
                    .unique()
                )

                hub_delta = new_hub.loc[
                    ~new_hub["hk_channel"].isin(current_keys)
                ].copy()

            else:

                hub_delta = new_hub.copy()



        # ==========================================================
        # INCREMENTAL / FULL / RELOAD SATELLITE SCD2
        # ==========================================================

        if new_satelite is None or new_satelite.empty:

            return (
                hub_delta,
                sat_delta,
                current_hub,
                current_satelite
            )



        if not current_satelite.empty:

            valid_satelite = current_satelite[
                current_satelite["valid_to"].isna()
            ].copy()


            valid_satelite = (
                valid_satelite
                .sort_values("valid_from")
                .drop_duplicates(
                    "hk_channel",
                    keep="last"
                )
            )

        else:

            valid_satelite = pd.DataFrame()



        if valid_satelite.empty:

            sat = new_satelite.copy()

            sat["valid_from"] = date_of_data
            sat["valid_to"] = None
            sat["is_current"] = True

            return (
                hub_delta,
                sat,
                current_hub,
                current_satelite
            )



        merged = new_satelite.merge(
            valid_satelite[
                [
                    "hk_channel",
                    "hash_diff"
                ]
            ],
            on="hk_channel",
            how="left",
            suffixes=(
                "",
                "_curr"
            )
        )


        change_mask = (
            merged["hash_diff_curr"].isna()
            |
            (
                merged["hash_diff"]
                !=
                merged["hash_diff_curr"]
            )
        )


        to_open = merged.loc[
            change_mask
        ].copy()


        to_open["valid_from"] = date_of_data
        to_open["valid_to"] = None
        to_open["is_current"] = True


        to_open.drop(
            columns=[
                "hash_diff_curr"
            ],
            inplace=True,
            errors="ignore"
        )



        to_close_mask = (
            merged["hash_diff_curr"].notna()
            &
            (
                merged["hash_diff"]
                !=
                merged["hash_diff_curr"]
            )
        )


        to_close_keys = (
            merged.loc[
                to_close_mask,
                "hk_channel"
            ]
            .unique()
        )


        if len(to_close_keys) > 0:

            prev_day = (
                datetime.strptime(
                    date_of_data,
                    "%Y-%m-%d"
                )
                -
                timedelta(days=1)
            ).strftime("%Y-%m-%d")


            close_mask = (
                current_satelite["hk_channel"].isin(
                    to_close_keys
                )
                &
                current_satelite["valid_to"].isna()
            )


            current_satelite.loc[
                close_mask,
                "valid_to"
            ] = prev_day


            current_satelite.loc[
                close_mask,
                "is_current"
            ] = False



        sat_delta = to_open


        return (
            hub_delta,
            sat_delta,
            current_hub,
            current_satelite
        )


    except Exception as e:

        print(
            f"Error in get_new_silver: {e}"
        )

        traceback.print_exc()

        return (
            pd.DataFrame(),
            pd.DataFrame(),
            current_hub,
            current_satelite
        )


def save_silver(run_id: str, hub: pd.DataFrame, satelite: pd.DataFrame, hub_path: Path = None, sat_path: Path = None):
    if hub_path and not hub.empty:
        if hub_path.is_file():
            raise NotADirectoryError(f"Path '{hub_path}' is file, not a directory")
        hub_path.mkdir(parents=True, exist_ok=True)
        hub.to_parquet(hub_path / f"hub_{run_id}.parquet", index=False)

    if sat_path and not satelite.empty:
        if sat_path.is_file():
            raise NotADirectoryError(f"Path '{sat_path}' is file, not a directory")
        sat_path.mkdir(parents=True, exist_ok=True)
        satelite.to_parquet(sat_path / f"sat_{run_id}.parquet", index=False)


def load_silver(run_id: str, mode: Literal["full", "incremental", "reload", "reload_date"], 
                channel: str, bronze_wtm_path: Path, silver_wtm_path: Path | None = None,
                date_of_data: str = None) -> None:
    
    hub = pd.DataFrame()
    satelite = pd.DataFrame()

    try:
        hub_path, sat_path, _, _, _ = wtm.get_data_dir_pattern(
            layer="silver", dataset_type="channels", mode=mode, status="success", channel=channel
        )

        bronze = get_new_bronze(mode, bronze_wtm_path, silver_wtm_path, date_of_data)
        hub_new, sat_new = transform_bronze(bronze)
        
        if mode in ["incremental", "reload_date"]:
            if bronze is None or bronze.empty:
                print(f"[{run_id}] No new bronze data — skipping")
                return

            current_hub, current_sat, _ = get_current_silver(hub_path, sat_path)

            hub_delta, sat_delta, current_hub, current_sat = get_new_silver(
                date_of_data, mode, current_hub, hub_new, current_sat, sat_new
            )

            # ==============================================================
            # RELOAD_DATE
            # ==============================================================
            if mode == "reload_date":
                hub = pd.concat([current_hub, hub_delta], ignore_index=True).drop_duplicates(subset=["hk_channel"], keep="last")
                satelite = pd.concat([current_sat, sat_delta], ignore_index=True)
                
                truncate_data(hub_path)
                truncate_data(sat_path)
            else:
                # ==============================================================
                # INCREMENTAL
                # ==============================================================
                hub = hub_delta
                satelite = sat_delta

        else:  
            # ==============================================================
            # FULL / RELOAD
            # ==============================================================
            truncate_data(hub_path)
            truncate_data(sat_path)

            hub, satelite = hub_new, sat_new
            satelite["valid_from"] = date_of_data
            satelite["valid_to"] = None
            satelite["is_current"] = True

        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.max_colwidth', None):
            print("new_hub\n", hub, "\n")
            print("new_sat\n", satelite, "\n")

        save_silver(run_id, hub, satelite, hub_path, sat_path)

    except Exception as e:
        traceback.print_exc()
        wtm.save_watermark(
            layer="silver", channel=channel, dataset_type="channels", watermark_column="run_id",
            watermark_value=run_id, run_id=run_id, date_of_data=date_of_data, mode=mode,
            status="failed", exception=str(e)
        )
        print(f"[{run_id}] Silver load FAILED")
        raise e