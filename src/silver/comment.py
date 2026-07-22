import hashlib
from typing import Any, Literal, Tuple
from pathlib import Path
import shutil
from datetime import date, timedelta
import traceback
from dags import watermark as wtm
import duckdb
import polars as pl
from polars import datatypes as dtypes


class Transformer:

    def __init__(self):
        self.duckdb_conn = duckdb.connect(database=':memory:')

    # ===========================
    # GET NEW BRONZE
    # ===========================
    def get_new_bronze(self, mode: Literal["full", "incremental", "reload", "reload_date"],
        bronze_wm_path: Path, silver_wm_path: Path = None, date_of_data: str = None) -> pl.DataFrame:    

        if not bronze_wm_path.exists():
            print(f"Bronze watermark not found at {bronze_wm_path}")
            return None

        pending_runs = wtm.get_pending_runs(
            "comments", mode, self.duckdb_conn, bronze_wm_path, silver_wm_path, date_of_data
        )

        if not pending_runs:
            print("No pending runs for comments")
            return None

        all_files = []
        mapping_data = []

        for item in pending_runs:
            data_dir = Path(item.get("data_dir"))

            if data_dir.exists():
                files = list(data_dir.glob("*.jsonl"))

                for f in files:
                    all_files.append(str(f))
                    mapping_data.append({
                        "file_path": str(f),
                        "run_id": item.get("run_id"),
                        "date_of_data": item.get("date_of_data")
                    })

        if not all_files:
            return None

        query = """
            SELECT *, filename as file_path
            FROM read_json_auto(?, filename=True)
        """

        df_raw = self.duckdb_conn.execute(query, [all_files]).pl()
        df_mapping = pl.DataFrame(mapping_data)

        df_final = (
            df_raw
            .join(df_mapping, on="file_path", how="left")
            .drop(["file_path", "filename"])
        )

        return df_final

    # ===========================
    # CURRENT SILVER
    # ===========================
    def get_current_silver(
        self,
        mode: Literal["full", "reload", "incremental", "reload_date"],
        hub_path: Path,
        sat_path: Path,
        link_path: Path = None
    ) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:

        df_hub = pl.DataFrame()
        df_sat = pl.DataFrame()
        df_link = pl.DataFrame()

        if hub_path.exists() and any(hub_path.rglob("*.parquet")):
            pattern = str(hub_path / "**" / "*.parquet")
            df_hub = self.duckdb_conn.execute(
                f"""
                SELECT * 
                FROM read_parquet('{pattern}', union_by_name=True)
                QUALIFY ROW_NUMBER() OVER (PARTITION BY hk_comment ORDER BY load_date DESC) = 1
                """
            ).pl()

        if sat_path.exists() and any(sat_path.rglob("*.parquet")):
            pattern = str(sat_path / "**" / "*.parquet")
            df_sat = self.duckdb_conn.execute(
                f"""
                SELECT * 
                FROM read_parquet('{pattern}', union_by_name=True) 
                {
                    """QUALIFY ROW_NUMBER() OVER (PARTITION BY hk_comment ORDER BY valid_from DESC) = 1 
                    AND valid_to IS NULL 
                    ORDER BY load_date""" 
                    if mode == "incremental" else ""
                }
                """
            ).pl()

        if link_path and link_path.exists() and any(link_path.rglob("*.parquet")):
            pattern = str(link_path / "**" / "*.parquet")
            df_link = self.duckdb_conn.execute(
                f"""
                SELECT *
                FROM read_parquet('{pattern}', union_by_name=True) 
                QUALIFY ROW_NUMBER() OVER (PARTITION BY hk_comment, hk_video ORDER BY load_date DESC) = 1 
                ORDER BY load_date
                """
            ).pl()

        return df_hub, df_sat, df_link

    # ===========================
    # HELPERS
    # ===========================
    def generate_hash_key(self, s: pl.Series) -> pl.Series:
        return s.map_elements(
            lambda x: hashlib.md5(str(x).strip().lower().encode()).hexdigest(),
            return_dtype=pl.String
        )

    def truncate_data(self, silver_path: Path):
        if silver_path.exists():
            shutil.rmtree(silver_path)
        silver_path.mkdir(parents=True, exist_ok=True)

    def safe_col(self, df: pl.DataFrame, col_name: str, default: Any = None, type: dtypes = dtypes.String) -> pl.Expr:
        if col_name in df.columns:
            return pl.col(col_name)
        return pl.lit(default, allow_object=True).cast(type).alias(col_name)

    def safe_bool(self, df: pl.DataFrame, col_name: str, default=None) -> pl.Expr:
        values = {
            "true": True, "false": False, "1": True, "0": False,
            "True": True, "False": False, None: None
        }

        return (
            self.safe_col(df, col_name, default=default)
            .cast(pl.String, strict=False)
            .replace_strict(values, default=False)
            .cast(pl.Boolean, strict=False)
            .alias(col_name)
        )

    def safe_numeric(self, df: pl.DataFrame, col_name: str, dtype=pl.Int64, default: Any = None) -> pl.Expr:
        return self.safe_col(df, col_name, default=default).cast(dtype, strict=False).alias(col_name)

    def safe_date(self, df: pl.DataFrame, col_name: str, dtype=pl.Date, default: Any = None) -> pl.Expr:
        if col_name in df.columns:
            return pl.col(col_name).cast(dtype, strict=False).alias(col_name)
        return pl.lit(default).cast(dtype).alias(col_name)

    # ===========================
    # TRANSFORM BRONZE
    # ===========================
    def transform_bronze(
        self,
        bronze: pl.DataFrame = None
    ) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        
        try:

            if bronze is None or bronze.is_empty():
                return (pl.DataFrame(),) * 4

            if "comment_id" not in bronze.columns:
                raise ValueError("Missing required column: comment_id")

            # ==========================
            # SCHEMA
            # ==========================
            SILVER_EXPECTED = {
                "video_id": pl.String,
                "comment_id": pl.String,
                "hk_video": pl.String,
                "hk_comment": pl.String,
                "hash_diff": pl.String,
                "comment_text": pl.String,
                "comment_original_text": pl.String,
                "comment_author": pl.String,
                "comment_author_url": pl.String,
                "comment_publish_date": pl.Datetime(time_unit='us'),
                "comment_update_date": pl.Datetime(time_unit='us'),
                "watermark_date": pl.Datetime(time_unit='us'),
                "comment_like_count": pl.Int64,
                "comment_can_reply": pl.Boolean,
                "comment_reply_count": pl.Int64,
                "comment_is_public": pl.Boolean,
                "load_date": pl.Datetime(time_unit='us'),
                "source": pl.String,
                "run_id": pl.String,
                "date_of_data": pl.Date,
                "valid_from": pl.String,
                "valid_to": pl.String,
                "is_current": pl.Boolean
            }

            # ==========================
            # DEDUP
            # ==========================
            if "comment_update_date" in bronze.columns:
                data = bronze.sort(
                    "comment_update_date", descending=True
                ).unique(subset=["comment_id"], keep="first")
            else:
                data = bronze

            # ==========================
            # UNNEST
            # ==========================
            if "etl_metadata" in data.columns and isinstance(
                data.get_column("etl_metadata").dtype, pl.Struct
            ):
                data = data.unnest("etl_metadata")

            # ==========================
            # HASH KEYS
            # ==========================
            data = data.with_columns([
                pl.col("video_id").pipe(self.generate_hash_key).alias("hk_video"),
                pl.col("comment_id").pipe(self.generate_hash_key).alias("hk_comment"),
                self.safe_col(data, "valid_from").cast(pl.String, strict=False),
                self.safe_col(data, "valid_to").cast(pl.String, strict=False),
                self.safe_bool(data, "is_current")
            ])

            # ==========================
            # HASH DIFF
            # ==========================
            diff_columns = [
                "comment_text",
                "comment_update_date",
                "comment_like_count",
                "comment_reply_count"
            ]

            data = data.with_columns(
                pl.struct(diff_columns)
                .pipe(self.generate_hash_key)
                .alias("hash_diff")
            )

            # ==========================
            # CAST + VALIDATION
            # ==========================
            expressions = []

            for col, dtype in SILVER_EXPECTED.items():

                if dtype == pl.Boolean:
                    expr = self.safe_bool(data, col)

                elif dtype == dtypes.String:
                    expr = self.safe_col(data, col, type=dtype)

                elif dtype in [pl.Int64, pl.Float64]:
                    expr = self.safe_numeric(data, col, dtype)

                elif isinstance(dtype, dtypes.Datetime) or dtype == pl.Date:
                    expr = self.safe_date(data, col, dtype)

                else:
                    expr = pl.lit(None).cast(dtype, strict=False).alias(col)

                expressions.append(expr)

            casted = data.with_columns(expressions)

            invalid_conditions = []

            for col, dtype in SILVER_EXPECTED.items():
                if col in data.columns:
                    invalid_conditions.append(
                        pl.col(col).is_not_null() &
                        pl.col(col).cast(dtype, strict=False).is_null()
                    )

            invalid_mask = casted.select(
                pl.any_horizontal(invalid_conditions).alias("invalid")
            )["invalid"]

            valid = casted.filter(~invalid_mask)
            invalid = casted.filter(invalid_mask)

            if valid.is_empty():
                return pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), invalid

            # ==========================
            # HUB
            # ==========================
            hub = valid.select(
                ["comment_id", "hk_comment", "load_date", "source"]
            ).unique("hk_comment")

            # ==========================
            # LINK
            # ==========================
            link = valid.select([
                "hk_comment",
                "hk_video",
                "load_date",
                "source",
                pl.struct(["hk_comment", "hk_video"])
                .pipe(self.generate_hash_key)
                .alias("hk_link_comment_video")
            ]).unique()

            # ==========================
            # SATELITE
            # ==========================
            valid = valid.drop(["hk_video", "video_id", "channel_handle"])

            return hub, valid, link, invalid
        
        except Exception as e:
            raise e

    # ===========================
    # SCD2
    # ===========================
    def get_new_silver(
        self, 
        mode: Literal["full", "reload", "incremental", "reload_date"], 
        date_of_data: str, 
        current_hub: pl.DataFrame,
        new_hub: pl.DataFrame,
        current_satelite: pl.DataFrame,
        new_satelite: pl.DataFrame,
        current_link: pl.DataFrame,
        new_link: pl.DataFrame
    ) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        
        hub_delta = pl.DataFrame()
        sat_delta = pl.DataFrame()
        link_delta = pl.DataFrame()
        
        current_satelite = current_satelite.clone() if not current_satelite.is_empty() else pl.DataFrame()
        current_hub = current_hub.clone() if not current_hub.is_empty() else pl.DataFrame()
        current_link = current_link.clone() if not current_link.is_empty() else pl.DataFrame()

        try:
            
            reload_date = date.fromisoformat(date_of_data)
            prev_day = (reload_date - timedelta(days=1)).strftime("%Y-%m-%d")

            # ==========================================================
            # HUB
            # ==========================================================

            if new_hub is not None and not new_hub.is_empty():

                if current_hub.is_empty():
                    hub_delta = new_hub.clone()

                else:
                    hub_delta = new_hub.join(
                        current_hub.select("hk_comment"),
                        on="hk_comment",
                        how="anti"
                    )

            
            # ==========================================================
            # HUB
            # ==========================================================

            if new_link is not None and not new_link.is_empty():

                if current_hub.is_empty():
                    link_delta = new_link.clone()

                else:
                    link_delta = new_link.join(
                        current_link.select("hk_comment"),
                        on="hk_link_comment_video",
                        how="anti"
                    )


            if mode == "reload_date" and new_satelite is not None and not new_satelite.is_empty():
            # ------------------------------------------------------
            # REF RECORD
            # odpowiednik:
            #
            # SELECT DISTINCT ON(hk_video)
            # ORDER BY valid_from DESC
            # ------------------------------------------------------

                ref_record = current_satelite.filter(
                    (pl.sql_expr("CAST(valid_from AS DATE)") < reload_date)
                    &
                    (
                        current_satelite["valid_to"].is_null()
                        |
                        (
                            pl.sql_expr("CAST(valid_to AS DATE)")
                            >= reload_date
                        )
                    )
                ).clone()

                if not ref_record.is_empty():
                    ref_record = (
                        ref_record
                        .sort(
                            "valid_from",
                            descending=True
                        )
                        .unique(
                            subset=["hk_comment"],
                            keep="first"
                        )
                    )

                # ------------------------------------------------------
                # CLOSE OLD RECORDS
                # ------------------------------------------------------

                if not ref_record.is_empty():
                    compare = ref_record.select(
                        ["hk_comment", "hash_diff"]
                    ).join(
                        new_satelite.select(
                            ["hk_comment", "hash_diff"]
                        ),
                        on="hk_comment",
                        how="inner",
                        suffixes=("_old", "_new")
                    )


                    changed = compare.filter(
                        pl.col("hash_diff_old") != pl.col("hash_diff_new")
                    ).select("hk_comment").unique().to_series()

                    close_keys = (
                        ref_record
                        .filter(pl.col("hk_comment").is_in(changed))
                        .select(["hk_comment", "valid_from"])
                    )

                    current_satelite = current_satelite.join(
                        close_keys.with_columns(pl.lit(True).alias("close_record")),
                        on=["hk_comment", "valid_from"],
                        how="left"
                    )

                    current_satelite = current_satelite.with_columns(
                        pl.when(pl.col("close_record") == True)
                            .then(pl.lit(prev_day))
                            .otherwise(pl.col("valid_to"))
                            .alias("valid_to"),

                        pl.when(pl.col("close_record") == True)
                            .then(False)
                            .otherwise(pl.col("is_current"))
                            .alias("is_current")
                    )
                # ------------------------------------------------------
                # DELETE RELOAD DATE
                # ------------------------------------------------------

                current_satelite = current_satelite.filter(
                    date.fromisoformat(pl.col("valid_from")) != reload_date
                ).clone()

                # ======================================================
                # NEXT VALID FROM
                # ======================================================

                future = current_satelite.filter(
                    date.fromisoformat(pl.col("valid_from")) > reload_date
                )

                if not future.is_empty():
                    next_valid = (
                        future
                        .group_by("hk_comment").agg(pl.col("valid_from").min())
                        .rename({"valid_from": "next_valid_from"})
                    )

                else:
                    next_valid = pl.DataFrame(
                        columns=["hk_comment", "next_valid_from"]
                    )

                # ======================================================
                # OPEN NEW RECORDS
                # ======================================================

                if ref_record.is_empty():
                    ref_hash = pl.DataFrame(
                        schema={
                            "hk_comment": pl.String,
                            "hash_diff": pl.String
                        }
                    )

                else:
                    ref_hash = ref_record.select(["hk_comment","hash_diff"])

                sat_delta = (
                    new_satelite
                    .join(
                        ref_hash,
                        on="hk_comment",
                        how="left",
                        suffix="_old"
                    )
                )

                sat_delta = (
                    sat_delta
                    .filter(
                        pl.col("hash_diff_old").is_null()
                        |
                        (pl.col("hash_diff") != pl.col("hash_diff_old"))
                    )
                )

                sat_delta = (
                    sat_delta
                    .join(
                        next_valid,
                        on="hk_comment",
                        how="left"
                    )
                )

                sat_delta = sat_delta.with_columns(
                    pl.lit(date_of_data).alias("valid_from")
                )

                sat_delta = sat_delta.with_columns(
                    pl.lit(None).cast(pl.String).alias("valid_to")
                )

                sat_delta = sat_delta.with_columns(
                    pl.when(pl.col("next_valid_from").is_not_null())
                    .then(
                        (pl.col("next_valid_from").str.to_date() - pl.duration(days=1))
                        .cast(pl.String)
                    )
                    .otherwise(pl.col("valid_to"))
                    .alias("valid_to")
                )

                sat_delta = sat_delta.with_columns(
                    pl.col("next_valid_from").is_null().alias("is_current")
                )

                sat_delta = sat_delta.drop(["hash_diff_old", "next_valid_from"])

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

            if new_satelite is None or new_satelite.is_empty():
                return (
                    hub_delta,
                    sat_delta,
                    link_delta,
                    current_hub,
                    current_satelite,
                    current_link
                )

            if current_satelite.is_empty():
                sat_delta = (
                    new_satelite
                    .with_columns(
                        pl.lit(date_of_data).alias("valid_from"),
                        pl.lit(None).cast(pl.String).alias("valid_to"),
                        pl.lit(True).alias("is_current")
                    )
                )

                return (
                    hub_delta,
                    sat_delta,
                    link_delta,
                    current_hub,
                    current_satelite,
                    current_link
                )

            current_open = (
                current_satelite
                .filter(pl.col("valid_to").is_null())
                .sort("valid_from")
                .unique(
                    subset=["hk_comment"],
                    keep="last"
                )
            )

            merged = (
                new_satelite
                .join(
                    current_open.select(["hk_comment","hash_diff"]),
                    on="hk_comment",
                    how="left",
                    suffix="_old"
                )
            )

            changed_condition = (
                pl.col("hash_diff_old").is_null()
                |
                (pl.col("hash_diff") != pl.col("hash_diff_old"))
            )


            to_open = (
                merged
                .filter(changed_condition)
                .drop("hash_diff_old")
                .with_columns(
                    pl.lit(date_of_data).alias("valid_from"),
                    pl.lit(None).cast(pl.String).alias("valid_to"),
                    pl.lit(True).alias("is_current")
                )
            )

            close_keys = (
                merged
                .filter(
                    pl.col("hash_diff_old").is_not_null()
                    &
                    (pl.col("hash_diff") != pl.col("hash_diff_old"))
                )
                .select("hk_comment")
                .unique()
            )

            if not close_keys.is_empty():
                current_satelite = (
                    current_satelite
                    .join(
                        close_keys.with_columns(
                            pl.lit(True)
                            .alias("close_record")
                        ),
                        on="hk_comment",
                        how="left"
                    )
                    .with_columns(
                        pl.when(
                            pl.col("close_record").eq(True)
                                & pl.col("valid_to").is_null()
                        )
                        .then(pl.lit(prev_day))
                        .otherwise(pl.col("valid_to"))
                        .alias("valid_to"),

                        pl.when(
                            pl.col("close_record").eq(True)
                                & (pl.col("valid_to") == prev_day)
                        )
                        .then(False)
                        .otherwise(pl.col("is_current"))
                        .alias("is_current")
                    )
                    .drop("close_record")
                )


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
                pl.DataFrame(),
                pl.DataFrame(),
                pl.DataFrame(),
                current_hub,
                current_satelite,
                current_link
            )

    # ===========================
    # LOAD
    # ===========================
    def load_silver(
        self,
        run_id: str,
        mode: Literal["full", "incremental", "reload", "reload_date"],
        channel: str,
        bronze_wm_path: Path,
        silver_wm_path: Path | None = None,
        date_of_data: str = None
    ):
        hub = pl.DataFrame()
        satelite = pl.DataFrame()
        link = pl.DataFrame()
        invalid = pl.DataFrame()

        try:
            hub_path, sat_path, link_path, invalid_path, _ = wtm.get_data_dir_pattern(
                layer="silver",
                dataset_type="comments",
                mode=mode,
                status="success",
                channel=channel
            )

            bronze = self.get_new_bronze(mode, bronze_wm_path, silver_wm_path, date_of_data)
            
            if bronze is None or bronze.is_empty():
                print(f"[{run_id}] No new bronze data — skipping")
                return

            hub_new, sat_new, link_new, invalid = self.transform_bronze(bronze)
            
            if mode in ["incremental", "reload_date"]:
                if bronze is None or bronze.is_empty():
                    print(f"[{run_id}] No new bronze data — skipping")
                    return

                current_hub, current_sat, current_link = self.get_current_silver(
                    mode, hub_path, sat_path, link_path
                )

                hub_delta, sat_delta, link_delta, current_hub, current_sat, current_link = self.get_new_silver(
                    mode, date_of_data,
                    current_hub, hub_new,
                    current_sat, sat_new,
                    current_link, link_new
                )

                # ==============================================================
                # RELOAD_DATE
                # ==============================================================
                if mode == "reload_date":
                    # SATELITE
                    satelite = pl.concat([current_sat, sat_delta], how="diagonal_relaxed") \
                        if not current_sat.is_empty() else sat_delta
                    
                    satelite = satelite.sort(["hk_comment", "valid_from"]) \
                                       .unique(subset=["hk_comment", "valid_from"], keep="last")

                    # HUB
                    hub = pl.concat([current_hub, hub_delta], how="diagonal_relaxed") \
                        if not current_hub.is_empty() else hub_delta
                    hub = hub.unique(subset=["hk_comment"], keep="last")

                    # LINK
                    link = pl.concat([current_link, link_delta], how="diagonal_relaxed") \
                        if not current_link.is_empty() else link_delta
                    link = link.unique(subset=["hk_link_comment_video"], keep="last")

                    self.truncate_data(sat_path)
                    self.truncate_data(hub_path)
                    self.truncate_data(link_path)

                # ==============================================================
                # INCREMENTAL
                # ==============================================================
                else:
                    hub = hub_delta
                    satelite = sat_delta
                    link = link_delta

            # ==============================================================
            # FULL / RELOAD
            # ==============================================================
            else:
                self.truncate_data(hub_path)
                self.truncate_data(sat_path)
                self.truncate_data(link_path)

                hub = hub_new
                satelite = sat_new
                link = link_new

                if not satelite.is_empty():
                    satelite = satelite.with_columns([
                        pl.lit(date_of_data).alias("valid_from"),
                        pl.lit(None).alias("valid_to"),
                        pl.lit(True).alias("is_current")
                    ])

            # ==============================================================
            # SAVE
            # ==============================================================
            self.save_silver(
                run_id=run_id,
                hub=hub,
                sat=satelite,
                link=link,
                invalid=invalid,
                hub_path=hub_path,
                sat_path=sat_path,
                link_path=link_path,
                invalid_path=invalid_path
            )

            # Watermark
            if not satelite.is_empty():
                watermark_value = satelite.select(
                    pl.col("watermark_date").max().cast(pl.String)
                ).item()
            else:
                watermark_value = None

            wtm.save_watermark(
                layer="silver",
                channel=channel,
                dataset_type="comments",
                watermark_column="watermark_date",
                watermark_value=watermark_value,
                run_id=run_id,
                date_of_data=date_of_data,
                mode=mode,
                status="success"
            )

            print(f"[{run_id}] Silver load SUCCESS")

        except Exception as e:
            traceback.print_exc()

            wtm.save_watermark(
                layer="silver",
                channel=channel,
                dataset_type="comments",
                watermark_column="watermark_date",
                watermark_value=None,
                run_id=run_id,
                date_of_data=date_of_data,
                mode=mode,
                status="failed",
                exception=str(e)
            )

            print(f"[{run_id}] Silver load FAILED")
            raise e

    # ===========================
    # SAVE
    # ===========================
    def save_silver(
        self,
        run_id: str,
        hub: pl.DataFrame,
        sat: pl.DataFrame,
        link: pl.DataFrame,
        invalid: pl.DataFrame,
        hub_path: Path,
        sat_path: Path,
        link_path: Path,
        invalid_path: Path
    ):
        if not hub.is_empty():
            hub.write_parquet(hub_path / f"hub_{run_id}.parquet")

        if not sat.is_empty():
            sat.write_parquet(sat_path / f"sat_{run_id}.parquet")

        if not link.is_empty():
            link.write_parquet(link_path / f"link_{run_id}.parquet")

        if not invalid.is_empty():
            invalid.write_parquet(invalid_path / f"link_{run_id}.parquet")