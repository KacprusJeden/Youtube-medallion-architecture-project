import json
from pathlib import Path
from typing import List, Literal, Tuple
from datetime import datetime
import traceback
import pandas as pd
from duckdb import DuckDBPyConnection

# =========================================================
# PROJECT_ROOT
# =========================================================
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# =========================================================
# PATHS
# =========================================================
def get_watermark_path(layer: Literal["bronze", "silver", "gold"], channel: str, dataset_type: str) -> Path:
    full_path = PROJECT_ROOT / Path(f"data/{layer}/youtube_api/{dataset_type}")

    match layer:
        case "bronze":
            full_path = full_path / f"{channel}/{dataset_type}_watermark.jsonl"
        case "silver" | "gold":
            full_path = full_path / f"watermarks/{channel}_watermark.jsonl"
        case _:
            raise ValueError("Layer must be bronze, silver or gold")

    full_path.parent.mkdir(parents=True, exist_ok=True)

    if not full_path.exists():
        full_path.touch()

    return full_path


# =========================================================
# DATA DIR
# =========================================================
def get_data_dir_pattern(
    layer: Literal["bronze", "silver"],
    dataset_type: str,
    mode: Literal["full", "incremental", "reload", "reload_date"],
    status: Literal["success", "failed"],
    channel: str = None,
    date_of_data: str = None,
    run_id: str = None,
    load_gold: bool = False
) -> Path | Tuple[Path, Path, Path, Path, Path]:

    base_dir = "data" if status == "success" else "data/errors"

    if layer == "bronze" and channel:
        path = PROJECT_ROOT / Path(f"{base_dir}/{layer}/youtube_api/{dataset_type}")
        if channel and mode and date_of_data and run_id:
            path = path / channel / mode / date_of_data / run_id
            path.mkdir(parents=True, exist_ok=True)
        return path

    elif layer == "silver":
        normal = ["hubs", "satellites", "links"]
        errors = ["invalid", "reports"]

        paths = []

        if load_gold:
            for cat in normal:
                main = "data/errors" if (cat in errors or status == "failed") else "data"
                p = PROJECT_ROOT / Path(f"data/{layer}/youtube_api/{dataset_type}/{cat}")
                p.mkdir(parents=True, exist_ok=True)
                paths.append(p)
        else:
            for cat in normal + errors:
                main = "data/errors" if (cat in errors or status == "failed") else "data"
                p = PROJECT_ROOT / Path(f"{main}/{layer}/youtube_api/{dataset_type}/{cat}/{channel}")
                p.mkdir(parents=True, exist_ok=True)
                paths.append(p)

        return tuple(paths)


# =========================================================
# WATERMARK COMPUTE
# =========================================================
def compute_watermark(data: list, column: str) -> str | None:
    try:
        values = [
            row.get(column) if isinstance(row, dict) else row
            for row in data if row
        ]
        values = [v for v in values if v]
        return max(values) if values else None
    except Exception:
        return None


# =========================================================
# GET LAST WATERMARK
# =========================================================
def get_last_success_watermark(
    watermark_path: Path,
    dataset_type: str,
    mode: Literal["full", "incremental", "reload", "reload_date"] = None,
    date_of_data: str = None
) -> str | None:

    dataset_key = f"youtube_{dataset_type}"

    if not watermark_path.exists() or watermark_path.stat().st_size == 0:
        return None

    records = []
    with open(watermark_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except:
                continue

    success = [
        r[dataset_key]
        for r in records
        if r.get(dataset_key, {}).get("status") == "success"
    ]

    if not success:
        return None

    # for reload_date
    if mode in ["reload", "full"] and date_of_data:
        filtered = [
            r for r in success
            if r["date_of_data"] == date_of_data and r["mode"] == mode
        ]
        return filtered[-1]["watermark_value"] if filtered else None

    # global max
    success.sort(
        key=lambda x: (x.get("watermark_value") or "", x.get("date_of_data")),
        reverse=True
    )
    return success[0]["watermark_value"]


# =========================================================
# PENDING RUNS
# =========================================================
def get_pending_runs(
    dataset_type: str,
    mode: Literal["full", "incremental", "reload", "reload_date"],
    duckdb_conn: DuckDBPyConnection,
    bronze_watermark_path: Path,
    silver_watermark_path: Path = None,
    date_of_data: str = None
) -> List[dict]:

    dataset_key = f"youtube_{dataset_type}"

    if mode != "reload_date":
        date_of_data = None

    if not bronze_watermark_path.exists():
        return []

    # =========================================================
    # LOAD BRONZE
    # =========================================================
    if bronze_watermark_path.stat().st_size == 0:
        return []

    raw = pd.read_json(bronze_watermark_path, lines=True)

    if dataset_key not in raw.columns:
        return []

    bronze_df = pd.json_normalize(raw[dataset_key])
    bronze_df = bronze_df.loc[bronze_df["status"] == "success"]

    if bronze_df.empty:
        return []

    duckdb_conn.register("bronze", bronze_df)

    # =========================================================
    # LOAD SILVER
    # =========================================================
    if not (silver_watermark_path and silver_watermark_path.exists()) \
            or silver_watermark_path.stat().st_size == 0:

        silver_df = pd.DataFrame(columns=["run_id"])

    else:
        raw = pd.read_json(silver_watermark_path, lines=True)

        if dataset_key not in raw.columns:
            silver_df = pd.DataFrame(columns=["run_id"])
        else:
            silver_df = pd.json_normalize(raw[dataset_key])
            silver_df = silver_df.loc[silver_df["status"] == "success"]

    duckdb_conn.register("silver", silver_df)

    # =========================================================
    # LOGIC (no SNAPSHOT)
    # =========================================================

    # FULL / RELOAD
    if mode in ["full", "reload"]:
        query = """
            SELECT run_id, date_of_data, data_dir
            FROM bronze
            ORDER BY load_timestamp
        """
        params = None

    # INCREMENTAL = NOT EXISTS
    elif mode == "incremental":
        query = """
            SELECT run_id, date_of_data, data_dir
            FROM bronze b
            WHERE NOT EXISTS (
                SELECT 1 FROM silver s
                WHERE b.run_id = s.run_id
            )
            ORDER BY load_timestamp
        """
        params = None

    # RELOAD_DATE
    elif mode == "reload_date":

        if not date_of_data:
            raise ValueError("date_of_data required")

        query = """
            SELECT run_id, date_of_data, data_dir
            FROM bronze
            WHERE date_of_data = ?
            ORDER BY load_timestamp
        """
        params = [date_of_data]

    else:
        raise ValueError(f"Unsupported mode: {mode}")

    df = duckdb_conn.execute(query, params).df() if params else duckdb_conn.execute(query).df()

    return df.to_dict("records")


# =========================================================
# SAVE WATERMARK
# =========================================================
def save_watermark(
    layer: Literal["bronze", "silver", "gold"],
    channel: str,
    dataset_type: str,
    watermark_column: str,
    watermark_value: str,
    run_id: str,
    date_of_data: str = None,
    mode: Literal["full", "incremental"] = "incremental",
    status: Literal["failed", "success"] = "success",
    exception: Exception = None
):

    file_path = get_watermark_path(layer, channel, dataset_type)
    dataset_key = f"youtube_{dataset_type}"

    data = {
        dataset_key: {
            "channel": channel,
            "run_id": run_id,
            "date_of_data": date_of_data,
            "mode": mode,
            "status": status,
            "watermark_column": watermark_column,
            "watermark_value": watermark_value,
            "load_timestamp": datetime.now().isoformat()
        }
    }

    if layer in ["bronze", "gold"]:
        data_dir = get_data_dir_pattern(layer, dataset_type, mode, status, channel, date_of_data, run_id)
        data[dataset_key]["data_dir"] = str(data_dir)

    elif layer == "silver":
        hub, sat, link, inv, rep = get_data_dir_pattern(layer, dataset_type, mode, status, channel, date_of_data, run_id)
        data[dataset_key].update({
            "hub_dir": str(hub),
            "satelite_dir": str(sat),
            "link_dir": str(link),
            "invalid_dir": str(inv),
            "failure_report_dir": str(rep)
        })

    if exception:
        data[dataset_key].update({
            "error": str(exception),
            "traceback": traceback.format_exc()
        })

    write_mode = "w" if mode == "full" else "a"

    with open(file_path, write_mode, encoding="utf-8") as f:
        f.write(json.dumps(data) + "\n")