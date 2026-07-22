from airflow.decorators import dag, task
from airflow.models import Param
from pendulum import datetime
from pathlib import Path
from dotenv import load_dotenv
from src.bronze import extract as e
from src.silver import transform as t
from src.gold import load_statistics as l
from airflow.utils.email import send_email
import uuid


def notify_success(context):
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    send_email(
        to=["kacper050710@gmail.com"],
        subject=f"Airflow Success: {dag_id}",
        html_content=f"<h3>DAG {dag_id} zakończył się sukcesem!</h3><p>Run ID: {run_id}</p>"
    )

def notify_failure(context):
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    send_email(
        to=["kacper050710@gmail.com"],
        subject=f"Airflow Failure: {dag_id}",
        html_content=f"<h3>DAG {dag_id} wywalił się!</h3><p>Sprawdź logi dla Run ID: {run_id}</p>"
    )

default_args = {
    "owner": "kacper",
}

@dag(
    dag_id="load_youtube_data",
    start_date=datetime(year=2026, month=6, day=21, tz="Europe/Warsaw"),
    schedule="0 9 * * *",
    catchup=False,
    params={
        "channels": Param(
            ["@nieinformatyk", "@DataWithBaraa"], 
            type="array", 
            description="youtube channels"
        ), 
        "mode": Param(
            "incremental",
            enum=["full", "reload", "incremental", "reload_date"],
            type="string",
            description="mode",
        ),
        "start_ETL_from": Param(
            "bronze",
            enum=["bronze", "silver", "gold"],
            type="string",
            description="from which layer you want to run pipeline"
        )
    },
    max_active_tasks=5,
    max_active_runs=3,
    default_args=default_args,
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
)
def load_youtube_data():

    @task.python
    def prepare_extraction_args(batch_id, ds=None, run_id=None, **context):
        channels = context["params"]["channels"]
        start_ETL_from = context["params"]["start_ETL_from"]
        
        if run_id and (run_id.startswith("manual__") or run_id.startswith("backfill__")):
            mode = context["params"]["mode"]
            set_mode_on_airflow = True
        else:
            mode = "incremental"
            set_mode_on_airflow = False

        return [
            {
                "run_id": batch_id,
                "channel": channel,
                "date_of_data": ds,
                "mode": mode,
                "set_mode_on_airflow": set_mode_on_airflow,
                "start_ETL_from": start_ETL_from
            }
            for channel in channels
        ]

    @task.python(multiple_outputs=True)
    def prepare_metadata(ds=None, run_id=None, **context):
        channels = context["params"]["channels"]
        start_ETL_from = context["params"]["start_ETL_from"]
        
        if run_id and (run_id.startswith("manual__") or run_id.startswith("backfill__")):
            mode = context["params"]["mode"]
            set_mode_on_airflow = True
        else:
            mode = "incremental"
            set_mode_on_airflow = False

        batch_id = str(uuid.uuid4().hex)
        
        return {
            "batch_id": batch_id,
            "channels": channels,
            "mode": mode,
            "date_of_data": ds,
            "set_mode_on_airflow": set_mode_on_airflow,
            "start_ETL_from": start_ETL_from
        }


    @task.python
    def extract_single_channel(channel_args):
        if channel_args["start_ETL_from"] == "bronze":
            base_dir = Path(__file__).resolve().parents[2]
            dotenv_path = base_dir / "config" / ".env"
            load_dotenv(dotenv_path=dotenv_path)

            e.process_channel(
                run_id=channel_args["run_id"],
                channel=channel_args["channel"],
                date_of_data=channel_args["date_of_data"],
                mode=channel_args["mode"]
            )

    @task.python
    def transform(batch_id, channels, date_of_data, mode, set_mode_on_airflow, start_ETL_from):
        if start_ETL_from in ["silver", "gold"]:
            t.transform_channels(
                run_id=batch_id, 
                channels=channels, 
                date_of_data=date_of_data, 
                mode=mode
            )

        return {
            "batch_id": batch_id,
            "mode": mode,
            "date_of_data": date_of_data,
            "set_mode_on_airflow": set_mode_on_airflow,
            "start_ETL_from": start_ETL_from 
        }

    @task.python
    def load(transform_results):
        l.load(
            run_id=transform_results["batch_id"], 
            date_of_data=transform_results["date_of_data"], 
            mode=transform_results["mode"],
            set_mode_on_airflow=transform_results["set_mode_on_airflow"]
        )
        return transform_results

    meta = prepare_metadata()
    extraction_args = prepare_extraction_args(batch_id=meta["batch_id"])
    
    extraction = extract_single_channel.expand(channel_args=extraction_args)
    
    transformation = transform(
        batch_id=meta["batch_id"],
        channels=meta["channels"],
        date_of_data=meta["date_of_data"],
        mode=meta["mode"],
        set_mode_on_airflow=meta["set_mode_on_airflow"],
        start_ETL_from=meta["start_ETL_from"]
    )
    
    loading = load(transformation)

    extraction >> transformation >> loading

load_youtube_data()