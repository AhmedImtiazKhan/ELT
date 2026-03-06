from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

local_tz = pendulum.timezone("Asia/Karachi")

default_args = {
"owner": "dataengineers",
"depends_on_past": False,
"email_on_failure": False,
"email": "ahmed.imtiaz2000@gmail.com" ,
# 'retries': 1,
# 'retry_delay': timedelta(minutes=5),
"max_active_runs": 1,
"dagrun_timeout": timedelta(hours=1),
"start_date": datetime(2025, 1, 1, tzinfo=local_tz),
# 'end _date':
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='Produce JSON file from YouTube API',
    schedule='0 14 * * *',
    catchup=False,
) as dag:

    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task=save_to_json(extract_data)


   #Define the dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json_task