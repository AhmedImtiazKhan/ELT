from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_ids
from datawarehouse.datatransformations import transform_data
from datawarehouse.datamodification import insert_row, update_row, delete_row
from datawarehouse.data_loading import load_path

import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():

    schema = "staging"
    cur, conn = None, None

    try:
        cur, conn = get_conn_cursor()
        YT_data = load_path()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        for row in YT_data:

            if len(table_ids) == 0:
                insert_row(cur, conn, schema, row)

            else:
                if row['video_id'] in table_ids:
                    update_row(cur, conn, schema, row)
                else:
                    insert_row(cur, conn, schema, row)
            
        ids_in_json = [row['video_id'] for row in YT_data]

        ids_to_delete = set(table_ids) - set(ids_in_json)

        if ids_to_delete:
            for video_id in ids_to_delete:
                delete_row(cur, conn, schema, video_id)

        logger.info(f"{schema}.{table} table update completed successfully")

    except Exception as e:
        logger.error(f"An error occured during the update of {schema} table: {e}")

    finally:
        if cur and conn:
            close_conn_cursor(cur, conn)
    

@task
def core_table():
    schema = "core"
    cur, conn = None, None

    try:
        cur, conn = get_conn_cursor()
        

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)
        
        current_video_ids = set()

        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        for row in rows:

            current_video_ids.add(row['video_id'])
            
            transformed_row = transform_data(row)

            if row['video_id'] in table_ids:
                update_row(cur, conn, schema, row['video_id'], transformed_row)
            else:
                insert_row(cur, conn, schema, transformed_row)

        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            for video_id in ids_to_delete:
                delete_row(cur, conn, schema, video_id)

        logger.info(f"{schema}.{table} table update completed successfully")

    except Exception as e:
        logger.error(f"An error occured during the update of {schema} table: {e}")
        raise e
    finally:
        if cur and conn:
            close_conn_cursor(cur, conn)
        