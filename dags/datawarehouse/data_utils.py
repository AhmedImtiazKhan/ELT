from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor


def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return cur, conn


def close_conn_cursor(cur, conn):
    cur.close()
    conn.close()

def create_schema(schema):
    cur, conn = get_conn_cursor()

    schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema}"

    cur.execute(schema_query)

    conn.commit()

    close_conn_cursor(cur, conn)
    

def create_table(schema):

    conn, cur = get_conn_cursor()

    if schema == 'staging':
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {staging}.{table}(
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload _Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments _Count" INT            
            );
        """
    else:
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table}(
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload _Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments _Count" INT            
            );
        """

    curr.execute(table_sql)

    conn.commit()

    close_conn_cursor(cur, conn) 


def get_video_ids(cur,schema):

    cur.execute(f"""SELECT Video_ID FROM {scehma}.{table};""")
    ids = cur.fetchall()

    video_ids = [row['Video_ID'] for row in ids]

    return video_ids

