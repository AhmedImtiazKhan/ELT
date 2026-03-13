import logging

logger = logging.getLogger(__name__)
table = "yt_api"


def insert_row(cur, conn, schema, row):
    try:
        cur.execute(
            f"""INSERT INTO {schema}.{table}
                (video_id, video_title, upload_date, duration, video_views, likes_count, comments_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (
                row["video_id"],
                row["video_title"],
                row["upload_date"],
                row["duration"],
                row["video_views"],
                row["likes_count"],
                row["comments_count"],
            ),
        )
        conn.commit()
        logger.info(f"Inserted video {row['video_id']} into {schema}.{table}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert video {row['video_id']}: {e}")
        raise


def update_row(cur, conn, schema, video_id, updates):
    if not updates:
        logger.warning("No updates provided")
        return

    set_clause = ", ".join([f"{col} = %s" for col in updates.keys()])
    values = list(updates.values()) + [video_id]

    try:
        cur.execute(
            f"""UPDATE {schema}.{table}
                SET {set_clause}
                WHERE video_id = %s""",
            values,
        )
        conn.commit()
        logger.info(f"Updated video {video_id} in {schema}.{table}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to update video {video_id}: {e}")
        raise


def delete_row(cur, conn, schema, video_id):
    try:
        cur.execute(
            f"""DELETE FROM {schema}.{table}
                WHERE video_id = %s""",
            (video_id,),
        )
        conn.commit()
        logger.info(f"Deleted video {video_id} from {schema}.{table}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to delete video {video_id}: {e}")
        raise
