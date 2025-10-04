from psycopg_pool import ConnectionPool
import os
from dotenv import load_dotenv

load_dotenv()

pool = ConnectionPool(conninfo=os.getenv("POSTGRES_URL"), min_size=1, max_size=5)

# offset seconds check
with pool.connection() as conn, conn.cursor() as cur:
    cur.execute("""
        UPDATE markers m
        SET offset_seconds = GREATEST(
            0,
            EXTRACT(EPOCH FROM (m.utc_timestamp - mt.recording_start_utc))
        )::int
        FROM meetings mt
        WHERE m.meeting_id = mt.id
            AND mt.recording_start_utc IS NOT NULL
            AND m.offset_seconds IS NULL;
    """)
    conn.commit()

    cur.execute("""
        SELECT meeting_id, label, utc_timestamp, offset_seconds
        FROM markers
        WHERE meeting_id = 'MSpjMWMzZjMzMC0zZWNlLTQxOTQtODI4OC1jOGNjNGVlNzRiZWUqMCoqMTk6bWVldGluZ19NRGhoTVRreU5EUXRaVGRpTlMwME9UVm1MVGd3TWprdFlUTXdOVE15WVdSbU1tRTVAdGhyZWFkLnYy'
        ORDER BY utc_timestamp;
    """)

    rows = cur.fetchall()
    for r in rows:
        print(r)

