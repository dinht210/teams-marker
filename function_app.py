import azure.functions as func
import logging
from psycopg_pool import ConnectionPool
import json
import os
import datetime as dt
from shared.auth import validate_bearer


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

pool = ConnectionPool(conninfo=os.getenv("POSTGRES_URL"), min_size=1, max_size=5)

@app.route(route="add_marker", methods=["POST"])
def add_marker(req: func.HttpRequest) -> func.HttpResponse:
    try:
        #user = validate_bearer(req.headers.get("Authorization"))
        #print("Authenticated user:", user)
        req_body = req.get_json()
        meeting_id = req_body.get("meeting_id")
        #meeting_id = req.params.get("meeting_id")
        label = (req_body.get("label") or "").strip()
        dummy_user_id = req_body.get("dummy_user_id") # temporary until user auth is implemented

        if not meeting_id:
            return func.HttpResponse("Missing meeting_id", status_code=400)
        if not dummy_user_id: # temp
            return func.HttpResponse("dummy_user_id is required for now", status_code=400)
        
        utc_timestamp = dt.datetime.now(dt.timezone.utc)

        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute("INSERT INTO meetings (id) VALUES (%s) ON CONFLICT (id) DO NOTHING", (meeting_id,))
            cur.execute("""
                INSERT INTO markers (meeting_id, label, utc_timestamp, user_id)
                VALUES (%s, %s, %s, %s)
                RETURNING id, meeting_id, label, utc_timestamp, user_id
            """, (meeting_id, label, utc_timestamp, dummy_user_id))
            new_marker = cur.fetchone() # fetched row of newly added marker
            conn.commit()
        
        _id, meeting_id, label, utc_timestamp, user_id = new_marker

        resp = {
            "id": str(_id),
            "meeting_id": meeting_id,
            "label": label,
            "utc_timestamp": utc_timestamp.isoformat(),
            "user_id": str(user_id)
        }
        
        return func.HttpResponse(
            json.dumps(resp), status_code=201, mimetype="application/json")
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

@app.route(route="get_markers", methods=["GET"])
def get_markers(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        meeting_id = req_body.get("meeting_id")
        if not meeting_id:
            return func.HttpResponse("Missing meeting_id", status_code=400)
        
        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT id, meeting_id, label, utc_timestamp
                FROM markers
                WHERE meeting_id = %s
                ORDER BY utc_timestamp ASC
            """, (meeting_id,))
            markers = cur.fetchall()

        markers_list = [
            {
                "id": str(row[0]),
                "meeting_id": row[1],
                "label": row[2],
                "utc_timestamp": row[3].isoformat()
            } for row in markers
        ]

        return func.HttpResponse(
            json.dumps(markers_list), status_code=200, mimetype="application/json")

    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

@app.route(route="db_check", methods=["GET"])
def db_check(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('DB check function processing a request.')
    with pool.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM meetings")
        meetings = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM markers")
        markers = cur.fetchone()[0]
        logging.info(f"PostgreSQL version: {version}, meetings count: {meetings}, markers count: {markers}")
    return func.HttpResponse(
        json.dumps({
            "postgres_version": version,
            "meetings_count": meetings,
            "markers_count": markers
        }), status_code=200, mimetype="application/json")

