import azure.functions as func
import logging
from psycopg_pool import ConnectionPool
import json
import os
import datetime as dt
from shared.auth import validate_bearer
from shared import graph
import asyncio


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

@app.route(route="get_meetings", methods=["GET"])
def get_meetings(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        meeting_id = req_body.get("meeting_id")
        if not meeting_id:
            return func.HttpResponse("Missing meeting_id", status_code=400)

        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT id, artifacts_ready, recording_start_utc, recording_base_url
                FROM meetings
                WHERE id = %s
                ORDER BY updated_at ASC
            """, (meeting_id,))
            meetings = cur.fetchall()
        
        meetings_list = [
            {
                "id": str(row[0]),
                "artifacts_ready": row[1],
                "recording_start_utc": row[2].isoformat(),
                "recording_base_url": row[3]
            } for row in meetings
        ]

        return func.HttpResponse(
            json.dumps(meetings_list), status_code=200, mimetype="application/json")
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

@app.service_bus_queue_trigger(arg_name="msg", 
                               queue_name="teams-marker-queue", 
                               connection="SERVICE_BUS_CONNECTION_STRING")
def process_meeting(msg: func.ServiceBusMessage):
    # logging.info('Service Bus queue trigger function processed a message.')
    # logging.info(f'Message ID: {msg.message_id}')
    # logging.info(f'Message Body: {msg.get_body().decode("utf-8")}')
    print("ASDOIJQWOIEJIOQWJDIODMWQD")
    try:
        msg_body = json.loads(msg.get_body().decode("utf-8"))
        organizer_id = msg_body.get("organizer_id")
        meeting_id = msg_body.get("online_meeting_id")
        join_url = msg_body.get("join_url")
        #transcript_id = req_body.get("transcript_id")
        #recording_id = req_body.get("recording_id")

        if organizer_id is None:
            return func.HttpResponse("No Organizer ID", status_code=400)
        if meeting_id is None:
            meeting_id = graph.resolve_meeting_by_join_url(join_web_url=join_url, organizer_id=organizer_id)
            if meeting_id is None:
                return func.HttpResponse("Cannot resolve meeting by join URL", status_code=400)

        #transcripts = graph.list_transcripts(organizer_id=organizer_id, online_meeting_id=meeting_id)
        recordings = graph.list_recordings(organizer_id=organizer_id, online_meeting_id=meeting_id)

        base_url = f"/users/{organizer_id}/onlineMeetings/{meeting_id}"
        start_time_utc = recordings[0]["createdDateTime"]

        #   create table if not exists meetings (
        #   id text primary key,                         
        #   artifacts_ready boolean default false,
        #   recording_start_utc timestamptz,        
        #   recording_base_url text,                 
        #   updated_at timestamptz default now()
        # );

        with pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO meetings (id) VALUES (%s) ON CONFLICT (id) DO NOTHING",
                (meeting_id,)
            )
            cur.execute("""
                UPDATE meetings
                SET artifacts_ready = TRUE,
                    recording_start_utc = %s,
                    recording_base_url = %s,
                    updated_at = now()
                WHERE id = %s
            """, (start_time_utc if recordings else None, base_url, meeting_id))
            conn.commit()
            print("DONEOISNDAOS")
    except Exception as e:
        return func.HttpResponse(f"Error = {e}", status_code=500)

#smoke testing graph functions
@app.route(route="debug_fetch_artifacts", methods=["POST"])
def debug_fetch_artifacts(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        organizer_id = req_body.get("organizer_id")
        meeting_id = req_body.get("online_meeting_id")
        join_url = req_body.get("join_url")
        #transcript_id = req_body.get("transcript_id")
        #recording_id = req_body.get("recording_id")

        if organizer_id is None:
            return func.HttpResponse("No Organizer ID", status_code=400)
        if meeting_id is None:
            meeting_id = graph.resolve_meeting_by_join_url(join_web_url=join_url, organizer_id=organizer_id)
            if meeting_id is None:
                return func.HttpResponse("Cannot resolve meeting by join URL", status_code=400)

        transcripts = graph.list_transcripts(organizer_id=organizer_id, online_meeting_id=meeting_id)
        recordings = graph.list_recordings(organizer_id=organizer_id, online_meeting_id=meeting_id)

        response = {
            "online_meeting_id": meeting_id,
            "transcripts_count": len(transcripts),
            "recordings_count": len(recordings),
            "transcripts": [
                {k: t.get(k) for k in ("id", "createdDateTime", "lastModifiedDateTime")}
                for t in transcripts
            ],
            "recordings": [
                {k: r.get(k) for k in ("id", "createdDateTime", "contentCorrelationId")}
                for r in recordings
            ]
        }

        return func.HttpResponse(
            json.dumps(response), status_code=200, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error = {e}", status_code=500)

