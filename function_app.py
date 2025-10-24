import azure.functions as func
import logging
import json
import os
import re
import datetime as dt
import asyncio
from urllib.parse import urlencode
# heavy imports
# from shared.auth import validate_bearer
# from shared import graph
# from azure.servicebus import ServiceBusClient, ServiceBusMessage
# from psycopg_pool import ConnectionPool


app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

#pool = ConnectionPool(conninfo=os.getenv("POSTGRES_URL"), min_size=1, max_size=5)
_pool = None
def get_pool():
    """Create the psycopg pool lazily to avoid blocking module import."""
    global _pool
    if _pool is None:
        from psycopg_pool import ConnectionPool  # import here to avoid startup failures
        conninfo = os.getenv("POSTGRES_URL")
        # Fail fast with a clear error instead of hanging
        if not conninfo:
            raise RuntimeError("POSTGRES_URL is not set")
        _pool = ConnectionPool(
            conninfo=conninfo,
            min_size=1,
            max_size=5,
            # don't attempt to open connections at construction time
            open=False,
            kwargs={"connect_timeout": 5}
        )
    return _pool


@app.function_name(name="ping")
@app.route(route="ping", methods=["GET"])
def ping(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse("ok")

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

        pool = get_pool()
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
        
        pool = get_pool()
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

        pool = get_pool()
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
    pool = get_pool()
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

def parse_ce_resource(resource: str):
    _RX_ITEM_USER = re.compile(
        r"^users\('(?P<org>[^']+)'\)/onlineMeetings\('(?P<mid>[^']+)'\)/(?P<kind>recordings|transcripts)\('(?P<aid>[^']+)'\)$"
    )

    m = _RX_ITEM_USER.match(resource)
    if m:
        return {
            "type": "item",
            "organizer_id": m.group("org"),
            "meeting_id": m.group("mid"),
            "kind": m.group("kind"),
            "artifact_id": m.group("aid"),
        }
    
    return None

@app.service_bus_queue_trigger(arg_name="msg", 
                               queue_name="teams-marker-queue", 
                               connection="SERVICE_BUS_CONNECTION_STRING")
def process_meeting(msg: func.ServiceBusMessage):
    try:
        from shared import graph
        raw = msg.get_body().decode("utf-8")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logging.error("ServiceBus message not JSON: %r", raw[:200])
            return

        if isinstance(payload, dict):
            events = [payload]
        else:
            events = payload

        per_item = []
        per_org = set()

        for ev in events:
            data = ev.get("data", {}) if isinstance(ev, dict) else {}
            resource = data.get("resource") or ev.get("subject")
            if not resource:
                logging.warning("Event without resource: %s", ev); continue

            ev_type = ev.get("type") or ev.get("eventType") or ""

            # filters lifecycle notifications
            if "LifecycleNotification" in ev_type:
                lifecycle = data.get("lifecycleEvent")
                subscription_id = data.get("subscriptionId")
                client_state = data.get("clientState")

                if os.getenv("GRAPH_SUBS_CLIENT_STATE") and client_state != os.getenv("GRAPH_SUBS_CLIENT_STATE"):
                    logging.warning("Mismatched client state secret, skipping")
                    continue
                
                try:
                    if lifecycle == "reauthorizationRequired":
                        # graph.reauthorize_subscription(subscription_id)
                        new_exp_date = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=23)).replace(microsecond=0).isoformat() + "Z"
                        graph.renew_subscription(subscription_id, new_exp_date)

                    elif lifecycle == "subscriptionRemoved":
                        organizer_id = os.getenv("ORGANIZER_ID")
                        if organizer_id:
                            recreate_subsriptions(organizer_id)
                        else:
                            logging.warning("ORGANIZER_ID not set, cannot recreate subscriptions")

                    elif lifecycle == "missed":
                        subs = graph.list_subscriptions()
                        for sub in subs:
                            exp_time_str = sub.get("expirationDateTime")
                            sub_id = sub.get("id")
                            if not exp_time_str or not sub_id:
                                continue
                            exp_time_dt = dt.datetime.fromisoformat(exp_time_str.replace("Z", "+00:00"))
                            time_diff = exp_time_dt - dt.datetime.now(dt.timezone.utc)
                            # if expiring within an hour and we get a missed notification, renew
                            if time_diff < dt.timedelta(hours=1):
                                new_exp_date = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=23)).replace(microsecond=0).isoformat() + "Z"
                                graph.renew_subscription(sub_id, new_exp_date)
                    
                    else:
                        logging.info("Unhandled lifecycle event: %s", lifecycle)
                except Exception:
                    logging.exception("Error handling lifecycle event: %s", lifecycle)

                return  # lifecycle events don't need further processing

            parsed = parse_ce_resource(resource)
            if not parsed:
                logging.warning("Unrecognized resource: %s", resource); continue

            if parsed["type"] == "agg":
                per_org.add(parsed["organizer_id"])
            elif parsed["type"] == "item":
                per_item.append((parsed["organizer_id"], parsed["meeting_id"], parsed["kind"]))
                    
            logging.info("EventGrid event: kind=%s organizer=%s meeting=%s", parsed.get("kind"), parsed.get("organizer_id"), parsed.get("meeting_id"))

        # process per-item first (cheapest, already gives meeting_id)
        if per_item:
            pool = get_pool()
            with pool.connection() as conn, conn.cursor() as cur:
                for organizer_id, meeting_id, kind in per_item:
                    recs = graph.list_recordings(organizer_id, meeting_id) if kind == "recordings" else []
                    start = recs[0].get("createdDateTime") if recs else None
                    base  = f"/users/{organizer_id}/onlineMeetings/{meeting_id}"
                    cur.execute("INSERT INTO meetings (id) VALUES (%s) ON CONFLICT (id) DO NOTHING", (meeting_id,))
                    cur.execute("""
                        UPDATE meetings
                        SET artifacts_ready = TRUE,
                            recording_start_utc = %s,
                            recording_base_url  = %s,
                            updated_at          = now()
                        WHERE id = %s
                    """, (start, base, meeting_id))
                conn.commit()

        # for organizers that sent aggregator events, discover which meetings to upsert
        for organizer_id in per_org:
            touched = set()
            try:
                for r in graph.get_all_recordings(organizer_id):
                    meeting_id = r.get("meetingId")
                    touched.add(meeting_id) if meeting_id else None
            except Exception:
                logging.exception("getAllRecordings failed org=%s", organizer_id)
            try:
                for t in graph.get_all_transcripts(organizer_id):
                    meeting_id = t.get("meetingId")
                    touched.add(meeting_id) if meeting_id else None
            except Exception:
                logging.exception("getAllTranscripts failed org=%s", organizer_id)

            if not touched:
                continue

            with pool.connection() as conn, conn.cursor() as cur:
                for meeting_id in touched:
                    try:
                        recs = graph.list_recordings(organizer_id, meeting_id)
                    except Exception:
                        logging.exception("list_recordings failed org=%s mid=%s", organizer_id, meeting_id)
                        recs = []
                    start = recs[0].get("createdDateTime") if recs else None
                    base  = f"/users/{organizer_id}/onlineMeetings/{meeting_id}"
                    logging.info("Upserting meeting=%s start=%s base=%s", meeting_id, start, base)
                    cur.execute("INSERT INTO meetings (id) VALUES (%s) ON CONFLICT (id) DO NOTHING", (meeting_id,))
                    cur.execute("""
                        UPDATE meetings
                        SET artifacts_ready = TRUE,
                            recording_start_utc = %s,
                            recording_base_url  = %s,
                            updated_at          = now()
                        WHERE id = %s
                    """, (start, base, meeting_id))
                conn.commit()

        logging.info("process_meeting: items=%d organizers=%d", len(per_item), len(per_org))

    except Exception:
        logging.exception("process_meeting failed")
        raise 

#smoke testing graph functions
@app.route(route="debug_fetch_artifacts", methods=["POST"])
def debug_fetch_artifacts(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from shared import graph
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
    
def enqueue_sb(payload: dict):
    from azure.servicebus import ServiceBusClient, ServiceBusMessage
    conn = os.getenv("SERVICE_BUS_CONNECTION_STRING")
    QUEUE_NAME = "teams-marker-queue"

    with ServiceBusClient.from_connection_string(conn) as client:
        with client.get_queue_sender(queue_name=QUEUE_NAME) as sender:
            message = ServiceBusMessage(json.dumps(payload))
            sender.send_messages(message)

#webhook endpoint (notification url)
@app.route(route="graph_notifications", methods=["POST"])
def graph_notifications(req: func.HttpRequest) -> func.HttpResponse:
    token = req.body.get("validationToken")

    if token:
        return func.HttpResponse(status_code=200, mimetype="text/plain")
    
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=202)
    
    client_state_secret = os.getenv("GRAPH_SUBS_CLIENT_STATE")
    for i in req_body.get("value", []):
        if client_state_secret and client_state_secret != i.get("clientState"):
            logging.warning("Mismatched client state secret, skipping")
            continue

        resource = i.get("resource", "")
        parts = resource.split("/")
        organizer_id = parts[2] if len(parts) >= 3 and parts[0] == "users" else None

        if not organizer_id:
            logging.warning("Unrecognized resource: %s", resource)
            continue

        payload = {
            "organizer_id": organizer_id
        }

        enqueue_sb(payload)
        logging.info("Webhook: enqueued organizer=%s", organizer_id)

    return func.HttpResponse(status_code=202)

def create_eventgrid_uri():
    q = urlencode({
        "azureSubscriptionId": os.environ["EG_SUBSCRIPTION_ID"],
        "resourceGroup": os.environ["EG_RESOURCE_GROUP"],
        "partnerTopic": os.environ["EG_PARTNER_TOPIC"],
        "location": os.environ["EG_LOCATION"]
    })
    return f"EventGrid:?{q}"

@app.route("create_subscriptions", methods=["POST"])
def create_subscriptions(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from shared import graph
        event_grid_notif_url = create_eventgrid_uri()
        exp_dt = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=23)
        exp_date = exp_dt.replace(microsecond=0).isoformat() + "Z"
        client_state = os.getenv("GRAPH_SUBS_CLIENT_STATE")
        organizer_id = req.get_json().get("organizer_id")
        #resources = [f"users/{organizer_id}/onlineMeetings/getAllRecordings", f"users/{organizer_id}/onlineMeetings/getAllTranscripts"]
        resources = ["onlineMeetings/getAllRecordings", "onlineMeetings/getAllTranscripts"]

        created = []
        for r in resources:
            sub = graph.create_subscription(notification_url=event_grid_notif_url,
                                            client_state=client_state,
                                            organizer_id=organizer_id,
                                            expiration_date=exp_date,
                                            resource=r)
            logging.info("Created subscription: %s", sub)
            created.append(sub)

        return func.HttpResponse(json.dumps({"created": created}), status_code=201, mimetype="application/json")
    except Exception as e:
        logging.exception("Error creating subscriptions")
        return func.HttpResponse(f"Exception occured: {e}", status_code=400)
    
def recreate_subsriptions(organizer_id: str):
    try:
        from shared import graph
        event_grid_notif_url = create_eventgrid_uri()
        exp_dt = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=23)
        exp_date = exp_dt.replace(microsecond=0).isoformat() + "Z"
        client_state = os.getenv("GRAPH_SUBS_CLIENT_STATE")
        resources = ["onlineMeetings/getAllRecordings", "onlineMeetings/getAllTransripts"]

        for r in resources:
            sub = graph.create_subscription(notification_url=event_grid_notif_url,
                                            client_state=client_state,
                                            organizer_id=organizer_id,
                                            expiration_date=exp_date,
                                            resource=r)
            logging.info("Created subscription: %s", sub)
    except Exception as e:
        logging.exception("Error recreating subscriptions for organizer %s", organizer_id)


# @app.route("create_subscriptions", methods=["POST"]) 
# def create_subscriptions(req: func.HttpRequest) -> func.HttpResponse:
#     try:
#         req_body = req.get_json()
#         organizer_id = req_body.get("organizer_id")
#         notification_url = os.getenv["GRAPH_NOTIF_URL"]
#         expiration = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=55)).replace(microsecond=0).isoformat() + "Z"
#         resources = ["/onlineMeetings/getAllRecordings", "onlineMeetings/getAllTranscripts"]
#         client_state = os.getenv["GRAPH_SUBS_CLIENT_STATE"]

#         for r in resources:
#             # body = {
#             #     "changeType": "created",
#             #     "notificationUrl": notification_url,
#             #     "resource": r,
#             #     "expirationDateTime": expiration,
#             #     "clientState": client_state
#             # }
#             sub = graph.create_subscription(
#                 notiication_url=notification_url,
#                 client_state=client_state,
#                 organizer_id=organizer_id,
#                 expiration_date=expiration,
#                 resource=r
#             )
#             logging.info(f"Created subscription: {sub}")
#     except Exception as e:
#         return func.HttpResponse(f"Exception occured: {e}", status_code=400)

# schedule param takes ncrontab expression
# @app.timer_trigger(
#         schedule="0 */6 * * * *", 
#         arg_name="mytimer",
#         use_monitor=True
#     )
# def renew_subscriptions(mytimer: func.TimerRequest) -> None:
#     try:
#         from shared import graph
#         s = graph._http()
#         new_exp_date = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=23)).replace(microsecond=0).isoformat() + "Z"
#         subs = s.get(f"{graph.GRAPH_ENDPOINT}/subscriptions")
#         subs.raise_for_status()
#         event_grid_notif_url = create_eventgrid_uri()
#         for sub in subs.json().get("value", []):
#             if sub.get("notificationUrl") != event_grid_notif_url:
#                 continue
#             if os.getenv("GRAPH_SUBS_CLIENT_STATE") and sub.get("clientState") != os.getenv("GRAPH_SUBS_CLIENT_STATE"):
#                 continue

#             sub_id = sub.get("id")
#             patch_url = f"{graph.GRAPH_ENDPOINT}/subscriptions/{sub_id}"
#             resp = s.patch(patch_url, json={"expirationDateTime": new_exp_date}, timeout=30)
#             resp.raise_for_status()

#             logging.info(f"Renewed subscription {sub_id} to {new_exp_date}")
#     except Exception as e:
#         logging.error(f"Cannot get HTTP session: {e}")
#         return

@app.route(route="list_subscriptions", methods=["GET"])
def list_subscriptions(req: func.HttpRequest) -> func.HttpResponse:
    try:
        from shared import graph
        subs = graph.list_subscriptions()
        return func.HttpResponse(json.dumps({"subscriptions": subs}), status_code=200, mimetype="application/json")
    except Exception as e:
        logging.error(f"Cannot get HTTP session: {e}")
        return func.HttpResponse(f"Cannot list subscriptions: {e}", status_code=500)
    
