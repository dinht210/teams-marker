import os
import msal
import requests
from urllib.parse import quote

GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID")
GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
GRAPH_AUTHORITY = f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}"
GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]
GRAPH_ENDPOINT = "https://graph.microsoft.com/v1.0"

_session = None
_token = None

def get_token():
    #add token cache
    global _session
    global _token
    if _token is None:
        msal_client = msal.ConfidentialClientApplication(
            GRAPH_CLIENT_ID,
            authority=GRAPH_AUTHORITY,
            client_credential=GRAPH_CLIENT_SECRET
        )
        result = msal_client.acquire_token_for_client(scopes=GRAPH_SCOPE)
        if "access_token" in result:
            _token = result["access_token"]
        else:
            raise Exception("Could not obtain access token")
    return _token

def _http():
    global _session
    if _session is None:
        # create a requests session once and reuse it
        _session = requests.Session()
    # always ensure Authorization header is current
    _session.headers.update({"Authorization": f"Bearer {get_token()}"})
    return _session

# transcript graph api functions
def list_transcripts(organizer_id: str, online_meeting_id: str):
    url = f"{GRAPH_ENDPOINT}/users/{organizer_id}/onlineMeetings/{online_meeting_id}/transcripts"
    response = _http().get(url)
    response.raise_for_status()
    return response.json().get("value", [])

def get_transcript(organizer_id: str, online_meeting_id: str, transcript_id: str):
    url = f"{GRAPH_ENDPOINT}/users/{organizer_id}/onlineMeetings/{online_meeting_id}/transcripts/{transcript_id}"
    response = _http().get(url, timeout=30)
    response.raise_for_status()
    return response.json()

def get_all_transcripts(organizer_id: str):
    url = f"{GRAPH_ENDPOINT}/users/{organizer_id}/communications/onlineMeetings/getAllTranscripts"
    response = _http().get(url, timeout=30)
    response.raise_for_status()
    return response.json().get("value", [])

def get_transcript_content(organizer_id: str, online_meeting_id: str, transcript_id: str, fmt: str = "vtt"):
    url = f"{GRAPH_ENDPOINT}/users/{organizer_id}/onlineMeetings/{online_meeting_id}/transcripts/{transcript_id}/content"
    params = {"format": fmt} if fmt else None
    response = _http().get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.content, response.headers.get("Content-Type", "text/plain")

# recording graph api functions
def list_recordings(organizer_id: str, online_meeting_id: str):
    url = f"{GRAPH_ENDPOINT}/users/{organizer_id}/onlineMeetings/{online_meeting_id}/recordings"
    response = _http().get(url)
    response.raise_for_status()
    return response.json().get("value", [])

def get_recording(organizer_id: str, online_meeting_id: str, recording_id: str):
    url = f"{GRAPH_ENDPOINT}/users/{organizer_id}/onlineMeetings/{online_meeting_id}/recordings/{recording_id}"
    response = _http().get(url, timeout=30)
    response.raise_for_status()
    return response.json()

def get_all_recordings(organizer_id: str):
    url = f"{GRAPH_ENDPOINT}/users/{organizer_id}/communications/onlineMeetings/getAllRecordings"
    response = _http().get(url, timeout=30)
    response.raise_for_status()
    return response.json().get("value", [])

def get_recording_content(organizer_id: str, online_meeting_id: str, recording_id: str):
    url = f"{GRAPH_ENDPOINT}/users/{organizer_id}/onlineMeetings/{online_meeting_id}/recordings/{recording_id}/content"
    response = _http().get(url, timeout=30)
    response.raise_for_status()
    return response.content, response.headers.get("Content-Type", "text/plain")

def resolve_meeting_by_join_url(join_web_url: str, organizer_id: str):
    flt = f"JoinWebUrl eq '{join_web_url}'"
    flt_quoted = quote(flt, safe="= ':")
    url = f"{GRAPH_ENDPOINT}/users/{organizer_id}/onlineMeetings?$filter={flt_quoted}"
    r = _http().get(url, timeout=30)
    r.raise_for_status()
    items = r.json().get("value", [])
    return items[0]["id"] if items else None

def create_subscription(notiication_url: str, client_state: str, organizer_id: str, expiration_date: str, resource: str):
    url = f"{GRAPH_ENDPOINT}/subscriptions"
    payload = {
        "changeType": "created",
        "notificationUrl": notiication_url,
        "resource": f"users/{organizer_id}/{resource}",
        "expirationDateTime": expiration_date,
        "clientState": client_state
    }
    response = _http().post(url, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()
