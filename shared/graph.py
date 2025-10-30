import os
import msal
import requests
import logging
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

def create_subscription(notification_url: str, client_state: str, organizer_id: str, expiration_date: str, resource: str):
    url = f"{GRAPH_ENDPOINT}/subscriptions"
    payload = {
        "changeType": "created",
        "notificationUrl": notification_url,
        "lifecycleNotificationUrl": notification_url,
        "resource": f"communications/{resource}",
        "expirationDateTime": expiration_date,
        "clientState": client_state
    }
    #print("Creating subscription with payload:", payload)
    #print(_token)
    ah = _http().headers.get("Authorization","")
    #logging.info("Auth header starts with: %r", ah[:12]) 
    r = _http().post(url, json=payload, timeout=30)
    #logging.info("Create sub status=%s body=%s", r.status_code, r.text if r.status_code>=400 else "<ok>")
    if r.status_code >= 400:
        logging.error("Graph create_subscription failed: %s\n%s",
                    r.status_code, r.text)
    r.raise_for_status()
    return r.json()

def list_subscriptions():
    url = f"{GRAPH_ENDPOINT}/subscriptions"
    r = _http().get(url, timeout=30)
    r.raise_for_status()
    return r.json().get("value", [])

def reauthorize_subscription(subscription_id: str):
    url = f"{GRAPH_ENDPOINT}/subscriptions/{subscription_id}/reauthorize"
    r = _http().post(url, timeout=30)
    r.raise_for_status()
    return r.json()

def renew_subscription(subscription_id: str, new_expiration_date: str):
    url = f"{GRAPH_ENDPOINT}/subscriptions/{subscription_id}"
    payload = {
        "expirationDateTime": new_expiration_date
    }
    print(type(new_expiration_date))
    print("Renewing subscription with payload:", payload)
    r = _http().patch(url, json=payload, timeout=30)
    if r.status_code >= 400:
        logging.error("Graph renew_subscription failed: %s\n%s",
                    r.status_code, r.text)
    r.raise_for_status()
    return r.json()

def delete_subscription(subscription_id: str):
    url = f"{GRAPH_ENDPOINT}/subscriptions/{subscription_id}"
    r = _http().delete(url, timeout=30)
    r.raise_for_status()
