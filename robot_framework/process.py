from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement

from datetime import datetime
import json
import requests
from functools import lru_cache
from math import radians, cos, sin, asin, sqrt
import regex
from azure.cosmos import CosmosClient

CVR_API_URL = "https://cvrapi.dk/api"
USER_AGENT = "Henstillinger AAK"
DEPOT = (56.161147, 10.13455)
# --- Overtrædelser der må faktureres ---
ALLOWED_NUMRE = {
    "1B.", "2B.", "3B.", "4B.", "5B.", "7B.",
    "8B.", "9B.", "10B.", "12B.", "19B.", "23B."
}

FAKTURALINJE_MAP = {
    "10B.": "751_Materiel pr. kvadratmeter",
    "12B.": "751_Materiel pr. kvadratmeter",
    "19B.": "751_Byggeplads pr.kvadratmeter",
    "1B.":  "751_Afmærkning pr.kvadratmeter",
    "23B.": "751_Bygninger pr. kvadratmeter",
    "2B.":  "751_Afmærkning pr.kvadratmeter",
    "3B.":  "751_Materiel pr. kvadratmeter",
    "4B.":  "751_Lift pr. kvadratmeter",
    "5B.":  "751_Kran pr. kvadratmeter",
    "7B.":  "751_Skurvogn pr. kvadratmeter",
    "8B.":  "751_Container pr. kvadratmeter",
    "9B.":  "751_Stillads pr. kvadratmeter",
}

def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")

    # --- Credentials / constants ---
    cred = orchestrator_connection.get_credential("PEZUI")
    USERNAME = cred.username
    PASSWORD = cred.password

    cosmos_credentials = orchestrator_connection.get_credential("AAKTilsynDB")
    COSMOS_URL = cosmos_credentials.username
    COSMOS_KEY = cosmos_credentials.password

    DB_NAME = "aak-tilsyn"
    CONTAINER = "henstillinger"

    client = CosmosClient(COSMOS_URL, COSMOS_KEY)
    container = client.get_database_client(DB_NAME).get_container_client(CONTAINER)

    # --- Requests session (must be reused throughout) ---
    session = requests.Session()
    session.headers.update({
        "accept-language": "en-US,en;q=0.9,en-AU;q=0.8,en-CA;q=0.7,en-IN;q=0.6,en-IE;q=0.5,en-NZ;q=0.4,en-GB-oxendict;q=0.3,en-GB;q=0.2,en-ZA;q=0.1",
        "sec-ch-ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0",
    })

    # 1) Load login page
    resp = session.get(
        "https://pez.giantleap.net/login",
        headers={"accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
        timeout=30,
    )
    resp.raise_for_status()

    # 2) initiate-login (JSON)
    resp = session.post(
        "https://pez.giantleap.net/rest/public/initiate-login",
        json={"username": USERNAME},
        headers={
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://pez.giantleap.net",
            "referer": "https://pez.giantleap.net/login",
        },
        timeout=30,
    )
    resp.raise_for_status()

    # 3) oauth token (x-www-form-urlencoded) — pass dict so requests encodes special chars safely
    resp = session.post(
        "https://pez.giantleap.net/rest/oauth/token",
        data={
            "client_id": "web-client",
            "grant_type": "password",
            "username": USERNAME,
            "password": PASSWORD,
        },
        headers={
            "accept": "application/json, text/plain, */*",
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://pez.giantleap.net",
            "referer": "https://pez.giantleap.net/login",
            "authorization": "Basic d2ViLWNsaWVudDp3ZWItY2xpZW50",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    access_token = data["access_token"]

    # --- Fetch list of cases (paged) ---
    base_url = "https://pez.giantleap.net/rest/tickets/cases"

    params = {
        "case_type": "PARKING_TICKET",
        "contract": "35e4ac45-f820-48a5-b156-ae24b76e4ae4",
        "current_step": "2a8e5c34-ed88-4c9c-9d3b-a6af1013b3c1",
        "limit": 50,
        "offset": 0,
        "q": "",
        "sort_column": "date",
        "sort_direction": "DESC",
    }

    headers_list = {
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {access_token}",
        "referer": "https://pez.giantleap.net/cases",
        "x-bpid": "bp_aarhus",
        "x-gltlocale": "da",
    }

    all_results = []
    while True:
        response = session.get(base_url, headers=headers_list, params=params, timeout=60)
        response.raise_for_status()
        j = response.json()

        all_results.extend(j.get("results", []))
        if not j.get("hasMore"):
            break

        params["offset"] += params["limit"]

    orchestrator_connection.log_info(f"Fetched {len(all_results)} total cases")

    # --- Per-case processing ---
    headers_detail = {
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {access_token}",
        "priority": "u=1, i",
        "x-bpid": "bp_aarhus",
        "x-gltlocale": "da",
    }

    processed = 0
    skipped = 0

    for case in all_results:
        case_id = case.get("id")
        if not case_id:
            skipped += 1
            continue

        # Case detail
        url = f"https://pez.giantleap.net/rest/tickets/cases/{case_id}"
        response = session.get(url, headers=headers_detail, timeout=60)
        if response.status_code != 200:
            skipped += 1
            continue
        cjson = response.json().get("result") or {}

        henstilling_id = cjson.get("number")  # matches prior "Nummer"
        if not henstilling_id:
            skipped += 1
            continue

        ticket = cjson.get("parkingTicket") or {}

        streetname = (ticket.get("streetLocation") or {}).get("streetName")
        housenumber = (ticket.get("streetLocation") or {}).get("houseNumber")
        locationby = ticket.get("locationBy")

        if not streetname:
            orchestrator_connection.log_info(f"Missing streetName for {henstilling_id}, skipping entry.")
            skipped += 1
            continue

        address_short = streetname
        if housenumber:
            address_short = f"{address_short} {housenumber}"

        address_text = address_short
        if locationby:
            address_text = f"{address_text} - {locationby}"

        latitude = (ticket.get("coordinates") or {}).get("latitude")
        longitude = (ticket.get("coordinates") or {}).get("longitude")

        from_time_raw = ticket.get("fromTime")
        to_time_raw = ticket.get("toTime")
        if from_time_raw:
            startdato = datetime.strptime(from_time_raw, "%Y-%m-%d %H:%M:%S").date()
        if to_time_raw:
            slutdato = datetime.strptime(to_time_raw, "%Y-%m-%d %H:%M:%S").date()

        # Violations (max 3, sometimes fewer)
        violations_raw = {
            1: ticket.get("violation1Name"),
            2: ticket.get("violation2Name"),
            3: ticket.get("violation3Name"),
        }

        forseelser = []

        for nummer, v in violations_raw.items():
            if not v or not isinstance(v, str):
                continue

            tokens = v.strip().split(maxsplit=1)
            if not tokens:
                continue

            first = tokens[0]  # e.g. "9B."

            if first in ALLOWED_NUMRE:
                tilladelsestype = FAKTURALINJE_MAP.get(first)

                forseelser.append({
                    "nummer": nummer,  # now comes directly from violation index
                    "text": v.strip(),
                    "tilladelsestype": tilladelsestype
                })

        if len(forseelser) == 0:
            skipped += 1
            continue

        # Vehicle owners (company + CVR)
        url = f"https://pez.giantleap.net/rest/tickets/cases/{case_id}/vehicle-owners"
        response = session.get(url, headers=headers_detail, timeout=60)
        if response.status_code != 200:
            skipped += 1
            continue
        vjson = response.json().get("result") or {}

        category = vjson.get("category")
        cvr_raw = vjson.get("identificationNumber")



        if not cvr_raw or not isinstance(cvr_raw, str):
            # orchestrator_connection.log_info(f"Missing CVR for {henstilling_id}, skipping entry.")
            skipped += 1
            continue

        cvr = cvr_raw.strip()
        if not is_valid_cvr(cvr):
            # orchestrator_connection.log_info(f"Invalid CVR '{cvr_raw}' for {henstilling_id}, skipping entry.")
            skipped += 1
            continue

        # Coordinates too close + geocode fallback
        if latitude and longitude:
            try:
                latitude = float(latitude)
                longitude = float(longitude)
            except Exception:
                latitude = None
                longitude = None

        if latitude and longitude:
            latitude, longitude = replace_coord_if_too_close(address_short, latitude, longitude, 100)

        if not latitude or not longitude:
            geocoded = geocode_address(address_short)
            if geocoded:
                latitude, longitude = geocoded

        # Company name
        company_name = vjson.get("name")
        firmanavn = company_name or get_firmanavn_cached(container, cvr) or "Ugyldigt CVR"

        meta = {
            "cvr": cvr,
            "firmanavn": firmanavn,
            "startdato": startdato,
            "slutdato": slutdato,
            "adresse": address_text,
            "latitude": latitude,
            "longitude": longitude
        }

        sync_henstilling(container, henstilling_id, forseelser, meta, case_id, session, access_token)

        processed += 1

    orchestrator_connection.log_info(f"Done. Processed={processed}, Skipped={skipped}")


def haversine(coord1, coord2):
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    R = 6371000
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return R * c


def geocode_address(address: str) -> tuple | None:
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": f"{address}, Aarhus, Denmark",
        "format": "json",
        "limit": 1,
    }
    headers = {"User-Agent": "AarhusRoutePlanner/1.0 (aarhuskommune.dk)"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=5)
        r.raise_for_status()
        data = r.json()
        if data:
            return (float(data[0]["lat"]), float(data[0]["lon"]))
    except Exception as e:
        print(f"Geocoding failed for '{address}': {e}")
    return None


def clean_address(address: str) -> str:
    address = regex.sub(r"(\d+[A-Za-z]?)-\d+[A-Za-z]?", r"\1", address.strip())
    match = regex.search(r"([\p{L} .\-]+?)\s+(\d+[A-Za-z]?)", address)
    if match:
        street, number = match.groups()
        return f"{street.strip()} {number.strip()}"
    return None


def replace_coord_if_too_close(address, latitude, longitude, threshold_m=100) -> dict:
    coord = (latitude, longitude)
    distance = haversine(coord, DEPOT)
    geocode_coord = None
    if distance > threshold_m:
        return latitude, longitude
    cleaned_address = clean_address(address)
    if cleaned_address:
        geocode_coord = geocode_address(cleaned_address)
    if geocode_coord:
        new_coord = geocode_coord
        latitude = new_coord[0]
        longitude = new_coord[1]
    return latitude, longitude



@lru_cache(maxsize=5000)
def get_firmanavn_cached(container, cvr: int):
    # Check Cosmos cache
    query = """
        SELECT TOP 1 c.FirmaNavn
        FROM c
        WHERE c.CVR = @cvr
        AND IS_DEFINED(c.FirmaNavn)
        AND IS_STRING(c.FirmaNavn)
        AND LENGTH(TRIM(c.FirmaNavn)) > 0
    """
    results = list(container.query_items(
        query=query,
        parameters=[{"name": "@cvr", "value": int(cvr)}],
        enable_cross_partition_query=True
    ))
    if results and results[0].get("FirmaNavn"):
        return results[0]["FirmaNavn"]

    # Fallback to CVR API
    try:
        r = requests.get(
            CVR_API_URL,
            params={"country": "dk", "search": cvr},
            headers={"User-Agent": USER_AGENT},
            timeout=5
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("name")
    except Exception as e:
        print(f"CVR lookup failed for {cvr}: {e}")
    return None


def sync_henstilling(container, henstilling_id, forseelser, meta, case_uuid, session, access_token):
    """
    Sync henstilling data in Cosmos DB:
    - Only inserts or updates rows that are 'Ny'
    - Never deletes any rows (preserves locked statuses)
    - Efficiently upserts items using partition keys
    """

    existing_docs = list(container.query_items(
        query="SELECT * FROM c WHERE c.HenstillingId = @h",
        parameters=[{"name": "@h", "value": henstilling_id}],
        enable_cross_partition_query=True
    ))

    existing_map = {d["id"]: d for d in existing_docs}

    for f in forseelser:
        fid = f"{henstilling_id}_{f['nummer']}"
        existing = existing_map.get(fid)

        # Skip locked statuses
        if existing and existing.get("FakturaStatus") not in ("Ny"):
            continue

        item = {
            "id": fid,
            "HenstillingId": henstilling_id,
            "PEZUUID": case_uuid,
            "ForseelseNr": f["nummer"],
            "Forseelse": f["text"],
            "CVR": meta.get("cvr"),
            "FirmaNavn": meta.get("firmanavn"),
            "Adresse": meta.get("adresse"),
            "Latitude": meta.get("latitude"),
            "Longitude": meta.get("longitude"),
            "Startdato": str(meta.get("startdato")),
            "Slutdato": existing.get("Slutdato") if existing else None,
            "Kvadratmeter": existing.get("Kvadratmeter") if existing else None,
            "Tilladelsestype": (
                existing.get("Tilladelsestype")
                if existing and existing.get("Tilladelsestype") is not None
                else f.get("tilladelsestype")
            ),
            "FakturaStatus": "Ny",
        }

        container.upsert_item(body=item)
    
    if len(existing_docs) == 0:
        add_sent_to_tilsyn_comment(session, access_token, case_uuid, forseelser)



def is_valid_cvr(cvr_str: str) -> bool:
    """Return True if CVR has 8 digits and passes modulus-11 validation."""
    if len(cvr_str) != 8 or not cvr_str.isdigit():
        return False

    weights = [2, 7, 6, 5, 4, 3, 2, 1]
    total = sum(int(d) * w for d, w in zip(cvr_str, weights))

    return total % 11 == 0


def add_sent_to_tilsyn_comment(session: requests.Session, access_token: str, case_uuid: str, forseelser: list[dict]) -> None:
    url = f"https://pez.giantleap.net/rest/tickets/cases/{case_uuid}/comments"

    summary = format_afvigelser_summary(forseelser)
    payload = {
        "comment": f"Sendt til AAK Tilsyn -> {summary}",
        "isInternal": True,
    }

    headers = {
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {access_token}",
        "content-type": "application/json;charset=UTF-8",
        "priority": "u=1, i",
        "x-bpid": "bp_aarhus",
        "x-gltlocale": "da",
    }

    r = session.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()


def format_afvigelser_summary(forseelser: list[dict]) -> str:
    """
    Output example:
    Afvigelse 1: 4B. Lifte ol. opstillet uden tilladelse;
    Afvigelse 2: 10B. Byggematerialer/affald placeret uden tilladelse.
    (but in one single line)
    """
    parts = []

    for f in sorted(forseelser, key=lambda x: x.get("nummer", 0)):
        nummer = f.get("nummer")
        text = (f.get("text") or "").strip()

        # Ensure text ends with a period for consistency
        if text and not text.endswith("."):
            text = text + "."

        parts.append(f"Afvigelse {nummer}: {text}")

    return " | ".join(parts)