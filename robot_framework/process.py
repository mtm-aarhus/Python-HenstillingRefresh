"""This module contains the main process of the robot."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import time
import re
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
TILLADELSESTYPE_MAP = {
    "9B.": "Henstilling Stillads m2",
    "19B.": "Henstilling Byggeplads m2",
    "23B.": "Henstilling Bygninger m2",
    "8B.": "Henstilling Container m2",
    "5B.": "Henstilling Kran m2",
    "4B.": "Henstilling Lift m2",
    "12B.": "Henstilling Materiel m2",
    "7B.": "Henstilling Skurvogn m2",
    "1B.": "Henstilling Afmærkning m2",
    "10B.": "Henstilling Materiel m2"
}

def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")
    Credentials = orchestrator_connection.get_credential("Mobility_Workspace")
    username = Credentials.username
    password = Credentials.password
    url = orchestrator_connection.get_constant("MobilityWorkspaceURL").value
    cosmos_credentials = orchestrator_connection.get_credential("AAKTilsynDB")
    COSMOS_URL = cosmos_credentials.username
    COSMOS_KEY = cosmos_credentials.password
    
    DB_NAME = "aak-tilsyn"
    CONTAINER = "henstillinger"

    client = CosmosClient(COSMOS_URL, COSMOS_KEY)
    container = client.get_database_client(DB_NAME).get_container_client(CONTAINER)
    all_results = []
    # Azure SQL connection string

        
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument('--remote-debugging-pipe')
    options.add_experimental_option("prefs", {
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 20)

    driver.get(f"{url}/login")

    try:
        wait.until(EC.presence_of_element_located((By.ID, "j_username"))).send_keys(username)
        driver.find_element(By.NAME, "j_password").send_keys(password)
        driver.find_element(By.NAME, "submit").click()
    except Exception:
        pass  # Login might not be required

    # --- Navigate to the correct page ---
    driver.get(f"{url}/parking/tab/1.2")

        # Reset first to '- Vælg -'
    select_predefined_filter(driver, wait, "")

    # Then select your actual one
    select_predefined_filter(driver, wait, "5aac770d-73ad-40ca-9493-42cfc7f2b1c3")

    # (optional) wait for the table or 'Søg' button to appear again
    wait.until(EC.presence_of_element_located((By.XPATH, "//input[@value='Søg']")))

    # Click "Søg"
    search_button = driver.find_element(By.XPATH, "//input[@type='submit' and @value='Søg']")
    search_button.click()
    
    # --- After clicking "Søg" ---
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.tabell.radlink")))

    prev_btn = driver.find_element(
                By.XPATH, "//button[contains(@title, 'Go to previous page')]"
            )
    if not prev_btn.get_attribute("disabled"):
        first_page_link = driver.find_element(
            By.XPATH, "//a[@title='Go to page 1']"
        )
        driver.execute_script("arguments[0].click();", first_page_link)

        # Wait for the first page to become active (no href, title='Go to page 1')
        wait.until(EC.presence_of_element_located((
            By.XPATH, "//span[@title='Go to page 1'][not(ancestor::a)]"
        )))
        
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.tabell.radlink")))
        
        
    all_results = []
    page_number = 1
    while True:
        all_results.extend(process_page(driver, wait, container, orchestrator_connection, all_results))

        # Try clicking "Næste" if it exists
        next_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Næste')]")))
        next_btn.click()
        page_number += 1
        page_xpath = f"//span[@title='Go to page {page_number}'][not(ancestor::a)]"

        wait.until(EC.presence_of_element_located((By.XPATH, page_xpath)))
        wait.until(EC.presence_of_element_located(
        (By.XPATH, "//th[a[normalize-space(text())='Overtrædelse 1']]")
        ))

        # Return rows of that table
        table = driver.find_element(
            By.XPATH, "//table[.//th[a[normalize-space(text())='Overtrædelse 1']]]"
        )
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr.tabellrad")
        if len(rows) == 0:
            break
        if page_number > 50:
            break

    driver.quit()
    orchestrator_connection.log_info("All pages processed and saved.")



def extract_latlon_from_maplink(driver):
    """Find Google Maps link and extract lat/lon if present."""
    try:
        link = driver.find_element(By.XPATH, "//a[contains(@href, 'maps.google.com/maps?ll=')]").get_attribute("href")
        match = re.search(r"ll=([-+]?\d*\.\d+),([-+]?\d*\.\d+)", link)
        if match:
            return float(match.group(1)), float(match.group(2))
    except Exception:
        pass
    return None, None

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
    headers = {
        "User-Agent": "AarhusRoutePlanner/1.0 (aarhuskommune.dk)"
    }
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

def process_page(driver, wait, container, orchestrator_connection, all_results):
    """Iterate rows, open details, extract widgets, update/insert DB."""
    rows = driver.find_elements(By.CSS_SELECTOR, "table.tabell.radlink tbody tr.tabellrad")
    total_rows = len(rows)
    orchestrator_connection.log_info(f"Found {total_rows} rows on this page.")

    for idx in range(total_rows):
        # Wait for the header to appear (ensures correct table)
        wait.until(EC.presence_of_element_located(
            (By.XPATH, "//th[a[normalize-space(text())='Overtrædelse 1']]")
        ))

        # Once header is present, safely get all rows under that table
        table = driver.find_element(
            By.XPATH, "//table[.//th[a[normalize-space(text())='Overtrædelse 1']]]"
        )
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr.tabellrad")
        row = rows[idx]
        driver.execute_script("arguments[0].scrollIntoView(true);", row)
        row.click()
        back_link = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.tilbakelink")))

        # --- Extract all widgets ---
        widgets = extract_all_widgets(driver)
        
        overtrædelse_info = widgets.get("Overtrædelse", {})
        ejerinfo = widgets.get("Ejerinfo", {})
        kontrol_info = widgets.get("Kontrolgebyrsinformation", {})
        
        henstilling_id = kontrol_info.get("Nummer")
        
        if not henstilling_id:
            orchestrator_connection.log_info("Missing Henstilling ID, skipping entry.")
            back_link.click()
            continue

        
        forseelser = []

        for k, v in (overtrædelse_info or {}).items():
            if not v or not isinstance(v, str) or not k.lower().startswith("overtrædelse"):
                continue

            tokens = v.strip().split(maxsplit=1)
            first = tokens[0]  # e.g. "9B."

            if first in ALLOWED_NUMRE:
                nummer = int(k.split()[-1])
                tilladelsestype = TILLADELSESTYPE_MAP.get(first)
                forseelser.append({
                    "nummer": nummer,
                    "text": v.strip(),
                    "tilladelsestype": tilladelsestype
                })

        if len(forseelser) == 0:
            back_link.click()
            continue
        
        cvr = None
        type_value = ejerinfo.get("Type")

        if type_value != "Organisationsnr.":
            orchestrator_connection.log_info(f"Not a company (Type={type_value}) for {henstilling_id}, skipping entry."            )
            back_link.click()
            continue

        cvr_raw = ejerinfo.get("Nummer")

        if not cvr_raw:
            orchestrator_connection.log_info(f"Missing CVR for {henstilling_id}, skipping entry.")
            back_link.click()
            continue

        if is_valid_cvr(cvr_raw):
            cvr = int(cvr_raw)
        else:
            orchestrator_connection.log_info(f"Invalid CVR '{cvr_raw}' for {henstilling_id}, skipping entry.")
            back_link.click()
            continue



        # --- Latitude/Longitude ---
        latitude, longitude = extract_latlon_from_maplink(driver)
        # --- Fallback to geocoding if no coords ---
        Vejnavn = kontrol_info.get("Gade") or None
        Husnummer = kontrol_info.get("Husnummer") or None
        address_text = None
        if Vejnavn:
            if Husnummer:
                address_text = Vejnavn+" "+Husnummer
            else:
                address_text = Vejnavn
                
        if latitude and longitude:
            latitude, longitude = replace_coord_if_too_close(address_text, latitude, longitude, 100)
        
        if not latitude or not longitude:
            if address_text:
                geocoded = geocode_address(address_text)
                if geocoded:
                    latitude, longitude = geocoded

        # --- CVR and company name lookup ---

        
        firmanavn = ejerinfo.get("Navn") or get_firmanavn_cached(container, cvr) or "Ugyldigt CVR"

        # --- Start/End Dates ---
        startdato = kontrol_info.get("Fra")
        slutdato = kontrol_info.get("Til")
        
        try:
            if startdato:
                startdato = datetime.strptime(startdato.split()[0], "%d-%m-%y").date()
            if slutdato:
                slutdato = datetime.strptime(slutdato.split()[0], "%d-%m-%y").date()
        except Exception:
            startdato = slutdato = None

        try:
            if kontrol_info.get("Fra"):
                startdato = datetime.strptime(kontrol_info["Fra"].split()[0], "%d-%m-%y").date()
            if kontrol_info.get("Til"):
                slutdato = datetime.strptime(kontrol_info["Til"].split()[0], "%d-%m-%y").date()
        except Exception:
            pass


        meta = {
            "cvr": cvr,
            "firmanavn": firmanavn,
            "startdato": startdato,
            "slutdato": slutdato,
            "adresse": address_text,
            "latitude": latitude,
            "longitude": longitude
        }
        sync_henstilling(container, henstilling_id, forseelser, meta)
        back_link.click()
    return all_results

def extract_all_widgets(driver):
    """
    Dynamically extract all widgets on the page based on their <h3> titles
    and the content under .widgetinnhold. Returns a dict of dicts/lists.
    """
    widgets_data = {}

    widgets = driver.find_elements(By.CSS_SELECTOR, "div.widget")
    for widget in widgets:
        try:
            # Get the title (widgethead > h3)
            title_el = widget.find_element(By.CSS_SELECTOR, ".widgethead h3")
            title = title_el.text.strip()
            if not title:
                continue
        except Exception:
            continue

        # Find the visible content section
        try:
            content_el = widget.find_element(By.CSS_SELECTOR, ".widgetinnhold")
        except Exception:
            continue

        # Try to parse the content heuristically
        section_data = parse_widget_content(content_el)
        widgets_data[title] = section_data

    return widgets_data


def parse_widget_content(content_el):
    """
    Generic parser for the inner content of a widget.
    Handles:
    - propertytable (th/td pairs)
    - tabell with headers
    - plain text / spans
    """
    data = {}

    # --- Case 1: Standard key/value table (propertytable) ---
    kv_rows = content_el.find_elements(By.XPATH, ".//table[contains(@class,'propertytable')]/tbody/tr")
    if kv_rows:
        for tr in kv_rows:
            try:
                key = tr.find_element(By.TAG_NAME, "th").text.strip().rstrip(":")
                val = tr.find_element(By.TAG_NAME, "td").text.strip()
                data[key] = val
            except Exception:
                continue
        return data

    # --- Case 2: Tabular comment/record list ---
    tab_rows = content_el.find_elements(By.XPATH, ".//table[contains(@class,'tabell')]/tbody/tr")
    if tab_rows:
        table_data = []
        for tr in tab_rows:
            cells = [td.text.strip() for td in tr.find_elements(By.TAG_NAME, "td")]
            if any(cells):
                table_data.append(cells)
        return table_data

    # --- Case 3: Plain text or nested spans ---
    text_content = content_el.text.strip()
    if text_content:
        return text_content

    return {}


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
        r = requests.get(CVR_API_URL, params={"country": "dk", "search": cvr},
                         headers={"User-Agent": USER_AGENT}, timeout=5)
        if r.status_code == 200:
            data = r.json()
            return data.get("name")
    except Exception as e:
        print(f"CVR lookup failed for {cvr}: {e}")
    return None

def sync_henstilling(container, henstilling_id, forseelser, meta):
    """
    Sync henstilling data in Cosmos DB:
    - Only inserts or updates rows that are 'Ny'
    - Never deletes any rows (preserves locked statuses)
    - Efficiently upserts items using partition keys
    """

    # Fetch all docs for this Henstilling (any FakturaStatus)
    existing_docs = list(container.query_items(
        query="SELECT * FROM c WHERE c.HenstillingId = @h",
        parameters=[{"name": "@h", "value": henstilling_id}],
        enable_cross_partition_query=True
    ))

    # Build quick lookup by ID
    existing_map = {d["id"]: d for d in existing_docs}

    for f in forseelser:
        fid = f"{henstilling_id}_{f['nummer']}"
        existing = existing_map.get(fid)

        # Skip locked statuses
        if existing and existing.get("FakturaStatus") not in ("Ny"):
            continue  # do not modify rows in other statuses


        # Build new or updated item
        item = {
            "id": fid,
            "HenstillingId": henstilling_id,
            "ForseelseNr": f["nummer"],
            "Forseelse": f["text"].strip().split(" ", 1)[1],
            "CVR": meta.get("cvr"),
            "FirmaNavn": meta.get("firmanavn"),
            "Adresse": meta.get("adresse"),
            "Latitude": meta.get("latitude"),
            "Longitude": meta.get("longitude"),
            "Startdato": str(meta.get("startdato")),
            "Slutdato": existing.get("Slutdato") if existing else None,
            "Kvadratmeter": existing.get("Kvadratmeter") if existing else None,
            "Tilladelsestype": (existing.get("Tilladelsestype") if existing and existing.get("Tilladelsestype") is not None else f.get("tilladelsestype")),
            "FakturaStatus": "Ny",
        }

        # Upsert is cheap and works fine when the partition key stays the same
        container.upsert_item(body=item)

            
def select_predefined_filter(driver, wait, value):
    """Selects a predefined filter safely, even after Wicket re-renders the DOM."""
    select_xpath = "//select[@name='topLevelTabContent:content:filterPanel:predefinedFilter:definition']"

    # Find the dropdown fresh every time
    select_el = wait.until(EC.presence_of_element_located((By.XPATH, select_xpath)))

    # Set the value and trigger change
    driver.execute_script("""
        const el = arguments[0];
        el.value = arguments[1];
        el.dispatchEvent(new Event('change'));
    """, select_el, value)

    # Wait for the next version of the dropdown to appear
    time.sleep(1)
    wait.until(EC.staleness_of(select_el))
    wait.until(EC.presence_of_element_located((By.XPATH, select_xpath)))

    
def is_valid_cvr(cvr_str: str) -> bool:
    """Return True if CVR has 8 digits and passes modulus-11 validation."""
    if len(cvr_str) != 8 or not cvr_str.isdigit():
        return False

    weights = [2, 7, 6, 5, 4, 3, 2, 1]
    total = sum(int(d) * w for d, w in zip(cvr_str, weights))

    return total % 11 == 0