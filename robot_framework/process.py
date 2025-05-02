"""This module contains the main process of the robot."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pathlib import Path
import os
from datetime import datetime
import time
import csv
import requests
import pyodbc
from math import radians, cos, sin, asin, sqrt
import regex

CVR_API_URL = "https://cvrapi.dk/api"
USER_AGENT = "Henstillinger AAK"
DEPOT = (56.161147, 10.13455)

def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")
    Credentials = orchestrator_connection.get_credential("Mobility_Workspace")
    username = Credentials.username
    password = Credentials.password
    url = orchestrator_connection.get_constant("MobilityWorkspaceURL").value
    sql_credentials = orchestrator_connection.get_credential("AzureSQL")
    sql_user = sql_credentials.username
    sql_password = sql_credentials.password
    sql_server = orchestrator_connection.get_constant("VejmanHistorikSQL").value

    # Azure SQL connection string
    SQL_CONN_STRING = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server=tcp:{sql_server};"
        f"Database=TilladelsesHistorik;"
        f"Persist Security Info=False;"
        f"UID={sql_user};"
        f"PWD={sql_password};"
        f"MultipleActiveResultSets=False;"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )

    conn = pyodbc.connect(SQL_CONN_STRING)
    cursor = conn.cursor()

    cursor.execute("SELECT DB_NAME()")
    orchestrator_connection.log_info("Connected to DB:", cursor.fetchone()[0])

    download_dir = str(Path.home() / "Downloads")
        
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument('--remote-debugging-pipe')
    options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
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

    driver.get(f"{url}/parking/tab/4.6")

    henstillinger_link = wait.until(EC.element_to_be_clickable(
        (By.XPATH, "//a[span[text()='Henstillinger']]")
    ))    
    henstillinger_link.click()

    wait.until(EC.visibility_of_element_located((
    By.XPATH,
    "//div[contains(@class, 'wicket-modal')]//span[@class='w_captionText' and text()='Henstillinger']"
    )))

    from_date_input = wait.until(EC.presence_of_element_located((
        By.XPATH,
        "//label[text()='From date']/following::input[@type='text'][1]"
    )))

    to_date_input = wait.until(EC.presence_of_element_located((
        By.XPATH,
        "//label[text()='To date']/following::input[@type='text'][1]"
    )))


    from_date_input.clear()
    from_date_input.send_keys("01-01-20")
    to_date_input.clear()
    to_date_input.send_keys(datetime.now().strftime("%d-%m-%y"))

    initial_files = set(os.listdir(download_dir))

    driver.find_element(By.XPATH, '//input[@type="submit" and @value="OK"]').click()

    # Wait for download to complete
    timeout = 60
    start_time = time.time()
    downloaded_file = None

    while True:
        current_files = set(os.listdir(download_dir))
        new_files = current_files - initial_files
        csv_files = [file for file in new_files if file.lower().endswith(".csv")]
        if csv_files:
            downloaded_file = os.path.join(download_dir, csv_files[0])
            orchestrator_connection.log_info(f"Download completed: {downloaded_file}")
            break

        if time.time() - start_time > timeout:
            orchestrator_connection.log_info("Timeout reached while waiting for a download.")
            break

        time.sleep(1)

    driver.quit()

    if not downloaded_file:
        raise FileNotFoundError("No CSV file was downloaded.")

    cursor.execute("SELECT HenstillingId, CVR, FakturaStatus FROM [dbo].[VejmanKassen]")
    existing_rows = {
        row.HenstillingId: {"CVR": row.CVR, "FakturaStatus": row.FakturaStatus}
        for row in cursor.fetchall()
    }

    # --- Process CSV ---
    with open(downloaded_file, encoding="cp1252") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            if row["Status på sagen"].strip() != "Henstilling til oppfølging":
                continue

            try:
                henstilling_id = row["Løbenummer"].strip()
                if not henstilling_id:
                    continue

                cvr = int(row["Ejerinfo"].strip())
                startdato = datetime.strptime(row["StartDato"].split()[0], "%d-%m-%Y").date()
                slutdato = datetime.strptime(row["SlutDato"].split()[0], "%d-%m-%Y").date()

                gade = row["Gade"].strip()
                husnr = row["Husnummer"].strip()
                adresse = f"{gade} {husnr}".strip()

                forseelse = row["Navn på forseelse"].strip()

                try:
                    latitude = float(row["Latitude"].replace(",", "."))
                    longitude = float(row["Longitude"].replace(",", "."))
                    latitude, longitude = replace_coord_if_too_close(adresse, latitude, longitude)
                except ValueError:
                    orchestrator_connection.log_info(f"{henstilling_id} has invalid lat/lon, skipping")
                    continue

                row_exists = henstilling_id in existing_rows

                if row_exists:
                    existing = existing_rows[henstilling_id]
                    if existing["FakturaStatus"] is not None and existing["FakturaStatus"] != "Ny":
                        continue  # Skip update if FakturaStatus already set and not "Ny"

                    if existing["CVR"] == cvr:
                        # CVR unchanged — skip CVR lookup and leave FirmaNavn alone
                        cursor.execute("""
                            UPDATE [dbo].[VejmanKassen]
                            SET Startdato = ?, Slutdato = ?, Adresse = ?, Forseelse = ?, Longitude = ?, Latitude = ?
                            WHERE HenstillingId = ?
                        """, (
                            startdato, slutdato, adresse, forseelse, longitude, latitude, henstilling_id
                        ))
                        orchestrator_connection.log_info(f"Updated {henstilling_id} without changing FirmaNavn")
                    else:
                        # CVR changed — fetch new name and update
                        firmanavn = get_firmanavn(cvr)
                        cursor.execute("""
                            UPDATE [dbo].[VejmanKassen]
                            SET CVR = ?, FirmaNavn = ?, Startdato = ?, Slutdato = ?, Adresse = ?, Forseelse = ?, Longitude = ?, Latitude = ?
                            WHERE HenstillingId = ?
                        """, (
                            cvr, firmanavn, startdato, slutdato, adresse, forseelse, longitude, latitude, henstilling_id
                        ))
                        orchestrator_connection.log_info(f"Updated {henstilling_id} with new FirmaNavn: {firmanavn}")
                else:
                    # New row — insert
                    firmanavn = get_firmanavn(cvr)
                    if firmanavn:
                        cursor.execute("""
                            INSERT INTO [dbo].[VejmanKassen] (
                                HenstillingId, CVR, FirmaNavn, Startdato, Slutdato, Adresse,
                                Forseelse, Longitude, Latitude, FakturaStatus
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            henstilling_id, cvr, firmanavn, startdato, slutdato, adresse,
                            forseelse, longitude, latitude, "Ny"
                        ))
                        orchestrator_connection.log_info(f"Inserted {henstilling_id} - {firmanavn}")
                    else:
                        orchestrator_connection.log_info(f"Ugyldigt CVR eller manglende firmanavn")
                        cursor.execute("""
                            INSERT INTO [dbo].[VejmanKassen] (
                                HenstillingId, CVR, FirmaNavn, Startdato, Slutdato, Adresse,
                                Forseelse, Longitude, Latitude, FakturaStatus
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            henstilling_id, cvr, "Ugyldigt CVR eller manglende firmanavn", startdato, slutdato, adresse,
                            forseelse, longitude, latitude, "Ny"
                        ))

            except Exception as e:
                orchestrator_connection.log_info(f"Error processing row {row.get('Løbenummer', '???')}: {e}")

    conn.commit()
    conn.close()

    os.remove(downloaded_file)

# --- CVR lookup helper ---
def get_firmanavn(cvr):
    try:
        r = requests.get(CVR_API_URL, params={
            "country": "dk",
            "search": cvr
        }, headers={"User-Agent": USER_AGENT}, timeout=5)

        if r.status_code == 200:
            data = r.json()
            return data.get("name", None)
        else:
            print(f"CVR lookup failed for {cvr}: status {r.status_code}")
            return None
    except Exception as e:
        print(f"Exception during CVR lookup for {cvr}: {e}")
        return None

def haversine(coord1, coord2):
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    R = 6371000
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return R * c

def get_firmanavn(cvr):
    try:
        r = requests.get(CVR_API_URL, params={
            "country": "dk",
            "search": cvr
        }, headers={"User-Agent": USER_AGENT}, timeout=5)

        if r.status_code == 200:
            data = r.json()
            return data.get("name", None)
        else:
            print(f"CVR lookup failed for {cvr}: status {r.status_code}")
            return None
    except Exception as e:
        print(f"Exception during CVR lookup for {cvr}: {e}")
        return None
    
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
    if distance > threshold_m:
        return latitude, longitude
    cleaned_address = clean_address(address)
    geocode_coord = geocode_address(cleaned_address)
    if cleaned_address:
        new_coord = geocode_coord
        latitude = new_coord[0]
        longitude = new_coord[1]
    return latitude, longitude