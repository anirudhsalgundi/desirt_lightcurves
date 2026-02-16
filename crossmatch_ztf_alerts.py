


#FIXME: fix imports, logging, and docstrings in this file. Also, add a function to save the updated master database with ZTF alerts added.

def argument_parser() -> argparse.Namespace:
    """
    Parse command-line arguments for the ZTF alert crossmatching script.
    """
    parser = argparse.ArgumentParser(description="Crossmatch ZTF alerts with DESIRT master database.")
    parser.add_argument("--desirt_database", type=str, required=True, help="Path to the DESIRT master database (HDF5 file).")
    parser.add_argument("--kowalski_creds", type=str, required=False, default = "", help="Path to the JSON file with Kowalski credentials.")
    parser.add_argument("--projections", type=str, required=False, default = "", help="Path to the JSON file with needed projections.")
    parser.add_argument("--search_radius", type=float, default=3.0, help="Search radius in arcseconds for crossmatching (default: 3 arcsec).")
    return parser.parse_args()

def get_coords(desirt_database) -> list:
    """
    Extract sky coordinates (RA, Dec) from the DESIRT master database.
    """
    coords = []
    for objid, data in desirt_database.items():
        ra = data.get("RA")
        dec = data.get("Dec")
        if ra is not None and dec is not None:
            coords.append((ra, dec))
    return coords


# repurposed
def get_kowalski_instance(kowalski_creds, projections) -> Kowalski:

    """
    Get an authenticated Kowalski instance.

    Parameters:

    - kowalski_creds (str): Path to the JSON file with Kowalski credentials. Set up a bash environment with export KOWALSKI_CREDENTIALS="path_to_file" for a safer login.

    - projections (str): Path to the JSON file with needed projections. Set up a bash environment with export DETAILED_ALERT_PROJECTIONS="path to file detailed_projections.json" for a safer login.

    Returns:
    - Kowalski: Authenticated Kowalski instance.
    """

    print("*" * 50)

    # Load credentials from the specified JSON file, raise exception if fails
    try:
        with open(kowalski_creds, "r") as f:
            creds = json.load(f)
            print(f"Loading Kowalski credentials from: {kowalski_creds}")

    except Exception as e:
        print("!! Failed to load Kowalski credentials. Error message: ", e)
        return None, None

    # Load needed projections from the specified JSON file, raise exception if fails
    try:
        with open(projections, "r") as f:
            needed_projections = json.load(f)["projection"]
        print(f"Loading needed projections from: {projections}")
    
    except Exception as e:
        print("!! Failed to load needed projections. Error message: ", e)
        return None, None

    # Create Kowalski instance
    kowalski_instance = Kowalski(
        username=creds["username"],
        password=creds["password"],
        protocol=creds["protocol"],
        host=creds["host"],
        port=creds["port"])

    # Test authentication, raise exception if fails
    try:
        kowalski_instance.ping()
        print("---Kowalski instance created and authenticated---")
        print("*" * 50)

    except Exception as e:
        print("!! Failed to authenticate Kowalski instance. Error message: ", e)
        print("*" * 50)
        return None, None

    # Return the instance and needed projections
    return kowalski_instance, needed_projections


def _query_kowalski(ra, dec, radius, projections, kowalski_instance) -> dict:

    """
    Query Kowalski for ZTF alerts around given RA and Dec within a specified radius.
    Parameters:
    - ra (float): Right Ascension in degrees.
    - dec (float): Declination in degrees.
    - radius (float): Search radius in arcseconds.
    - needed_projections (dict): Fields to project in the query.
    - kowalski_instance (Kowalski): Authenticated Kowalski instance.

    Returns:
    - alerts: Dictionary containing the query results.
    - unique_object_ids: List of unique object IDs found in the query.
    """

    # Build the query payload for a cone search
    payload = {

    "query_type": "cone_search",

    "query": {
        "object_coordinates": {
            "cone_search_radius": radius,
            "cone_search_unit": "arcsec",
            "radec": {"candidates": [ra, dec]}
        },
        "catalogs": {
            "ZTF_alerts": {
                "projection": projections
                }
            }
        }
    }

    # Execute the query and retrieve results
    results = kowalski_instance.query(payload)

    # Extract alerts for the candidates
    alerts = results['default']['data']['ZTF_alerts'].get('candidates', [])

    # Extract unique object IDs, for reporting number of candidates found in the localisation region
    object_ids = np.array([alert['objectId'] for alert in alerts])

    # Return the alerts and unique object IDs
    return alerts

def crossmatch_ztf_alerts(coords, radius, projections, kowalski_instance) -> dict:
    
    """
    Crossmatch ZTF alerts with given coordinates and search radius.

    Parameters:
    - coords (list): List of tuples containing RA and Dec coordinates.
    - radius (float): Search radius in arcseconds.
    - projections (dict): Fields to project in the query.
    - kowalski_instance (Kowalski): Authenticated Kowalski instance.

    Returns:
    - crossmatched_alerts: Dictionary mapping object IDs to their corresponding alerts.
    """

    crossmatched_alerts = {}

    for ra, dec in tqdm(coords, desc="Crossmatching ZTF alerts"):
        alerts = _query_kowalski(ra, dec, radius, projections, kowalski_instance)
        for alert in alerts:
            object_id = alert['objectId']
            if object_id not in crossmatched_alerts:
                crossmatched_alerts[object_id] = []
            crossmatched_alerts[object_id].append(alert)

    return crossmatched_alerts


def add_ztf_alerts_to_master_database(desirt_database, crossmatched_alerts) -> dict:
    """
    Add crossmatched ZTF alerts to the DESIRT master database.

    Parameters:
    - desirt_master_database (dict): The existing DESIRT master database.
    - crossmatched_alerts (dict): Dictionary mapping object IDs to their corresponding alerts.

    Returns:
    - updated_master_database: Updated DESIRT master database with ZTF alerts added.
    """

    updated_database = desirt_database.copy()

    #FIXME:  still dont know how the desirt_database looks like
    for object_id, alerts in crossmatched_alerts.items():
        if object_id not in updated_database:
            updated_database[object_id] = {}
        updated_database[object_id]['ZTF_alerts'] = alerts

    return updated_database


def main():