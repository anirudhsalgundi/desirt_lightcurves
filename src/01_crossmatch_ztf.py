import argparse
import json
import logging
from pathlib import Path
from datetime import datetime
import numpy as np
import h5py
from tqdm import tqdm
from penquins import Kowalski
from astropy.coordinates import SkyCoord
import astropy.units as u
from astropy.time import Time

def setup_logging():
    """
    Set up logging to both console and timestamped log file.
    """
    # Create logs directory if it doesn't exist
    log_dir = Path("./logs")
    log_dir.mkdir(exist_ok=True)
    
    # Create timestamped log filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"log_ztfcrossmatch_{timestamp}.log"
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Log file created: {log_file}")
    return logger

# Set up logging
logger = setup_logging()

def argument_parser() -> argparse.Namespace:
    """
    Parse command-line arguments for the ZTF alert crossmatching script.
    """
    parser = argparse.ArgumentParser(description="Crossmatch ZTF alerts with DESIRT master database.")
    parser.add_argument("--desirt_database", type=str, required=True, help="Path to the DESIRT master database (HDF5 file).")
    parser.add_argument("--kowalski_creds", type=str, default="../utils/kowalski_credentials.json", 
                   help="Path to the JSON file with Kowalski credentials (default: ../utils/kowalski_credentials.json).")
    parser.add_argument("--projections", type=str, required=False, default = "", help="Path to the JSON file with needed projections.")
    parser.add_argument("--search_radius", type=float, default=3.0, help="Search radius in arcseconds for crossmatching (default: 3 arcsec).")
    return parser.parse_args()

def get_coords(desirt_database) -> list:
    """
    Extract sky coordinates (RA, Dec) from the DESIRT master database (HDF5 file).
    
    Parameters:
    - desirt_database: h5py.File object with DESIRT data
    
    Returns:
    - List of tuples (objid, ra, dec)
    """
    coords = []
    for objid in desirt_database.keys():
        group = desirt_database[objid]
        ra = group.attrs.get("ra")
        dec = group.attrs.get("dec")
        if ra is not None and dec is not None:
            coords.append((objid, ra, dec))
    logger.info(f"Extracted {len(coords)} coordinates from DESIRT database")
    return coords

def get_kowalski_instance(kowalski_creds, projections) -> Kowalski:
    """
    Get an authenticated Kowalski instance.

    Parameters:
    - kowalski_creds (str): Path to the JSON file with Kowalski credentials.
    - projections (str): Path to the JSON file with needed projections.

    Returns:
    - Kowalski: Authenticated Kowalski instance.
    """
    logger.info("Initializing Kowalski instance...")

    try:
        with open(kowalski_creds, "r") as f:
            creds = json.load(f)
            logger.info(f"Loaded Kowalski credentials from: {kowalski_creds}")
    except Exception as e:
        logger.error(f"Failed to load Kowalski credentials: {e}")
        return None, None

    try:
        with open(projections, "r") as f:
            needed_projections = json.load(f)["projection"]
            logger.info(f"Loaded needed projections from: {projections}")
    except Exception as e:
        logger.error(f"Failed to load needed projections: {e}")
        return None, None

    kowalski_instance = Kowalski(
        username=creds["username"],
        password=creds["password"],
        protocol=creds["protocol"],
        host=creds["host"],
        port=creds["port"]
    )

    try:
        kowalski_instance.ping()
        logger.info("Kowalski instance authenticated successfully")
    except Exception as e:
        logger.error(f"Failed to authenticate Kowalski instance: {e}")
        return None, None

    return kowalski_instance, needed_projections

def _query_kowalski(ra, dec, radius, projections, kowalski_instance) -> dict:
    """
    Query Kowalski for ZTF alerts around given RA and Dec within a specified radius.
    """
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

    results = kowalski_instance.query(payload)
    alerts = results['default']['data']['ZTF_alerts'].get('candidates', [])
    return alerts

def crossmatch_ztf_alerts(coords, radius, projections, kowalski_instance) -> dict:
    """
    Crossmatch ZTF alerts with given coordinates and search radius.

    Parameters:
    - coords (list): List of tuples containing (objid, RA, Dec).
    - radius (float): Search radius in arcseconds.
    - projections (dict): Fields to project in the query.
    - kowalski_instance (Kowalski): Authenticated Kowalski instance.

    Returns:
    - crossmatched_alerts: Dictionary mapping DESIRT object IDs to their corresponding ZTF alerts.
    """
    crossmatched_alerts = {}

    for objid, ra, dec in tqdm(coords, desc="Crossmatching ZTF alerts"):
        alerts = _query_kowalski(ra, dec, radius, projections, kowalski_instance)
        
        if alerts:
            crossmatched_alerts[objid] = alerts
            logger.info(f"Found {len(alerts)} ZTF alerts for {objid}")

    logger.info(f"Total DESIRT objects with ZTF matches: {len(crossmatched_alerts)}")
    return crossmatched_alerts

def add_ztf_alerts_to_master_database(desirt_database, crossmatched_alerts, search_radius) -> dict:
    """
    Add ZTF alert data to the DESIRT master database.
    
    Parameters:
    - desirt_database: h5py.File object (opened in 'a' mode for appending)
    - crossmatched_alerts: Dictionary mapping DESIRT object IDs to ZTF alerts
    - search_radius: Search radius used for crossmatching (for metadata)
    
    Returns:
    - Summary statistics dictionary
    """
    stats = {
        'desirt_objects_processed': 0,
        'desirt_objects_with_ztf_matches': 0,
        'total_ztf_alerts_added': 0,
        'ztf_objectIds_found': set()
    }
    
    for desirt_objid, ztf_alerts in tqdm(crossmatched_alerts.items(), desc="Adding ZTF data to database"):
        if desirt_objid not in desirt_database:
            logger.warning(f"DESIRT object {desirt_objid} not found in database, skipping")
            continue
            
        group = desirt_database[desirt_objid]
        
        # Create a subgroup for ZTF data if it doesn't exist
        if 'ztf_crossmatches' in group:
            del group['ztf_crossmatches']  # Remove old data if re-running
            
        ztf_group = group.create_group('ztf_crossmatches')
        ztf_group.attrs['search_radius_arcsec'] = search_radius
        ztf_group.attrs['num_alerts'] = len(ztf_alerts)
        ztf_group.attrs['crossmatch_date'] = datetime.now().isoformat()
        
        # Collect all unique ZTF objectIds for this DESIRT object
        unique_ztf_objects = {}
        for alert in ztf_alerts:
            ztf_objid = alert.get('objectId', None)
            if ztf_objid:
                if ztf_objid not in unique_ztf_objects:
                    unique_ztf_objects[ztf_objid] = []
                unique_ztf_objects[ztf_objid].append(alert)
                stats['ztf_objectIds_found'].add(ztf_objid)
        
        # Create a subgroup for each unique ZTF object
        for idx, (ztf_objid, alerts_for_object) in enumerate(unique_ztf_objects.items()):
            ztf_obj_group = ztf_group.create_group(f'ztf_{idx:03d}_{ztf_objid}')
            ztf_obj_group.attrs['objectId'] = ztf_objid
            ztf_obj_group.attrs['num_detections'] = len(alerts_for_object)
            
            # Extract time series data from all alerts for this ZTF object
            mjds = []
            mags = []
            mag_errs = []
            fids = []
            
            # Store additional useful metadata
            ra_list = []
            dec_list = []
            
            for alert in alerts_for_object:
                candidate = alert.get('candidate', {})
                
                # Convert JD to MJD
                jd = candidate.get('jd')
                if jd is not None:
                    mjd = Time(jd, format='jd').mjd
                    mjds.append(mjd)
                else:
                    mjds.append(np.nan)
                
                # Extract photometry
                mags.append(candidate.get('magpsf', np.nan))
                mag_errs.append(candidate.get('sigmapsf', np.nan))
                fids.append(candidate.get('fid', -1))
                
                # Extract coordinates
                ra_list.append(candidate.get('ra', np.nan))
                dec_list.append(candidate.get('dec', np.nan))
                
                # Store scalar metadata from first alert as attributes
                if len(mjds) == 1:
                    for key in ['programid', 'isdiffpos', 'drb', 'classtar', 'ndethist', 'ncovhist']:
                        value = candidate.get(key)
                        if value is not None:
                            try:
                                ztf_obj_group.attrs[key] = value
                            except (TypeError, ValueError):
                                pass
            
            # Convert to numpy arrays
            mjds = np.array(mjds)
            mags = np.array(mags)
            mag_errs = np.array(mag_errs)
            fids = np.array(fids, dtype=int)
            ra_list = np.array(ra_list)
            dec_list = np.array(dec_list)
            
            # Sort by MJD
            sort_idx = np.argsort(mjds)
            mjds = mjds[sort_idx]
            mags = mags[sort_idx]
            mag_errs = mag_errs[sort_idx]
            fids = fids[sort_idx]
            ra_list = ra_list[sort_idx]
            dec_list = dec_list[sort_idx]
            
            # Store time series as datasets
            ztf_obj_group.create_dataset('ztf_mjd', data=mjds, compression='gzip')
            ztf_obj_group.create_dataset('ztf_mag', data=mags, compression='gzip')
            ztf_obj_group.create_dataset('ztf_magerr', data=mag_errs, compression='gzip')
            ztf_obj_group.create_dataset('ztf_fid', data=fids, compression='gzip')
            ztf_obj_group.create_dataset('ztf_ra', data=ra_list, compression='gzip')
            ztf_obj_group.create_dataset('ztf_dec', data=dec_list, compression='gzip')
            
            # Store filter information as attribute (1=g, 2=r, 3=i)
            unique_fids = np.unique(fids[fids > 0])
            ztf_obj_group.attrs['filters_present'] = unique_fids.tolist()
            
            # Store cutout images from the most recent detection
            most_recent_alert = alerts_for_object[sort_idx[-1]]
            
            if 'cutoutScience' in most_recent_alert:
                cutout = most_recent_alert['cutoutScience']
                if isinstance(cutout, dict) and 'stampData' in cutout:
                    stamp = cutout['stampData']
                    if isinstance(stamp, bytes):
                        stamp = np.void(stamp)
                    ztf_obj_group.create_dataset('science_image', data=stamp)
                    
            if 'cutoutTemplate' in most_recent_alert:
                cutout = most_recent_alert['cutoutTemplate']
                if isinstance(cutout, dict) and 'stampData' in cutout:
                    stamp = cutout['stampData']
                    if isinstance(stamp, bytes):
                        stamp = np.void(stamp)
                    ztf_obj_group.create_dataset('template_image', data=stamp)
                    
            if 'cutoutDifference' in most_recent_alert:
                cutout = most_recent_alert['cutoutDifference']
                if isinstance(cutout, dict) and 'stampData' in cutout:
                    stamp = cutout['stampData']
                    if isinstance(stamp, bytes):
                        stamp = np.void(stamp)
                    ztf_obj_group.create_dataset('difference_image', data=stamp)
            
            stats['total_ztf_alerts_added'] += len(alerts_for_object)
        
        stats['desirt_objects_processed'] += 1
        stats['desirt_objects_with_ztf_matches'] += 1
    
    logger.info(f"Added ZTF data for {stats['desirt_objects_with_ztf_matches']} DESIRT objects")
    logger.info(f"Total ZTF alerts added: {stats['total_ztf_alerts_added']}")
    logger.info(f"Unique ZTF objectIds: {len(stats['ztf_objectIds_found'])}")
    
    stats['ztf_objectIds_found'] = list(stats['ztf_objectIds_found'])
    return stats

def save_crossmatch_summary(stats, output_path):
    """
    Save crossmatch statistics to a JSON file.
    """
    output_file = Path(output_path).parent / f"ztf_crossmatch_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(output_file, 'w') as f:
        json.dump(stats, f, indent=2)
    
    logger.info(f"Crossmatch summary saved to: {output_file}")
    return output_file

def main():
    """
    Main function to crossmatch ZTF alerts with DESIRT master database.
    """
    args = argument_parser()
    
    logger.info("="*60)
    logger.info("Starting ZTF Alert Crossmatching Pipeline")
    logger.info("="*60)
    
    logger.info(f"Opening DESIRT database: {args.desirt_database}")
    try:
        desirt_db = h5py.File(args.desirt_database, 'a')
    except Exception as e:
        logger.error(f"Failed to open DESIRT database: {e}")
        return
    
    kowalski_instance, projections = get_kowalski_instance(args.kowalski_creds, args.projections)
    
    if kowalski_instance is None or projections is None:
        logger.error("Failed to initialize Kowalski instance. Exiting.")
        desirt_db.close()
        return
    
    coords = get_coords(desirt_db)
    
    if not coords:
        logger.warning("No coordinates found in DESIRT database. Exiting.")
        desirt_db.close()
        return
    
    logger.info(f"Crossmatching with search radius: {args.search_radius} arcsec")
    crossmatched_alerts = crossmatch_ztf_alerts(coords, args.search_radius, projections, kowalski_instance)
    
    if not crossmatched_alerts:
        logger.warning("No ZTF matches found. Exiting.")
        desirt_db.close()
        return
    
    logger.info("Adding ZTF alerts to master database...")
    stats = add_ztf_alerts_to_master_database(desirt_db, crossmatched_alerts, args.search_radius)
    
    summary_file = save_crossmatch_summary(stats, args.desirt_database)
    
    desirt_db.close()
    logger.info("Database closed successfully")
    
    logger.info("="*60)
    logger.info("ZTF Crossmatching Complete!")
    logger.info("="*60)

if __name__ == "__main__":
    main()