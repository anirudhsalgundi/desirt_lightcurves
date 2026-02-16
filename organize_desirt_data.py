import os
import glob
import json
import pickle
import re
import argparse
import h5py
from tqdm import tqdm 
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from functools import partial

import numpy as np
import polars as pl
import matplotlib.pyplot as plt

from astropy.io import fits
from astropy.time import Time
from astropy.coordinates import SkyCoord
from astropy.table import vstack
import astropy.units as u
from typing import Dict, Tuple, List, Optional

import logging

# Constants
INFER_SCHEMA_LENGTH = 10000
REQUIRED_FITS_COLS = ['MJD_OBS', 'FILTER', 'MAG_ALT', 'MAGERR_ALT', 'MAG_FPHOT', 'MAGERR_FPHOT']

# Create logs directory
LOGSDIR = Path("./logs")
LOGSDIR.mkdir(exist_ok=True)

log_filename = LOGSDIR / f"log_from_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Configure logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Log the log file location at the start
logger.info(f"Logging to: {log_filename}")

def argument_parser() -> argparse.Namespace:
    """
    Parse command-line arguments for the DESIRT data organization script.
    """
    parser = argparse.ArgumentParser(description="Organize DESIRT data from CSV and FITS files and create a database")
    parser.add_argument("--csv_path", type=str, required=True, help="Path to the CSV file containing list of summary CSVs")
    parser.add_argument("--fits_path", type=str, required=True, help="Path to the text file containing list of FITS directories")
    parser.add_argument("--n_workers", type=int, default=None, help="Number of parallel workers (default: CPU count - 1)")
    parser.add_argument("--checkpoint_file", type=str, default="./checkpoint.pkl", help="Path to checkpoint file for resuming")
    parser.add_argument("--output_file", type=str, default="./results/desirt_master_database.h5", help="Path to output HDF5 file")

    return parser.parse_args()


def merge_csvs(file_with_path_to_csvs) -> pl.DataFrame:
    """
    Read multiple CSV files listed in a text file, merge them into a single Polars DataFrame, and return it.
    --------
    Parameters:
    - file_with_path_to_csvs: str, path to a text file where each line is a path to a CSV file to be merged.
     Returns:
    - merged_csv: pl.DataFrame, a Polars DataFrame containing the merged data from all the CSV files.
     Note: The function uses Polars for efficient CSV reading and merging. It reads each CSV file with an increased infer_schema_length to ensure correct data types are inferred, especially for larger datasets. The merged DataFrame is created using a diagonal concatenation to handle potential differences in columns across the CSV files.
    """

    with open(file_with_path_to_csvs, 'r') as f:
        summary_csv_paths = [line.strip() for line in f.readlines()]

        logger.info(f"Read {len(summary_csv_paths)} CSV files")
        merged_csv = pl.concat([pl.read_csv(f, infer_schema_length=INFER_SCHEMA_LENGTH) for f in summary_csv_paths], how="diagonal") 

    return merged_csv


def group_csv_data_by_objid(merged_csv) -> dict:
    """
    Group the merged CSV data by object ID (objid).
    ------------
    Parameters:
    - merged_csv: pl.DataFrame, the merged Polars DataFrame containing all the CSV data.

    Returns:
    - grouped_data: dict, a dictionary where each key is an objid and the value is another dictionary containing the grouped data for that objid.
    """

    grouped_data = {}
    grouped = merged_csv.group_by("objid").first()

    for row in grouped.iter_rows(named=True):
        objid = row["objid"]
        grouped_data[objid] = dict(row)

    logger.info(f"Grouped CSV data for {len(grouped_data)} unique objids")
    return grouped_data


def get_unique_objids(organised_csv_data: dict) -> list:

    """
    Get a list of unique object IDs (objids) from the organized CSV data.
    ------------
    Parameters:
    - organised_csv_data: dict, a dictionary where each key is an objid and the value is another dictionary containing the grouped data for that objid.
    Returns:
    - unique_objids: list, a list of unique object IDs extracted from the organized CSV data.

    The function simply extracts the keys from the organized CSV data dictionary, which represent the unique object IDs (objids), and returns them as a list. It also includes logging to indicate that the extraction of unique objids is taking place.
    """

    logger.info(f"Extracting unique objids from organized CSV data")
    return list(organised_csv_data.keys()) 


def list_all_fits_files(file_with_path_to_fits_files: str) -> list:

    """
    List all FITS files from the directories specified in a text file.
    ------------
    Parameters:
    - file_with_path_to_fits_files: str, path to a text file where each line is a path to a directory containing FITS files. 

    The function will read this file, iterate through each directory, and collect the paths of all FITS files found in those directories. It returns a list of paths to all the FITS files. The function also includes logging to indicate how many FITS files were found in total and prints a warning if any specified directory is not found.

    Returns:
    - paths_to_all_fits_files: list, a list of strings where each string is the path to a FITS file found in the specified directories.

    """

    paths_to_all_fits_files = []
    
    logger.info(f"Listing all FITS files from specified directories")
    with open(file_with_path_to_fits_files, 'r') as f:
        candidate_dirs = [line.strip() for line in f.readlines()]
    
    for cand_dir in tqdm(candidate_dirs, desc="Listing FITS files"):
        cand_path = Path(cand_dir)
        if cand_path.exists():
            fits_files = list(cand_path.glob("*.fits"))
            paths_to_all_fits_files.extend([str(f) for f in fits_files])  # Convert to strings
        else:
            logger.warning(f"Directory not found: {cand_path}")
    
    logger.info(f"Found {len(paths_to_all_fits_files)} fits files in total!!")
    return paths_to_all_fits_files


def organize_fits_path_by_objid(unique_objects: list, paths_to_all_fits_files: list) -> dict:

    """
    Organize FITS file paths by object ID (objid).
    ------------
    Parameters:
    - unique_objects: list, a list of unique object IDs (objids) for which we want to organize the FITS file paths.

    Returns:
    - organised_fits_paths: dict, a dictionary where each key is an objid and the value is a list of paths to FITS files that contain that objid in their filename. 
    
    The function iterates through each unique object ID and filters the list of all FITS file paths to find those that contain the object ID in their filename. It uses tqdm to provide a progress bar for the organization process. Only object IDs that have corresponding FITS files will be included in the returned dictionary.
    """

    organized_fits_paths = defaultdict(list)
    unique_objects_set = set(unique_objects)  # O(1) lookup
    
    logger.info(f"Organizing FITS paths for {len(unique_objects)} objects")
    
    for fits_file in tqdm(paths_to_all_fits_files, desc="Organizing FITS paths by objid"):
        filename = Path(fits_file).name
        # Find which objid this file belongs to
        for objid in unique_objects_set:
            if objid in filename:
                organized_fits_paths[objid].append(fits_file)
                break  # Assumes each file belongs to only one objid

    logger.info(f"Organized FITS paths for {len(organized_fits_paths)} unique objects")
    return dict(organized_fits_paths)


def _read_single_fits_file(args: tuple) -> Optional[dict]:
    """
    Worker function to read a single FITS file. Designed for parallel processing.
    
    Parameters:
    - args: tuple of (fits_file, objid, required_cols)
    
    Returns:
    - dict with objid and data, or None if reading failed
    """
    fits_file, objid = args
    
    try:
        with fits.open(fits_file, memmap=True) as hdul:
            # Check if extension 1 exists and has data
            if len(hdul) < 2 or hdul[1].data is None:
                return None
            
            data = hdul[1].data
            
            # Check if required columns exist
            if not all(col in data.dtype.names for col in REQUIRED_FITS_COLS):
                return None
            
            # Extract and return data
            return {
                'objid': objid,
                'mjds': np.array(data['MJD_OBS']),
                'filters': np.array(data['FILTER']),
                'mag_alt': np.array(data['MAG_ALT']),
                'magerr_alt': np.array(data['MAGERR_ALT']),
                'mag_fphot': np.array(data['MAG_FPHOT']),
                'magerr_fphot': np.array(data['MAGERR_FPHOT'])
            }
                
    except Exception as e:
        logger.error(f"Error reading {fits_file}: {e}")
        return None


def organize_fits_data(organized_fits_paths: dict, n_workers: Optional[int] = None) -> dict:

    """
    Organize FITS data by object ID (objid).
    ------------
    Parameters:
    - organized_fits_paths: dict, a dictionary where each key is an objid and the value is a list of paths to FITS files that contain that objid in their filename.

    Returns:
    - organised_fits_data: dict, a dictionary where each key is an objid and the value is another dictionary containing the organized FITS data for that objid, including arrays of MJD observations, filters, magnitudes and their errors for both aperture photometry (ALT) and forced photometry (FPHOT). 
    
    
    The function reads each FITS file for a given objid, extracts the relevant data, and concatenates it across all FITS files to create a comprehensive dataset for each object. It also includes error handling to skip files that cannot be read or do not contain the expected data, and logs warnings for any issues encountered during the process.
    """

    if n_workers is None:
        n_workers = max(1, cpu_count() - 1)
    
    logger.info(f"Using {n_workers} parallel workers for FITS data organization")
    
    # Prepare tasks for parallel processing
    tasks = []
    for objid, fits_files_list in organized_fits_paths.items():
        for fits_file in fits_files_list:
            tasks.append((fits_file, objid))
    
    logger.info(f"Processing {len(tasks)} FITS files across {len(organized_fits_paths)} objects")
    
    # Process files in parallel
    organized_fits_data = defaultdict(lambda: defaultdict(list))
    
    with Pool(n_workers) as pool:
        results = list(tqdm(
            pool.imap_unordered(_read_single_fits_file, tasks, chunksize=10),
            total=len(tasks),
            desc=f"Reading FITS files (parallel job with {n_workers} workers)"
        ))
    
    # Aggregate results by objid
    for result in results:
        if result is None:
            continue
        
        objid = result['objid']
        for key in ['mjds', 'filters', 'mag_alt', 'magerr_alt', 'mag_fphot', 'magerr_fphot']:
            organized_fits_data[objid][key].append(result[key])
    
    # Concatenate arrays for each object
    final_organized_data = {}
    for objid, data_dict in tqdm(organized_fits_data.items(), desc="Concatenating arrays"):
        try:
            final_organized_data[objid] = {
                key: np.concatenate(arrays) for key, arrays in data_dict.items()
            }
        except Exception as e:
            logger.error(f"Error concatenating data for {objid}: {e}")
            continue

    logger.info(f"Organized FITS data for {len(final_organized_data)} objects")
    return final_organized_data


def get_cutouts(organized_fits_paths: dict) -> Tuple[dict, dict, dict]:
    """
    Get cutout images (science, template, difference) for each object ID from the FITS files.
    
    Parameters:
    - organized_fits_paths: dict, a dictionary where each key is an objid and the value is a list of FITS file paths.

    Returns:
    - Tuple of three dicts: (science_images, template_images, difference_images)
    """

    science_images, template_images, difference_images = {}, {}, {}

    for objid, fits_files_list in tqdm(organized_fits_paths.items(), desc="Extracting cutout images"):
        last_fits_file = fits_files_list[-1]
        
        try:
            with fits.open(last_fits_file, memmap=True) as hdul:
                data = hdul[1].data

                science_images[objid] = np.array(data["PixA_THUMB_SCI"])
                template_images[objid] = np.array(data["PixA_THUMB_TEMP"])
                difference_images[objid] = np.array(data["PixA_THUMB_DIFF"])
                    
        except Exception as e:
            logger.error(f"Error reading cutouts for {objid} from {last_fits_file}: {e}")
            continue

    logger.info(f"Extracted cutout images for {len(science_images)} objects")
    return science_images, template_images, difference_images

    """                        
    Create a master database by combining organized FITS data, CSV data, and cutout images.
    ------------
    Parameters:
    - organized_fits_data: dict, a dictionary where each key is an objid and the value is another dictionary containing the organized FITS data for that objid.
    - organised_csv_data: dict, a dictionary where each key is an objid and the value is another dictionary containing the grouped CSV data for that objid.
    - science_images: dict, a dictionary where each key is an objid and the value is the science image cutout array extracted from the FITS file.
    - template_images: dict, a dictionary where each key is an objid and the value is the template image cutout array extracted from the FITS file.
    - difference_images: dict, a dictionary where each key is an objid and the value is the difference image cutout array extracted from the FITS file.

    Returns:
    - master_database: dict, a dictionary where each key is an objid and the value is another dictionary containing all combined data for that objid, including CSV metadata, FITS photometry data, and cutout images.

     The function first checks if all input dictionaries have the same keys (object IDs) and logs a warning if they do not. It then iterates through each object ID, combines the corresponding data from the FITS and CSV dictionaries, and includes the cutout images. The FITS photometry data is sorted by MJD before being added to the master database. Finally, it logs the number of objects in the created master database and returns it.
    """

def create_master_database(organized_fits_data: dict, 
                            organized_csv_data: dict, 
                            science_images: dict, 
                            template_images: dict, 
                            difference_images: dict) -> dict:
    """                        
    Create a master database by combining organized FITS data, CSV data, and cutout images.
    
    Parameters:
    - organized_fits_data: dict, organized FITS data by objid
    - organized_csv_data: dict, grouped CSV data by objid
    - science_images: dict, science image cutouts by objid
    - template_images: dict, template image cutouts by objid
    - difference_images: dict, difference image cutouts by objid

    Returns:
    - master_database: dict, combined data for all objects
    """

    # Check if all dictionaries have the same keys (object IDs)
    sizes = {
        'FITS data': len(organized_fits_data),
        'CSV data': len(organized_csv_data),
        'Science images': len(science_images),
        'Template images': len(template_images),
        'Difference images': len(difference_images)
    }
    
    if len(set(sizes.values())) == 1:
        logger.info(f"Success! All dictionaries have {list(sizes.values())[0]} objects.")
    else:
        logger.warning("Warning: Dictionary sizes don't match!")
        for name, size in sizes.items():
            logger.warning(f"  {name}: {size}")
    
    master_database = {}
    
    # Combine all data for each object
    for objid in tqdm(organized_fits_data.keys(), desc="Creating master database"):
        # Get FITS data
        fits_data = organized_fits_data[objid]
        
        # Sort by MJD
        if fits_data.get("mjds") is not None and len(fits_data["mjds"]) > 0:
            sort_idx = np.argsort(fits_data["mjds"])
            
            # Create sorted copies
            sorted_fits_data = {
                "mjds": fits_data["mjds"][sort_idx],
                "filters": fits_data["filters"][sort_idx],
                "mag_alt": fits_data["mag_alt"][sort_idx],
                "magerr_alt": fits_data["magerr_alt"][sort_idx],
                "mag_fphot": fits_data["mag_fphot"][sort_idx],
                "magerr_fphot": fits_data["magerr_fphot"][sort_idx]
            }
        else:
            sorted_fits_data = fits_data
        
        master_database[objid] = {
            # CSV data
            "ra": organized_csv_data.get(objid, {}).get("ra"),
            "dec": organized_csv_data.get(objid, {}).get("dec"),
            "num_alerts": organized_csv_data.get(objid, {}).get("num_alerts"),
            "dates": organized_csv_data.get(objid, {}).get("dates"),
            "alert_mags": organized_csv_data.get(objid, {}).get("mags"),
            "alert_magerrs": organized_csv_data.get(objid, {}).get("magerrs"),
            "alert_filters": organized_csv_data.get(objid, {}).get("filters"),
            
            # FITS photometry data (sorted by MJD)
            "mjds": sorted_fits_data.get("mjds"),
            "filters": sorted_fits_data.get("filters"),
            "mag_alt": sorted_fits_data.get("mag_alt"),
            "magerr_alt": sorted_fits_data.get("magerr_alt"),
            "mag_fphot": sorted_fits_data.get("mag_fphot"),
            "magerr_fphot": sorted_fits_data.get("magerr_fphot"),
            
            # Cutout images
            "science_image": science_images.get(objid),
            "template_image": template_images.get(objid),
            "difference_image": difference_images.get(objid)
        }
    
    logger.info(f"Master database created with {len(master_database)} objects!")
    return master_database


def save_master_database_to_hdf5(master_database: dict, output_file: str = './results/desirt_master_database.h5') -> None:
    """
    Save master database to HDF5 format.

    Parameters:
    - master_database: dict, the master database to save
    - output_file: str, path to output HDF5 file
    """
    
    # Create output directory if it doesn't exist
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Saving master database to HDF5 file: {output_file}")
    
    with h5py.File(output_file, 'w') as f:
        for objid, data in tqdm(master_database.items(), desc="Saving to HDF5"):
            # Create a group for each object
            obj_group = f.create_group(objid)
            
            # Store scalar metadata (only if not None)
            if data['ra'] is not None:
                obj_group.attrs['ra'] = data['ra']
            if data['dec'] is not None:
                obj_group.attrs['dec'] = data['dec']
            
            # Store arrays
            if data.get('mjds') is not None:
                obj_group.create_dataset('mjds', data=data['mjds'], compression='gzip')
            if data.get('filters') is not None:
                obj_group.create_dataset('filters', data=data['filters'], compression='gzip')
            if data.get('mag_alt') is not None:
                obj_group.create_dataset('mag_alt', data=data['mag_alt'], compression='gzip')
            if data.get('magerr_alt') is not None:
                obj_group.create_dataset('magerr_alt', data=data['magerr_alt'], compression='gzip')
            if data.get('mag_fphot') is not None:
                obj_group.create_dataset('mag_fphot', data=data['mag_fphot'], compression='gzip')
            if data.get('magerr_fphot') is not None:
                obj_group.create_dataset('magerr_fphot', data=data['magerr_fphot'], compression='gzip')
            
            # Store images
            if data.get('science_image') is not None:
                obj_group.create_dataset('science_image', data=data['science_image'], compression='gzip')
            if data.get('template_image') is not None:
                obj_group.create_dataset('template_image', data=data['template_image'], compression='gzip')
            if data.get('difference_image') is not None:
                obj_group.create_dataset('difference_image', data=data['difference_image'], compression='gzip')
            
            # Store nested dictionaries as JSON strings in attributes (only if not None)
            if data.get('dates') is not None:
                obj_group.attrs['dates'] = json.dumps(data['dates'])
            if data.get('alert_mags') is not None:
                obj_group.attrs['alert_mags'] = json.dumps(data['alert_mags'])
            if data.get('alert_magerrs') is not None:
                obj_group.attrs['alert_magerrs'] = json.dumps(data['alert_magerrs'])
            if data.get('alert_filters') is not None:
                obj_group.attrs['alert_filters'] = json.dumps(data['alert_filters'])
    
    logger.info(f"Database saved to {output_file}")


def load_from_hdf5(input_file: str, objids: Optional[list] = None) -> dict:
    """
    Load master database from HDF5 format.
    
    Parameters:
    - input_file: str, path to input HDF5 file
    - objids: list or None, specific objids to load (None = load all)

    Returns:
    - master_database: dict, loaded database
    """

    master_database = {}
    
    with h5py.File(input_file, 'r') as f:
        objects_to_load = objids if objids else list(f.keys())

        logger.info(f"Loading data for {len(objects_to_load)} objects from HDF5 file: {input_file}")
        
        for objid in tqdm(objects_to_load, desc="Loading objects from HDF5"):
            if objid not in f:
                continue
                
            obj_group = f[objid]
            
            master_database[objid] = {
                'ra': obj_group.attrs.get('ra'),
                'dec': obj_group.attrs.get('dec'),
                'mjds': obj_group['mjds'][:] if 'mjds' in obj_group else None,
                'filters': obj_group['filters'][:] if 'filters' in obj_group else None,
                'mag_alt': obj_group['mag_alt'][:] if 'mag_alt' in obj_group else None,
                'magerr_alt': obj_group['magerr_alt'][:] if 'magerr_alt' in obj_group else None,
                'mag_fphot': obj_group['mag_fphot'][:] if 'mag_fphot' in obj_group else None,
                'magerr_fphot': obj_group['magerr_fphot'][:] if 'magerr_fphot' in obj_group else None,
                'science_image': obj_group['science_image'][:] if 'science_image' in obj_group else None,
                'template_image': obj_group['template_image'][:] if 'template_image' in obj_group else None,
                'difference_image': obj_group['difference_image'][:] if 'difference_image' in obj_group else None,
                'dates': json.loads(obj_group.attrs.get('dates', '{}')),
                'alert_mags': json.loads(obj_group.attrs.get('alert_mags', '{}')),
                'alert_magerrs': json.loads(obj_group.attrs.get('alert_magerrs', '{}')),
                'alert_filters': json.loads(obj_group.attrs.get('alert_filters', '{}'))
            }
    
    logger.info(f"Loaded master database with {len(master_database)} objects from {input_file}")
    return master_database


def main():
    """
    Main function to orchestrate the organization of DESIRT data from CSV and FITS files.
    """

    args = argument_parser()
    
    logger.info("="*60)
    logger.info("DESIRT DATA ORGANIZATION PIPELINE STARTED (OPTIMIZED)")
    logger.info("="*60)
    
    # Step 1: Process CSV data
    logger.info("="*60)
    logger.info("\n[STEP 1/5] Processing CSV data...")
    logger.info("="*60)
    merged_csv = merge_csvs(args.csv_path)
    organized_csv_data = group_csv_data_by_objid(merged_csv)
    unique_objids = get_unique_objids(organized_csv_data)
    
    # Step 2: List FITS files
    logger.info("="*60)
    logger.info("\n[STEP 2/5] Listing FITS files...")
    logger.info("="*60)
    paths_to_all_fits_files = list_all_fits_files(args.fits_path)
    
    # Step 3: Organize FITS paths and data
    logger.info("="*60)
    logger.info("\n[STEP 3/5] Organizing FITS data...")
    logger.info("="*60)
    organized_fits_paths = organize_fits_path_by_objid(unique_objids, paths_to_all_fits_files)
    organized_fits_data = organize_fits_data(organized_fits_paths, n_workers=args.n_workers)
    
    # Step 4: Extract cutouts
    logger.info("="*60)
    logger.info("\n[STEP 4/5] Extracting cutout images...")
    logger.info("="*60)
    science_images, template_images, difference_images = get_cutouts(organized_fits_paths)
    
    # Step 5: Create master database
    logger.info("="*60)
    logger.info("\n[STEP 5/5] Creating and saving master database...")
    logger.info("="*60)
    master_database = create_master_database(organized_fits_data, organized_csv_data, 
                                             science_images, template_images, difference_images)
    save_master_database_to_hdf5(master_database, output_file=args.output_file)
    
    logger.info("="*60)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info(f"Master database saved with {len(master_database)} objects")
    logger.info("="*60)


if __name__ == "__main__":
    main()