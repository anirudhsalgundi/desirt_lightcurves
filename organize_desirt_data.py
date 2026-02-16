import os
import glob
import json  # Move here
import pickle
import re
import argparse
import h5py
from tqdm import tqdm 
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import numpy as np
import polars as pl
import matplotlib.pyplot as plt

from astropy.io import fits
from astropy.time import Time
from astropy.coordinates import SkyCoord
from astropy.table import vstack
import astropy.units as u
from typing import Dict, Tuple, List

import logging
from datetime import datetime

# Create logs directory
LOGSDIR = Path("./logs")
LOGSDIR.mkdir(exist_ok=True)

log_filename = LOGSDIR / f"log_from_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Configure logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),  # Write to file
        logging.StreamHandler()              # Print to console
    ]
)
logger = logging.getLogger(__name__)

# Log the log file location at the start
logger.info(f"Logging to: {log_filename}")

def argument_parser() -> argparse.Namespace:

    """
    Parse command-line arguments for the DESIRT data organization script.
    """
    parser = argparse.ArgumentParser(description="Organise DESIRT data from CSV and FITS files and create a database")
    parser.add_argument("--csv_path", type=str, required=True, help="Path to the CSV file containing list of summary CSVs")
    parser.add_argument("--fits_path", type=str, required=True, help="Path to the text file containing list of FITS directories")

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
        # Scan more rows to infer correct type

        # log here
        logger.info(f"Read {len(summary_csv_paths)} CSV files")
        merged_csv = pl.concat([pl.read_csv(f, infer_schema_length=10000) for f in summary_csv_paths], how="diagonal") 

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

    grouped_data = defaultdict(dict)

    for row in merged_csv.iter_rows(named=True):
        objid = row["objid"]
        grouped_data[objid].update(row)

    logger.info(f"Grouped CSV data by objid")
    return grouped_data


def get_unique_objids(organised_csv_data) -> list:

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


def list_all_fits_files(file_with_path_to_fits_files) -> list:

    """
    List all FITS files from the directories specified in a text file.
    ------------
    Parameters:
    - file_with_path_to_fits_files: str, path to a text file where each line is a path to a directory containing FITS files. 

    The function will read this file, iterate through each directory, and collect the paths of all FITS files found in those directories. It returns a list of paths to all the FITS files. The function also includes logging to indicate how many FITS files were found in total and prints a warning if any specified directory is not found.

    Returns:
    - paths_to_all_fits_files: list, a list of strings where each string is the path to a FITS file found in the specified directories.

    """

    # Initialize an empty list to hold the paths to all FITS files
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


def organize_fits_path_by_objid(unique_objects, paths_to_all_fits_files) -> dict:

    """
    Organize FITS file paths by object ID (objid).
    ------------
    Parameters:
    - unique_objects: list, a list of unique object IDs (objids) for which we want to organize the FITS file paths.

    Returns:
    - organised_fits_paths: dict, a dictionary where each key is an objid and the value is a list of paths to FITS files that contain that objid in their filename. 
    
    The function iterates through each unique object ID and filters the list of all FITS file paths to find those that contain the object ID in their filename. It uses tqdm to provide a progress bar for the organization process. Only object IDs that have corresponding FITS files will be included in the returned dictionary.
    """

    organised_fits_paths = {}

    for unique_object in tqdm(unique_objects, desc="Organizing FITS paths by objid"):
        # Filter FITS files that contain the unique_object ID in their filename
        fits_files_of_unique_obj = [
            fits_file for fits_file in paths_to_all_fits_files 
            if unique_object in Path(fits_file).name
        ]
        
        if fits_files_of_unique_obj:  # Only add if files were found
            organised_fits_paths[unique_object] = fits_files_of_unique_obj

    logger.info(f"Organized FITS paths for {len(organised_fits_paths)} unique objects")
    return organised_fits_paths


def organize_fits_data(organised_fits_paths) -> dict:

    """
    Organize FITS data by object ID (objid).
    ------------
    Parameters:
    - organised_fits_paths: dict, a dictionary where each key is an objid and the value is a list of paths to FITS files that contain that objid in their filename.

    Returns:
    - organised_fits_data: dict, a dictionary where each key is an objid and the value is another dictionary containing the organized FITS data for that objid, including arrays of MJD observations, filters, magnitudes and their errors for both aperture photometry (ALT) and forced photometry (FPHOT). 
    
    
    The function reads each FITS file for a given objid, extracts the relevant data, and concatenates it across all FITS files to create a comprehensive dataset for each object. It also includes error handling to skip files that cannot be read or do not contain the expected data, and logs warnings for any issues encountered during the process.
    """

    organised_fits_data = {}

    for objid, fits_files_list in tqdm(organised_fits_paths.items(), desc="Organizing FITS data by objid"):
        
        # Initialize lists to collect data from all FITS files for this object
        all_mjds = []
        all_filters = []
        all_mag_alt = []
        all_magerr_alt = []
        all_mag_fphot = []
        all_magerr_fphot = []
        
        # Loop through all FITS files for this object
        for fits_file in fits_files_list:
            try:
                with fits.open(fits_file) as hdul:
                    # Check if extension 1 exists and has data
                    if len(hdul) < 2 or hdul[1].data is None:
                        print(f"Warning: No data in extension 1 for {fits_file}")
                        continue
                    
                    data = hdul[1].data
                    
                    # Check if required columns exist
                    required_cols = ['MJD_OBS', 'FILTER', 'MAG_ALT', 'MAGERR_ALT', 'MAG_FPHOT', 'MAGERR_FPHOT']
                    if not all(col in data.dtype.names for col in required_cols):
                        print(f"Warning: Missing columns in {fits_file}")
                        continue
                    
                    # Collect data from this FITS file
                    all_mjds.append(data['MJD_OBS'])
                    all_filters.append(data['FILTER'])
                    all_mag_alt.append(data['MAG_ALT'])
                    all_magerr_alt.append(data['MAGERR_ALT'])
                    all_mag_fphot.append(data['MAG_FPHOT'])
                    all_magerr_fphot.append(data['MAGERR_FPHOT'])
                    
            except Exception as e:
                logger.error(f"Error reading {fits_file}: {e}")
                continue
        
        # Only add to database if we successfully read data
        if all_mjds:
            try:
                # Concatenate all arrays and store for this object
                organised_fits_data[objid] = {
                    "mjds": np.concatenate(all_mjds),
                    "filters": np.concatenate(all_filters),
                    "mag_alt": np.concatenate(all_mag_alt),
                    "magerr_alt": np.concatenate(all_magerr_alt),
                    "mag_fphot": np.concatenate(all_mag_fphot),
                    "magerr_fphot": np.concatenate(all_magerr_fphot)
                }
            except Exception as e:
                logger.error(f"Error concatenating data for {objid}: {e}")
                continue
        else:
            logger.warning(f"Warning: No valid data found for {objid}")

    # log here
    logger.info(f"Organized FITS data for {len(organised_fits_data)} objects")
    return organised_fits_data


def get_cutouts(organised_fits_paths) -> dict:

    """
    Get cutout images (science, template, difference) for each object ID (objid) from the FITS files.
    ------------
    Parameters:
    - organised_fits_paths: dict, a dictionary where each key is an objid and the value is a list of paths to FITS files that contain that objid in their filename.

    Returns:
    - science_images: dict, a dictionary where each key is an objid and the value is the science image cutout array extracted from the FITS file.

    - template_images: dict, a dictionary where each key is an objid and the value is the template image cutout array extracted from the FITS file.

    - difference_images: dict, a dictionary where each key is an objid and the value is the difference image cutout array extracted from the FITS file.

    The function reads the last FITS file for each objid (assuming it contains the most recent cutouts), extracts the science, template, and difference image cutouts from the specified columns in the FITS data, and stores them in separate dictionaries. It includes error handling to skip files that cannot be read or do not contain the expected data, and logs any issues encountered during the process.
    """

    science_images, template_images, difference_images = {}, {}, {}

    for objid, fits_files_list in tqdm(organised_fits_paths.items(), desc="Reading FITS files"):
        last_fits_file = fits_files_list[-1]
        
        try:
            with fits.open(last_fits_file) as hdul:
                data = hdul[1].data

                science_images[objid] = data["PixA_THUMB_SCI"]
                template_images[objid] = data["PixA_THUMB_TEMP"]
                difference_images[objid] = data["PixA_THUMB_DIFF"]
                    
        except Exception as e:
            logger.error(f"Error reading cutouts for {objid} from {last_fits_file}: {e}")
            continue

    logger.info(f"Extracted cutout images for {len(science_images)} objects")
    return science_images, template_images, difference_images


def create_master_database(organized_fits_data, 
                            organised_csv_data, 
                            science_images, 
                            template_images, 
                            difference_images) -> dict:

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

    # Check if all dictionaries have the same keys (object IDs)
    if len(organized_fits_data.keys()) == len(organised_csv_data.keys()) == len(science_images.keys()) == len(template_images.keys()) == len(difference_images.keys()):
        logger.info("Success! All dictionaries have the same number of objects.")
    else:
        logger.warning(f"Warning: Dictionary sizes don't match!")
        logger.warning(f"  FITS data: {len(organized_fits_data.keys())}")
        logger.warning(f"  CSV data: {len(organised_csv_data.keys())}")
        logger.warning(f"  Science images: {len(science_images.keys())}")
        logger.warning(f"  Template images: {len(template_images.keys())}")
        logger.warning(f"  Difference images: {len(difference_images.keys())}")
    
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
            "ra": organised_csv_data.get(objid, {}).get("ra"),
            "dec": organised_csv_data.get(objid, {}).get("dec"),
            "num_alerts": organised_csv_data.get(objid, {}).get("num_alerts"),
            "dates": organised_csv_data.get(objid, {}).get("dates"),
            "alert_mags": organised_csv_data.get(objid, {}).get("mags"),
            "alert_magerrs": organised_csv_data.get(objid, {}).get("magerrs"),
            "alert_filters": organised_csv_data.get(objid, {}).get("filters"),
            
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


def save_master_database_to_hdf5(master_database, output_file='./results/desirt_master_database.h5') -> None:

    """
    Save master database to HDF5 format.

    Parameters:
    - master_database: dict, a dictionary where each key is an objid and the value is another dictionary containing all combined data for that objid, including CSV metadata, FITS photometry data, and cutout images.
    - output_file: str, the path to the output HDF5 file where the master database will be saved. Default is './results/desirt_master_database.h5'.

    Returns:
    - organised_csv_data: dict, a dictionary where each key is an objid and the value is another dictionary containing the grouped data for that objid, including ra, dec, number of alerts, dates of first/peak/last alerts, magnitudes and their errors for first/peak/last alerts, and filters used for first/peak/last alerts.

    The function creates an HDF5 file and organizes the data by object ID (objid). For each objid, it creates a group in the HDF5 file and stores scalar metadata as attributes, arrays as datasets with gzip compression, and nested dictionaries as JSON strings in attributes. It also ensures that the output directory exists before saving the file. Finally, it logs a message indicating that the database has been successfully saved to the specified location.
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


def load_from_hdf5(input_file, objids=None) -> dict:

    """
    Load master database from HDF5 format.
    Parameters:
    - input_file: str, the path to the input HDF5 file from which the master database will be loaded.
    - objids: list or None, a list of object IDs (objids) to load from the HDF5 file. If None, all objids in the file will be loaded. Default is None.

    Returns:
    - master_database: dict, a dictionary where each key is an objid and the value is another dictionary containing all combined data for that objid, including CSV metadata, FITS photometry data, and cutout images.

    The function reads the specified HDF5 file, iterates through the groups corresponding to each objid, and extracts the stored data. It uses .get() to safely access attributes and datasets, providing default values if they are not present. The extracted data is organized into a dictionary format similar to the original master database structure. If objids are specified, only those will be loaded; otherwise, all objids in the file will be included in the returned dictionary.
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
                'ra': obj_group.attrs.get('ra'),  # Use .get() instead of ['ra']
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
                'dates': json.loads(obj_group.attrs.get('dates', '{}')),  # Provide default
                'alert_mags': json.loads(obj_group.attrs.get('alert_mags', '{}')),
                'alert_magerrs': json.loads(obj_group.attrs.get('alert_magerrs', '{}')),
                'alert_filters': json.loads(obj_group.attrs.get('alert_filters', '{}'))
            }
    
    logger.info(f"Loaded master database with {len(master_database)} objects from {input_file}")
    return master_database


def main():
    """
    Main function to orchestrate the organization of DESIRT data from CSV and FITS files and create a master database.
    """

    args = argument_parser()
    
    logger.info("="*60)
    logger.info("DESIRT DATA ORGANIZATION PIPELINE STARTED")
    logger.info("="*60)
    
    # Step 1: Process CSV data
    logger.info("\n[STEP 1/5] Processing CSV data...")
    merged_csv = merge_csvs(args.csv_path)
    organised_csv_data = group_csv_data_by_objid(merged_csv)
    unique_objids = get_unique_objids(organised_csv_data)
    
    # Step 2: List FITS files
    logger.info("\n[STEP 2/5] Listing FITS files...")
    paths_to_all_fits_files = list_all_fits_files(args.fits_path)
    
    # Step 3: Organize FITS paths and data
    logger.info("\n[STEP 3/5] Organizing FITS data...")
    organised_fits_paths = organize_fits_path_by_objid(unique_objids, paths_to_all_fits_files)
    organised_fits_data = organize_fits_data(organised_fits_paths)
    
    # Step 4: Extract cutouts
    logger.info("\n[STEP 4/5] Extracting cutout images...")
    science_images, template_images, difference_images = get_cutouts(organised_fits_paths)
    
    # Step 5: Create master database
    logger.info("\n[STEP 5/5] Creating and saving master database...")
    master_database = create_master_database(organised_fits_data, organised_csv_data, 
                                             science_images, template_images, difference_images)
    save_master_database_to_hdf5(master_database, output_file='./results/desirt_master_database.h5')
    
    logger.info("="*60)
    logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info(f"Master database saved with {len(master_database)} objects")
    logger.info("="*60)


if __name__ == "__main__":
    main()
