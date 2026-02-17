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
from typing import Dict, Tuple, List, Optional, Union

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
    parser.add_argument("--data", type=str, required=True, help="Path to the text file containing list of FITS directories")
    parser.add_argument("--n_workers", type=int, default=None, help="Number of parallel workers (default: CPU count - 1)")
    parser.add_argument("--batch_size", type=int, default=1000, required=False, help="Number of FITS files to process in each batch. Default is 1000.")

    return parser.parse_args()


def extract_objid_from_fits_path(fits_path: str) -> Optional[str]:
    """
    Extract objid from FITS filename.
    Example: 3_T202504071346367m032044.fits -> T202504071346367m032044
    """
    filename = Path(fits_path).name
    # Match pattern: N_objid.fits where N is a number
    match = re.match(r'^\d+_([A-Z]\d+[mp]\d+)\.fits$', filename)
    if match:
        return match.group(1)
    return None


def get_unique_objids(paths_to_all_fits_files: str) -> list:
    """
    Read FITS file paths and extract unique object IDs.
    
    Parameters:
    - paths_to_all_fits_files: str, path to text file with FITS paths
    
    Returns:
    - list of unique objids (sorted)
    """
    unique_objids = set()
    
    with open(paths_to_all_fits_files, 'r') as f:
        for line in f:
            fits_path = line.strip()
            if fits_path:
                objid = extract_objid_from_fits_path(fits_path)
                if objid:
                    unique_objids.add(objid)
    
    # Return sorted list for reproducibility
    return sorted(unique_objids)


def read_all_fits_files_to_temp(paths_to_all_fits_files: str, 
                                 temp_file: str = "temp_fits_data.h5",
                                 batch_size: int = 1000,
                                 n_workers: Optional[int] = None) -> str:
    """
    Read all FITS files and write to temporary HDF5 file to save memory.
    
    Parameters:
    - paths_to_all_fits_files: str, path to text file with FITS paths
    - temp_file: str, path to temporary HDF5 file
    - n_workers: int, number of parallel workers
    
    Returns:
    - temp_file: str, path to temporary file containing all FITS data
    """
    
    if n_workers is None:
        n_workers = max(1, cpu_count() - 1)
    
    # Read file list
    with open(paths_to_all_fits_files, 'r') as f:
        fits_files = [line.strip() for line in f if line.strip()]
    
    logger.info(f"Reading {len(fits_files)} FITS files with {n_workers} workers")
    logger.info(f"Writing to temporary file: {temp_file}")
    
    # Create/overwrite temp HDF5 file
    with h5py.File(temp_file, 'w') as f:
        pass  # Just create empty file
    
    # Process in batches to control memory
    batch_size = int(batch_size)
    num_batches = (len(fits_files) + batch_size - 1) // batch_size
    
    for batch_idx in range(num_batches):
        logger.info(f"Processing batch {batch_idx + 1}/{num_batches} (files {batch_idx * batch_size} to {min((batch_idx + 1) * batch_size, len(fits_files)) - 1})")
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(fits_files))
        batch_files = fits_files[start_idx:end_idx]
        
        # Read batch in parallel
        with Pool(n_workers) as pool:
            results = pool.map(_read_fits_file_data, batch_files, chunksize=10)
        
        # Write batch to temp file
        with h5py.File(temp_file, 'a') as f:
            for result in tqdm(results, desc=f"Processing fits files in batch {batch_idx + 1}/{num_batches}"):
                if result is None:
                    continue
                
                objid = result['objid']
                
                # Create or append to objid group
                if objid in f:
                    # Append to existing group
                    grp = f[objid]
                    for key in ['mjds', 'filters', 'mag_alt', 'magerr_alt', 'mag_fphot', 'magerr_fphot', 'cutout_science', 'cutout_template', 'cutout_difference']:
                        old_data = grp[key][:]
                        new_data = np.concatenate([old_data, result[key]])
                        del grp[key]
                        grp.create_dataset(key, data=new_data, compression='gzip')
                else:
                    # Create new group
                    grp = f.create_group(objid)
                    for key in ['mjds', 'filters', 'mag_alt', 'magerr_alt', 'mag_fphot', 'magerr_fphot', 'cutout_science', 'cutout_template', 'cutout_difference']:
                        grp.create_dataset(key, data=result[key], compression='gzip')
    
    logger.info(f"All FITS data written to {temp_file}")
    return temp_file


def _read_fits_file_data(fits_file: str) -> Optional[dict]:
    """
    Read a single FITS file and extract objid from filename.
    
    Parameters:
    - fits_file: str, path to FITS file
    
    Returns:
    - dict with objid and data, or None if reading failed
    """
    
    # Extract objid from filename
    objid = extract_objid_from_fits_path(fits_file)
    if objid is None:
        return None
    
    try:
        with fits.open(fits_file, memmap=True) as hdul:
            if len(hdul) < 2 or hdul[1].data is None:
                return None
            
            data = hdul[1].data
            
            # Check required columns
            if not all(col in data.dtype.names for col in REQUIRED_FITS_COLS):
                return None

            # Convert filters to byte strings for HDF5 compatibility
            filters = np.array(data['FILTER'])
            if filters.dtype.kind == 'U':  # Unicode string
                filters = filters.astype('S')  # Convert to byte string
                        
            return {
                'objid': objid,
                'mjds': np.array(data['MJD_OBS']),
                'filters': filters,
                'mag_alt': np.array(data['MAG_ALT']),
                'magerr_alt': np.array(data['MAGERR_ALT']),
                'mag_fphot': np.array(data['MAG_FPHOT']),
                'magerr_fphot': np.array(data['MAGERR_FPHOT']),
                'cutout_science': np.array(data['PixA_THUMB_SCI']),
                'cutout_template': np.array(data['PixA_THUMB_TEMP']),
                'cutout_difference': np.array(data['PixA_THUMB_DIFF'])
            }
                
    except Exception as e:
        logger.error(f"Error reading {fits_file}: {e}")
        return None


def main():
    """
    Main function - memory-efficient version using temporary files.
    """
    
    args = argument_parser()
    
    logger.info("="*60)
    logger.info("DESIRT DATA ORGANIZATION PIPELINE (MEMORY-EFFICIENT)")
    logger.info("="*60)
    
    # Step 1: Read all FITS files and write to temp file
    logger.info("="*60)
    logger.info("\n[STEP 1/2] Reading FITS files to temporary storage...")
    logger.info("="*60)
    temp_file = "./temp_fits_data.h5"
    read_all_fits_files_to_temp(args.data, temp_file=temp_file, batch_size = args.batch_size, n_workers=args.n_workers)
    
    # Step 2: Load, sort, and save to final database
    logger.info("="*60)
    logger.info("\n[STEP 2/2] Creating final database from temporary file...")
    logger.info("="*60)
    
    # Create output file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'./results/desirt_master_database_{timestamp}.h5'
    Path("./results").mkdir(exist_ok=True)
    
    # Read from temp and write to final (with sorting)
    with h5py.File(temp_file, 'r') as f_in:
        with h5py.File(output_file, 'w') as f_out:
            objids = list(f_in.keys())
            logger.info(f"Processing {len(objids)} unique objects")
            
            for objid in tqdm(objids, desc="Sorting and saving final data"):
                grp_in = f_in[objid]
                grp_out = f_out.create_group(objid)
                
                # Load data
                mjds = grp_in['mjds'][:]
                filters = grp_in['filters'][:]
                mag_alt = grp_in['mag_alt'][:]
                magerr_alt = grp_in['magerr_alt'][:]
                mag_fphot = grp_in['mag_fphot'][:]
                magerr_fphot = grp_in['magerr_fphot'][:]
                cutout_science = grp_in['cutout_science'][:]
                cutout_template = grp_in['cutout_template'][:]
                cutout_difference = grp_in['cutout_difference'][:]
                
                # Sort by MJD
                sort_idx = np.argsort(mjds)
                
                # Save sorted data to final file
                grp_out.create_dataset('mjds', data=mjds[sort_idx], compression='gzip')
                
                # Handle filters (convert to byte strings if needed)
                filters_sorted = filters[sort_idx]
                if filters_sorted.dtype.kind == 'U':
                    filters_sorted = filters_sorted.astype('S')
                grp_out.create_dataset('filters', data=filters_sorted, compression='gzip')
                
                grp_out.create_dataset('mag_alt', data=mag_alt[sort_idx], compression='gzip')
                grp_out.create_dataset('magerr_alt', data=magerr_alt[sort_idx], compression='gzip')
                grp_out.create_dataset('mag_fphot', data=mag_fphot[sort_idx], compression='gzip')
                grp_out.create_dataset('magerr_fphot', data=magerr_fphot[sort_idx], compression='gzip')
                grp_out.create_dataset('science_image', data=cutout_science[sort_idx], compression='gzip')
                grp_out.create_dataset('template_image', data=cutout_template[sort_idx], compression='gzip')
                grp_out.create_dataset('difference_image', data=cutout_difference[sort_idx], compression='gzip')
    
    # Cleanup temp file
    logger.info(f"Removing temporary file: {temp_file}")
    Path(temp_file).unlink(missing_ok=True)
    
    logger.info("="*60)
    logger.info("PIPELINE COMPLETED!")
    logger.info(f"Processed {len(objids)} objects")
    logger.info(f"Output: {output_file}")
    logger.info("="*60)

if __name__ == "__main__":
    main()