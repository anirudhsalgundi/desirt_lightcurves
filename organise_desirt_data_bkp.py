import os
import glob
import json
import pickle
import re
import argparse
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
from typing import Dict, Tuple

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def argument_parser():
    """
    Parse command-line arguments for the DESIRT data organization script.
    Returns
    -------
    argparse.Namespace
        Parsed command-line arguments with attributes:
        - csv_path: Path to the CSV file containing list of summary CSVs
        - fits_path: Path to the text file containing list of FITS directories
    """
    parser = argparse.ArgumentParser(description="Organise DESIRT data from CSV and FITS files and create a database")
    parser.add_argument("--csv_path", type=str, required=True, help="Path to the CSV file containing list of summary CSVs")
    parser.add_argument("--fits_path", type=str, required=True, help="Path to the text file containing list of FITS directories")
    return parser.parse_args()

class DesirtCsv:
    """
    A class to process DESIRT CSV files and organize them by object_id.
    """


    def __init__(self, path_to_csv):

        """
        Initialize the DesirtCsv object.
        """

        self.path_to_csv = path_to_csv
        self.df = self._load_data()
        self.database = self._organise_csv_data()
        self.unique_objids = self._get_unique_object_ids()


    def _load_data(self) -> pl.DataFrame:

        """
        Load and concatenate all CSV files listed in the provided path.
        """

        with open(self.path_to_csv, 'r') as f:
            summary_csv_paths = [line.strip() for line in f.readlines()]

            # Use polars to read and concatenate all CSV files efficiently, polar is based on rust, and is much faster than pandas for large datasets
            df = pl.concat([pl.read_csv(f, infer_schema_length=10000) for f in summary_csv_paths], 
                            how="diagonal")
        return df


    def _organise_csv_data(self) -> dict:

        """
        Organise the loaded CSV data into a structured dictionary format grouped by object_id.
        """

        # Initialize an empty dictionary to hold the organized data
        database = {}

        for objid_tuple, obj_data in tqdm(self.df.group_by("objid"), desc="Organising CSV data"):
            objid = objid_tuple[0]  # Extract string from tuple
            
            database[objid] = {
                "ra": obj_data["ra_obj"][0],
                "dec": obj_data["dec_obj"][0],
                "num_alerts": obj_data["num_alert"].to_list(),
                "dates": {
                    "first": obj_data["date_first_alert"].to_list(),
                    "peak": obj_data["date_peak_alert"].to_list(),
                    "last": obj_data["date_last_alert"].to_list()
                },
                "mags": {
                    "first": obj_data["mag_first_alert"].to_list(),
                    "peak": obj_data["mag_peak_alert"].to_list(),
                    "last": obj_data["mag_last_alert"].to_list()
                },
                "magerrs": {
                    "first": obj_data["magerr_first_alert"].to_list(),
                    "peak": obj_data["magerr_peak_alert"].to_list(),
                    "last": obj_data["magerr_last_alert"].to_list()
                },
                "filters": {
                    "first": obj_data["filter_first_alert"].to_list(),
                    "peak": obj_data["filter_peak_alert"].to_list(),
                    "last": obj_data["filter_last_alert"].to_list()
                }
            }

        return database

    def _get_unique_object_ids(self) -> list:

        """
        Get a list of unique object IDs from the CSV data.
        """

        return self.df["objid"].unique().to_list()  

class DesirtFits:

    """
    A class to process DESIRT FITS files and organize them by object_id.
    
    Parameters
    ----------
    path_to_csvs : str or list
        Path to directory containing date folders (date1, date2, ..., date30)
        OR a list of paths to individual date folders
    
    Attributes
    ----------
    paths : list
        List of all date folder paths
    fits_files : list
        List of all FITS file paths found
    grouped_data : dict
        Dictionary with object_id as keys and organized data as values
    """
    
    def __init__(self, path_to_fits, desirt_csvs):

        """
        Initialize the DesirtFits object.
        
        Parameters
        ----------
        path_to_fits : str
            Path to the text file containing list of FITS directories
        desirt_csvs : DesirtCsv
            An instance of the DesirtCsv class. This will be called in the main function later on.
        """

        with open(path_to_fits, 'r') as f:
            fits_paths = [line.strip() for line in f.readlines()]

        self.fits_paths = fits_paths
        self.unique_objids = desirt_csvs.unique_objids  # ✓ Define FIRST
        self.master_fits_file_list = self._find_fits_files()
        self.grouped_fits_files = self._organise_fits_files()  # ✓ Now can use it
        self.grouped_data = self._get_fits_data()
    


    def _find_fits_files(self):
        master_fits_file_list = []
        for fits_path in self.fits_paths:
            fits_files = glob.glob(os.path.join(fits_path, "*.fits"))
            master_fits_file_list.extend(fits_files) 

        print(f"Found {len(master_fits_file_list)} fits files in total")

        return master_fits_file_list

    def _organise_fits_files(self):
        """
        Group FITS files by object ID (optimized version)
        """
        # Convert to set for O(1) lookup
        unique_objids_set = set(self.unique_objids)
        master_fits_file_list = self.master_fits_file_list

        grouped_fits_files = defaultdict(list)
        match = re.search(r'(DESIRT_\d+)', filename)
        objid =  match.group(1) if match else None

        # Use tqdm for progress tracking
        for fits_file in tqdm(master_fits_file_list, desc="Grouping FITS files"):
            # Extract object ID from filename
            
            # Only add if this object ID is in our CSV data
            if objid and objid in unique_objids_set:  # O(1) lookup!
                grouped_fits_files[objid].append(fits_file)
        
        return dict(grouped_fits_files)


    def _get_fits_data(self):
        grouped_data = {}
        for objid, fits_file_list in tqdm(self.grouped_fits_files.items(), desc="Reading FITS data"):
            obj_data = self._process_single_object(objid, fits_file_list)
            if obj_data:
                grouped_data[objid] = obj_data

        return grouped_data


    def _process_single_object(self, objid: str, fits_file_list: list) -> dict:
        """
        Process all FITS files for a single object
        
        Parameters
        ----------
        objid : str
            Object identifier
        fits_file_list : list
            List of FITS file paths for this object
        
        Returns
        -------
        dict or None
            Dictionary containing combined photometry data, or None if no valid data
        """
        # Initialize storage for this object
        obj_data = {
            'mjd': [],
            'filter': [],
            'mag_alt': [],
            'magerr_alt': [],
            'mag_fphot': [],
            'magerr_fphot': [],
            'snr_alt': [], 
            'snr_fphot': [] 
        }
        
        # Process each FITS file
        for fits_file in fits_file_list:
            try:
                with fits.open(fits_file) as hdul:
                    # Usually data is in extension 1
                    if len(hdul) > 1 and hdul[1].data is not None:
                        data = hdul[1].data
                        
                        # Check if required columns exist
                        required_cols = ['MJD_OBS', 'FILTER', "MAG_ALT", "MAGERR_ALT", "MAG_FPHOT", "MAGERR_FPHOT"]
                        if not all(col in data.dtype.names for col in required_cols):
                            logger.warning(f"Missing required columns in {Path(fits_file).name}")
                            continue
                        
                        # Extract data
                        obj_data['mjd'].append(data['MJD_OBS'])
                        
                        # Handle filter data (might be bytes)
                        filters = data['FILTER']
                        if isinstance(filters[0], bytes):
                            filters = np.array([f.decode('utf-8').strip() for f in filters])
                        obj_data['filter'].append(filters)
                        
                        # photometry
                        if 'MAG_ALT' in data.dtype.names:
                            obj_data['mag_alt'].append(data['MAG_ALT'])
                            obj_data['magerr_alt'].append(data.get('MAGERR_ALT', np.full_like(data['MAG_ALT'], np.nan)))
                            obj_data['snr_alt'].append(data.get('SNR_ALT', np.full_like(data['MAG_ALT'], np.nan)))
                        
                        if 'MAG_FPHOT' in data.dtype.names:
                            obj_data['mag_fphot'].append(data['MAG_FPHOT'])
                            obj_data['magerr_fphot'].append(data.get('MAGERR_FPHOT', np.full_like(data['MAG_FPHOT'], np.nan)))
                            obj_data['snr_fphot'].append(data.get('SNR_FPHOT', np.full_like(data['MAG_FPHOT'], np.nan)))
                            
            except Exception as e:
                continue
        
        # Concatenate all arrays
        if obj_data['mjd']:
            for key in obj_data.keys():
                if obj_data[key]:
                    obj_data[key] = np.concatenate(obj_data[key])
                else:
                    obj_data[key] = np.array([])
            
            # Sort by MJD
            sort_idx = np.argsort(obj_data['mjd'])
            for key in obj_data.keys():
                if len(obj_data[key]) > 0:
                    obj_data[key] = obj_data[key][sort_idx]
            
            return obj_data
        
        return None

def save_to_pickle(self, output_file: str):
    """Save database to pickle file"""
    print(f"Saving CSV data to {output_file}...")
    with open(output_file, 'wb') as f:
        pickle.dump(self.database, f, protocol=pickle.HIGHEST_PROTOCOL)
    print("✓ CSV data saved successfully!")

def organize_desirt_data(csv_path: str, fits_path: str, output_dir: str = ".") -> Tuple[Dict, Dict]:
    """
    Organize DESIRT CSV and FITS data into structured databases
    
    Parameters
    ----------
    csv_path : str
        Path to file containing list of CSV files
    fits_path : str
        Path to file containing list of FITS directories
    output_dir : str
        Directory to save output files
    
    Returns
    -------
    tuple
        (csv_database, fits_database)
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    

    desirt_csv = DesirtCsv(csv_path)
    desirt_fits = DesirtFits(fits_path, desirt_csv)
    
    # Step 3: Save data
    csv_output = output_path / "desirt_csv_data.pkl"
    fits_output = output_path / "desirt_grouped_data.pkl"
    
    save_to_pickle(str(csv_output))
    save_to_pickle(str(fits_output))
    
    return desirt_csv.database, desirt_fits.grouped_data

def main():
    args = argument_parser()
    organize_desirt_data(args.csv_path, args.fits_path)

    #plot_data()
    #make_summary()
    
if __name__ == "__main__":
    main()