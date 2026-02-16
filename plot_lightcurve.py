#!/usr/bin/env python3
"""
Plot DESIRT Lightcurves

This script loads the pickle files created by organize_desirt_data.py and provides
functions to visualize lightcurves from both CSV and FITS data.

Usage:
    python plot_lightcurve.py --csv_pkl desirt_csv_data.pkl --fits_pkl desirt_grouped_data.pkl --objid <object_id>
    python plot_lightcurve.py --csv_pkl desirt_csv_data.pkl --fits_pkl desirt_grouped_data.pkl --plot_all --output_dir plots/
"""

import os
import pickle
import argparse
from pathlib import Path
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
from astropy.time import Time

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_pickle(filepath):
    """Load data from pickle file"""
    logger.info(f"Loading data from {filepath}...")
    with open(filepath, 'rb') as f:
        data = pickle.load(f)
    logger.info(f"✓ Loaded {len(data)} objects")
    return data


def inspect_pickle(filepath):
    """Quick inspection of pickle file contents"""
    data = load_pickle(filepath)
    
    print(f"\nType: {type(data)}")
    print(f"Number of objects: {len(data) if hasattr(data, '__len__') else 'N/A'}")
    
    if isinstance(data, dict):
        print(f"\nFirst 10 object IDs: {list(data.keys())[:10]}")
        print(f"\nSample entry structure:")
        sample_key = list(data.keys())[0]
        print(f"  Object ID: {sample_key}")
        sample_data = data[sample_key]
        if isinstance(sample_data, dict):
            for key, value in sample_data.items():
                if isinstance(value, np.ndarray):
                    print(f"    {key}: array of shape {value.shape}")
                elif isinstance(value, dict):
                    print(f"    {key}: dict with keys {list(value.keys())}")
                else:
                    print(f"    {key}: {type(value)}")
    
    return data


def plot_lightcurve_csv(obj_data, objid, save_path=None):
    """
    Plot lightcurve from CSV summary data
    
    Parameters:
    -----------
    obj_data : dict
        Dictionary containing the object's CSV data with dates, mags, magerrs, filters
    objid : str
        Object identifier for the title
    save_path : str, optional
        Path to save the figure
    """
    
    # Extract data
    dates_first = obj_data['dates']['first']
    dates_peak = obj_data['dates']['peak']
    dates_last = obj_data['dates']['last']
    
    mags_first = obj_data['mags']['first']
    mags_peak = obj_data['mags']['peak']
    mags_last = obj_data['mags']['last']
    
    errors_first = obj_data['magerrs']['first']
    errors_peak = obj_data['magerrs']['peak']
    errors_last = obj_data['magerrs']['last']
    
    filters_first = obj_data['filters']['first']
    filters_peak = obj_data['filters']['peak']
    filters_last = obj_data['filters']['last']
    
    # Combine all data
    all_dates = dates_first + dates_peak + dates_last
    all_mags = mags_first + mags_peak + mags_last
    all_errors = errors_first + errors_peak + errors_last
    all_filters = filters_first + filters_peak + filters_last
    
    # Convert dates to datetime if they're strings
    if isinstance(all_dates[0], str):
        all_dates = [datetime.fromisoformat(d.replace('Z', '+00:00')) for d in all_dates]
    
    # Filter color mapping (common astronomical filters)
    filter_colors = {
        'g': 'green',
        'r': 'red',
        'i': 'orange',
        'z': 'purple',
        'u': 'blue',
        'Y': 'brown',
        'B': 'blue',
        'V': 'green',
        'R': 'red',
        'I': 'orange'
    }
    
    # Create plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Plot by filter
    unique_filters = set(all_filters)
    for filt in unique_filters:
        # Get indices for this filter
        indices = [i for i, f in enumerate(all_filters) if f == filt]
        
        dates_filt = [all_dates[i] for i in indices]
        mags_filt = [float(all_mags[i]) for i in indices]
        errors_filt = [float(all_errors[i]) for i in indices]
        
        color = filter_colors.get(filt, 'gray')
        
        ax.errorbar(dates_filt, mags_filt, yerr=errors_filt,
                   fmt='o', color=color, label=f'Filter {filt}',
                   markersize=8, capsize=3, alpha=0.7,
                   markeredgecolor='black', markeredgewidth=0.5)
    
    # Formatting
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Magnitude', fontsize=12)
    ax.set_title(f'Lightcurve (CSV Summary) for: {objid}', fontsize=14, fontweight='bold')
    ax.invert_yaxis()  # Magnitudes are inverted (brighter = lower mag)
    ax.legend(loc='best')
    ax.grid(True, alpha=0.3)
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        logger.info(f"Saved plot to {save_path}")
    else:
        plt.show()
    
    plt.close()


def plot_lightcurve_fits(fits_data, objid, save_path=None):
    """
    Plot lightcurve from FITS data with different markers for normal and forced photometry
    
    Parameters:
    -----------
    fits_data : dict
        Dictionary containing MJD, FILTER, MAG_ALT, MAGERR_ALT, MAG_FPHOT, MAGERR_FPHOT
    objid : str
        Object identifier for the title
    save_path : str, optional
        Path to save the figure
    """
    
    # Extract data
    mjds = fits_data['mjd']
    filters = fits_data['filter']
    mag_alt = fits_data['mag_alt']
    error_alt = fits_data['magerr_alt']
    mag_fphot = fits_data['mag_fphot']
    error_fphot = fits_data['magerr_fphot']
    
    # Filter color mapping
    filter_colors = {
        'g': 'green',
        'r': 'red',
        'i': 'orange',
        'z': 'purple',
        'u': 'blue',
        'Y': 'brown',
        'B': 'blue',
        'V': 'green',
        'R': 'red',
        'I': 'orange'
    }
    
    # Create plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Get unique filters
    unique_filters = np.unique(filters)
    
    for filt in unique_filters:
        # Get indices for this filter
        mask = filters == filt
        mjds_filt = mjds[mask]
        mag_alt_filt = mag_alt[mask]
        error_alt_filt = error_alt[mask]
        mag_fphot_filt = mag_fphot[mask]
        error_fphot_filt = error_fphot[mask]
        
        color = filter_colors.get(filt, 'gray')
        
        # Plot normal photometry (circles)
        # Filter out NaN/invalid/999 values
        valid_alt = (mag_alt_filt != 999) & ~np.isnan(mag_alt_filt) & ~np.isnan(error_alt_filt) & (mag_alt_filt > 0)
        if np.any(valid_alt):
            ax.errorbar(mjds_filt[valid_alt], mag_alt_filt[valid_alt], 
                       yerr=error_alt_filt[valid_alt],
                       fmt='o', color=color, label=f'{filt} (normal)',
                       markersize=8, capsize=3, alpha=0.7, 
                       markeredgecolor='black', markeredgewidth=0.5)
        
        # Plot forced photometry (squares)
        valid_fphot = (mag_fphot_filt != 999) & ~np.isnan(mag_fphot_filt) & ~np.isnan(error_fphot_filt) & (mag_fphot_filt > 0)
        if np.any(valid_fphot):
            ax.errorbar(mjds_filt[valid_fphot], mag_fphot_filt[valid_fphot],
                       yerr=error_fphot_filt[valid_fphot],
                       fmt='s', color=color, label=f'{filt} (forced)',
                       markersize=8, capsize=3, alpha=0.7,
                       markeredgecolor='black', markeredgewidth=0.5)
    
    # Convert MJD to dates for display
    if len(mjds) > 0:
        min_mjd = np.min(mjds)
        max_mjd = np.max(mjds)
        min_date = Time(min_mjd, format='mjd').iso[:10]
        max_date = Time(max_mjd, format='mjd').iso[:10]
        date_range = f"{min_date} to {max_date}"
    else:
        date_range = "No data"
    
    # Formatting
    ax.set_xlabel('MJD', fontsize=12)
    ax.set_ylabel('Magnitude', fontsize=12)
    ax.set_title(f'Lightcurve (FITS) for: {objid}\n{date_range}', fontsize=14, fontweight='bold')
    ax.invert_yaxis()  # Magnitudes are inverted
    ax.legend(loc='best', fontsize=9, ncol=2)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        logger.info(f"Saved plot to {save_path}")
    else:
        plt.show()
    
    plt.close()


def plot_combined_lightcurve(csv_data, fits_data, objid, save_path=None):
    """
    Plot both CSV and FITS lightcurves in subplots
    
    Parameters:
    -----------
    csv_data : dict
        CSV data for the object
    fits_data : dict
        FITS data for the object
    objid : str
        Object identifier
    save_path : str, optional
        Path to save the figure
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # --- Plot CSV data ---
    dates_first = csv_data['dates']['first']
    dates_peak = csv_data['dates']['peak']
    dates_last = csv_data['dates']['last']
    
    mags_first = csv_data['mags']['first']
    mags_peak = csv_data['mags']['peak']
    mags_last = csv_data['mags']['last']
    
    errors_first = csv_data['magerrs']['first']
    errors_peak = csv_data['magerrs']['peak']
    errors_last = csv_data['magerrs']['last']
    
    filters_first = csv_data['filters']['first']
    filters_peak = csv_data['filters']['peak']
    filters_last = csv_data['filters']['last']
    
    all_dates = dates_first + dates_peak + dates_last
    all_mags = mags_first + mags_peak + mags_last
    all_errors = errors_first + errors_peak + errors_last
    all_filters = filters_first + filters_peak + filters_last
    
    if isinstance(all_dates[0], str):
        all_dates = [datetime.fromisoformat(d.replace('Z', '+00:00')) for d in all_dates]
    
    filter_colors = {
        'g': 'green', 'r': 'red', 'i': 'orange', 'z': 'purple',
        'u': 'blue', 'Y': 'brown', 'B': 'blue', 'V': 'green',
        'R': 'red', 'I': 'orange'
    }
    
    unique_filters = set(all_filters)
    for filt in unique_filters:
        indices = [i for i, f in enumerate(all_filters) if f == filt]
        dates_filt = [all_dates[i] for i in indices]
        mags_filt = [float(all_mags[i]) for i in indices]
        errors_filt = [float(all_errors[i]) for i in indices]
        color = filter_colors.get(filt, 'gray')
        
        ax1.errorbar(dates_filt, mags_filt, yerr=errors_filt,
                    fmt='o', color=color, label=f'Filter {filt}',
                    markersize=8, capsize=3, alpha=0.7,
                    markeredgecolor='black', markeredgewidth=0.5)
    
    ax1.set_xlabel('Date', fontsize=11)
    ax1.set_ylabel('Magnitude', fontsize=11)
    ax1.set_title('CSV Summary Data', fontsize=12, fontweight='bold')
    ax1.invert_yaxis()
    ax1.legend(loc='best', fontsize=9)
    ax1.grid(True, alpha=0.3)
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
    
    # --- Plot FITS data ---
    mjds = fits_data['mjd']
    filters = fits_data['filter']
    mag_alt = fits_data['mag_alt']
    error_alt = fits_data['magerr_alt']
    mag_fphot = fits_data['mag_fphot']
    error_fphot = fits_data['magerr_fphot']
    
    unique_filters_fits = np.unique(filters)
    for filt in unique_filters_fits:
        mask = filters == filt
        mjds_filt = mjds[mask]
        mag_alt_filt = mag_alt[mask]
        error_alt_filt = error_alt[mask]
        mag_fphot_filt = mag_fphot[mask]
        error_fphot_filt = error_fphot[mask]
        color = filter_colors.get(filt, 'gray')
        
        valid_alt = (mag_alt_filt != 999) & ~np.isnan(mag_alt_filt) & ~np.isnan(error_alt_filt) & (mag_alt_filt > 0)
        if np.any(valid_alt):
            ax2.errorbar(mjds_filt[valid_alt], mag_alt_filt[valid_alt], 
                        yerr=error_alt_filt[valid_alt],
                        fmt='o', color=color, label=f'{filt} (normal)',
                        markersize=8, capsize=3, alpha=0.7, 
                        markeredgecolor='black', markeredgewidth=0.5)
        
        valid_fphot = (mag_fphot_filt != 999) & ~np.isnan(mag_fphot_filt) & ~np.isnan(error_fphot_filt) & (mag_fphot_filt > 0)
        if np.any(valid_fphot):
            ax2.errorbar(mjds_filt[valid_fphot], mag_fphot_filt[valid_fphot],
                        yerr=error_fphot_filt[valid_fphot],
                        fmt='s', color=color, label=f'{filt} (forced)',
                        markersize=8, capsize=3, alpha=0.7,
                        markeredgecolor='black', markeredgewidth=0.5)
    
    ax2.set_xlabel('MJD', fontsize=11)
    ax2.set_ylabel('Magnitude', fontsize=11)
    ax2.set_title('FITS Photometry Data', fontsize=12, fontweight='bold')
    ax2.invert_yaxis()
    ax2.legend(loc='best', fontsize=9, ncol=2)
    ax2.grid(True, alpha=0.3)
    
    fig.suptitle(f'Combined Lightcurve for: {objid}', fontsize=16, fontweight='bold', y=0.995)
    plt.tight_layout()
    
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
        logger.info(f"Saved combined plot to {save_path}")
    else:
        plt.show()
    
    plt.close()


def plot_all_objects(csv_database, fits_database, output_dir='plots'):
    """
    Plot lightcurves for all objects that have both CSV and FITS data
    
    Parameters:
    -----------
    csv_database : dict
        Dictionary of CSV data
    fits_database : dict
        Dictionary of FITS data
    output_dir : str
        Directory to save plots
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Find common objects
    common_objids = set(csv_database.keys()) & set(fits_database.keys())
    logger.info(f"Found {len(common_objids)} objects with both CSV and FITS data")
    
    for i, objid in enumerate(common_objids, 1):
        logger.info(f"Plotting {i}/{len(common_objids)}: {objid}")
        
        save_path = output_path / f"{objid}_lightcurve.png"
        
        try:
            plot_combined_lightcurve(
                csv_database[objid],
                fits_database[objid],
                objid,
                save_path=str(save_path)
            )
        except Exception as e:
            logger.error(f"Error plotting {objid}: {e}")
            continue
    
    logger.info(f"✓ Finished plotting all objects. Plots saved to {output_dir}/")


def argument_parser():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description="Plot DESIRT lightcurves from pickle files")
    parser.add_argument("--csv_pkl", type=str, required=True, help="Path to CSV pickle file")
    parser.add_argument("--fits_pkl", type=str, required=True, help="Path to FITS pickle file")
    parser.add_argument("--objid", type=str, help="Object ID to plot (if not specified, use --plot_all)")
    parser.add_argument("--plot_all", action='store_true', help="Plot all objects")
    parser.add_argument("--output_dir", type=str, default="plots", help="Output directory for plots")
    parser.add_argument("--inspect", action='store_true', help="Inspect pickle files without plotting")
    parser.add_argument("--list_objects", action='store_true', help="List all available object IDs")
    return parser.parse_args()


def main():
    args = argument_parser()
    
    # Load data
    csv_database = load_pickle(args.csv_pkl)
    fits_database = load_pickle(args.fits_pkl)
    
    # Inspect mode
    if args.inspect:
        print("\n" + "="*60)
        print("CSV DATABASE")
        print("="*60)
        inspect_pickle(args.csv_pkl)
        
        print("\n" + "="*60)
        print("FITS DATABASE")
        print("="*60)
        inspect_pickle(args.fits_pkl)
        return
    
    # List objects mode
    if args.list_objects:
        csv_objids = set(csv_database.keys())
        fits_objids = set(fits_database.keys())
        common_objids = csv_objids & fits_objids
        
        print(f"\nTotal objects in CSV: {len(csv_objids)}")
        print(f"Total objects in FITS: {len(fits_objids)}")
        print(f"Objects with both CSV and FITS data: {len(common_objids)}")
        print(f"\nFirst 20 common object IDs:")
        for objid in list(common_objids)[:20]:
            print(f"  - {objid}")
        return
    
    # Plot single object
    if args.objid:
        if args.objid not in csv_database:
            logger.error(f"Object {args.objid} not found in CSV database")
            return
        if args.objid not in fits_database:
            logger.error(f"Object {args.objid} not found in FITS database")
            return
        
        logger.info(f"Plotting lightcurve for {args.objid}")
        plot_combined_lightcurve(
            csv_database[args.objid],
            fits_database[args.objid],
            args.objid
        )
        return
    
    # Plot all objects
    if args.plot_all:
        plot_all_objects(csv_database, fits_database, args.output_dir)
        return
    
    # No action specified
    logger.warning("No action specified. Use --objid, --plot_all, --inspect, or --list_objects")
    logger.info("Example: python plot_lightcurve.py --csv_pkl desirt_csv_data.pkl --fits_pkl desirt_grouped_data.pkl --list_objects")


if __name__ == "__main__":
    main()
