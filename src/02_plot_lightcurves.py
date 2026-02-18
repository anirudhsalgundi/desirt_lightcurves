#!/usr/bin/env python3
"""
Plot DESIRT Lightcurves

This script loads the HDF5 master database and creates visualizations of lightcurves
and cutout images from both DESIRT and ZTF data.

Usage:
    python 02_plot_lightcurves.py --database path/to/database.h5 --objid <object_id>
    python 02_plot_lightcurves.py --database path/to/database.h5 --plot_all
"""

import os
import gzip
import argparse
from pathlib import Path
from datetime import datetime
from io import BytesIO

import numpy as np
import matplotlib.pyplot as plt
import h5py
from astropy.io import fits
from astropy.time import Time
from tqdm import tqdm

import logging

def setup_logging():
    """Set up logging to both console and timestamped log file."""
    log_dir = Path("./logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"log_plot_lightcurves_{timestamp}.log"
    
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

logger = setup_logging()


def argument_parser() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Plot DESIRT and ZTF lightcurves and cutouts.")
    parser.add_argument("--database", type=str, required=True, 
                       help="Path to the DESIRT master database (HDF5 file).")
    parser.add_argument("--objid", type=str, required=False, 
                       help="Specific DESIRT object ID to plot.")
    parser.add_argument("--plot_all", action="store_true", 
                       help="Plot all objects in the database.")
    parser.add_argument("--output_dir", type=str, default="./results/plots",
                       help="Base output directory for plots (default: ./results/plots).")
    return parser.parse_args()


def create_output_directories(base_dir: str):
    """Create output directories for plots."""
    base_path = Path(base_dir)
    lc_dir = base_path / "lightcurves"
    cutout_dir = base_path / "cutouts"
    
    lc_dir.mkdir(parents=True, exist_ok=True)
    cutout_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Output directories created: {lc_dir}, {cutout_dir}")
    return lc_dir, cutout_dir


def extract_desirt_data(obj_group):
    """Extract DESIRT photometry data from HDF5 group."""
    data = {
        'mjds': obj_group['mjds'][:] if 'mjds' in obj_group else None,
        'filters': obj_group['filters'][:] if 'filters' in obj_group else None,
        'mag_alt': obj_group['mag_alt'][:] if 'mag_alt' in obj_group else None,
        'magerr_alt': obj_group['magerr_alt'][:] if 'magerr_alt' in obj_group else None,
        'mag_fphot': obj_group['mag_fphot'][:] if 'mag_fphot' in obj_group else None,
        'magerr_fphot': obj_group['magerr_fphot'][:] if 'magerr_fphot' in obj_group else None,
        'science_image': obj_group['science_image'][:] if 'science_image' in obj_group else None,
        'template_image': obj_group['template_image'][:] if 'template_image' in obj_group else None,
        'difference_image': obj_group['difference_image'][:] if 'difference_image' in obj_group else None,
    }
    return data


def extract_ztf_data(ztf_group):
    """Extract ZTF photometry data from a single ZTF object group."""
    data = {
        'mjd': ztf_group['ztf_mjd'][:],
        'mag': ztf_group['ztf_mag'][:],
        'magerr': ztf_group['ztf_magerr'][:],
        'fid': ztf_group['ztf_fid'][:],
        'science_image': ztf_group['science_image'][:] if 'science_image' in ztf_group else None,
        'template_image': ztf_group['template_image'][:] if 'template_image' in ztf_group else None,
        'difference_image': ztf_group['difference_image'][:] if 'difference_image' in ztf_group else None,
    }
    return data


def plot_lightcurve(objid, desirt_data, ztf_data_list, output_path):
    """
    Plot combined lightcurve for DESIRT and ZTF data.
    
    Parameters:
    - objid: DESIRT object ID
    - desirt_data: Dictionary with DESIRT photometry
    - ztf_data_list: List of dictionaries with ZTF photometry (one per ZTF object)
    - output_path: Path to save the plot
    """
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Define filter colors
    filter_colors = {'g': 'green', 
                     'r': 'red', 
                     'i': 'orange', 
                     'z': 'purple', 
                     'u': 'blue', 
                     "Y": "brown",
                     "B": "blue",
                     "V": "green",
                     "R": "red",
                     "I": "orange"}
                     
    ztf_fid_colors = {1: 'green',
                      2: 'red', 
                      3: 'orange'}  # ZTF: 1=g, 2=r, 3=i
    
    legend_handles = []
    
    # Plot ZTF data if available
    if ztf_data_list:
        for idx, ztf_data in enumerate(ztf_data_list):
            mjd = ztf_data['mjd']
            mag = ztf_data['mag']
            magerr = ztf_data['magerr']
            fid = ztf_data['fid']
            
            # Plot each filter separately
            for fid_val in np.unique(fid):
                if fid_val <= 0:
                    continue
                mask = fid == fid_val
                color = ztf_fid_colors.get(fid_val, 'gray')
                filter_name = {1: 'g', 2: 'r', 3: 'i'}.get(fid_val, f'fid{fid_val}')
                
                handle = ax.errorbar(
                    mjd[mask], mag[mask], yerr=magerr[mask],
                    marker='*', markersize=10, linestyle='', 
                    color=color, alpha=0.7, capsize=3,
                    label=f'ZTF-{filter_name}' if idx == 0 else None
                )
                if idx == 0 and mask.sum() > 0:
                    legend_handles.append(handle)
    
    # Plot DESIRT data
    if desirt_data['mjds'] is not None:
        mjds = desirt_data['mjds']
        filters = desirt_data['filters']
        
        # Decode filter names if they're bytes
        if filters is not None and len(filters) > 0:
            if isinstance(filters[0], bytes):
                filters = [f.decode('utf-8') for f in filters]
        
        # Plot mag_alt (decam)
        if desirt_data['mag_alt'] is not None:
            for filter_name in np.unique(filters):
                mask = filters == filter_name
                color = filter_colors.get(filter_name, 'black')
                
                handle = ax.errorbar(
                    mjds[mask], desirt_data['mag_alt'][mask], 
                    yerr=desirt_data['magerr_alt'][mask],
                    marker='o', markersize=6, linestyle='', 
                    color=color, alpha=0.8, capsize=2,
                    label=f'DECam-{filter_name}'
                )
                if mask.sum() > 0:
                    legend_handles.append(handle)
        
        # Plot mag_fphot (forced photometry) with different marker
        if desirt_data['mag_fphot'] is not None:
            for filter_name in np.unique(filters):
                mask = filters == filter_name
                color = filter_colors.get(filter_name, 'black')
                
                handle = ax.errorbar(
                    mjds[mask], desirt_data['mag_fphot'][mask], 
                    yerr=desirt_data['magerr_fphot'][mask],
                    marker='s', markersize=5, linestyle='', 
                    color=color, alpha=0.6, capsize=2,
                    label=f'DECam-fphot-{filter_name}'
                )
                if mask.sum() > 0:
                    legend_handles.append(handle)
    
    # Formatting
    ax.invert_yaxis()
    ax.set_xlabel('MJD', fontsize=12)
    ax.set_ylabel('Magnitude', fontsize=12)
    ax.set_title(f'Lightcurve: {objid}', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend(loc='best', fontsize=9)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Saved lightcurve: {output_path}")


def plot_cutouts_ztf(ztf_data_list, objid, output_path):
    """
    Plot ZTF cutouts (science, template, difference) for the first ZTF match.
    
    Parameters:
    - ztf_data_list: List of ZTF data dictionaries
    - objid: DESIRT object ID
    - output_path: Path to save the plot
    """
    if not ztf_data_list:
        logger.debug(f"No ZTF data for {objid}")
        return
    
    # Use the first ZTF match
    ztf_data = ztf_data_list[0]
    
    # Check if cutouts exist
    if (ztf_data['science_image'] is None and 
        ztf_data['template_image'] is None and 
        ztf_data['difference_image'] is None):
        logger.debug(f"No ZTF cutouts for {objid}")
        return
    
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    
    cutout_types = [
        ('science_image', 'Science'),
        ('template_image', 'Template'),
        ('difference_image', 'Difference')
    ]
    
    for ax, (cutout_key, title) in zip(axes, cutout_types):
        cutout_data = ztf_data[cutout_key]
        
        if cutout_data is not None:
            # Decompress and load the FITS data
            try:
                with gzip.open(BytesIO(cutout_data), 'rb') as f:
                    with fits.open(BytesIO(f.read())) as hdul:
                        img = hdul[0].data
                        
                        # Get image center
                        ny, nx = img.shape
                        center_x, center_y = (nx - 1) / 2.0, (ny - 1) / 2.0
                        
                        # Plot with scaling
                        vmin, vmax = np.percentile(img, [5, 95])
                        ax.imshow(img, cmap="gray", origin='lower', vmin=vmin, vmax=vmax)
                        
                        # Add circle at center
                        circle = plt.Circle((center_x, center_y), radius=3.5,
                                          color='red', fill=False, linewidth=1.5)
                        ax.add_patch(circle)
                        
                        ax.set_title(f"ZTF {title}", fontsize=12, fontweight='bold')
                        ax.set_xticks([])
                        ax.set_yticks([])
            except Exception as e:
                logger.warning(f"Could not load ZTF {title} cutout for {objid}: {e}")
                ax.text(0.5, 0.5, f'Error loading\n{title}', 
                       ha='center', va='center', transform=ax.transAxes)
                ax.set_xticks([])
                ax.set_yticks([])
        else:
            ax.text(0.5, 0.5, f'No {title}', 
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_xticks([])
            ax.set_yticks([])
    
    plt.suptitle(f'ZTF Cutouts: {objid}', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Saved ZTF cutouts: {output_path}")


def plot_cutouts_desirt(desirt_data, objid, output_path):
    """
    Plot DESIRT cutouts (science, template, difference).
    
    Parameters:
    - desirt_data: Dictionary with DESIRT data
    - objid: DESIRT object ID
    - output_path: Path to save the plot
    """
    # Check if cutouts exist
    if (desirt_data['science_image'] is None and 
        desirt_data['template_image'] is None and 
        desirt_data['difference_image'] is None):
        logger.debug(f"No DESIRT cutouts for {objid}")
        return
    
    # DESIRT stores multiple epochs, use the first one
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    
    cutout_types = [
        ('science_image', 'Science'),
        ('template_image', 'Template'),
        ('difference_image', 'Difference')
    ]
    
    for ax, (cutout_key, title) in zip(axes, cutout_types):
        cutout_data = desirt_data[cutout_key]
        
        if cutout_data is not None and len(cutout_data) > 0:
            # Use the first epoch
            img = cutout_data[0]
            
            # Get image center
            ny, nx = img.shape
            center_x, center_y = (nx - 1) / 2.0, (ny - 1) / 2.0
            
            # Plot with scaling
            vmin, vmax = np.percentile(img, [5, 95])
            ax.imshow(img, cmap="gray", origin='lower', vmin=vmin, vmax=vmax)
            
            # Add circle at center
            circle = plt.Circle((center_x, center_y), radius=3.5,
                              color='red', fill=False, linewidth=1.5)
            ax.add_patch(circle)
            
            ax.set_title(f"DECam {title}", fontsize=12, fontweight='bold')
            ax.set_xticks([])
            ax.set_yticks([])
        else:
            ax.text(0.5, 0.5, f'No {title}', 
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_xticks([])
            ax.set_yticks([])
    
    plt.suptitle(f'DECam Cutouts: {objid}', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Saved DESIRT cutouts: {output_path}")


def process_object(objid, obj_group, lc_dir, cutout_dir):
    """
    Process a single object: extract data and create plots.
    
    Parameters:
    - objid: DESIRT object ID
    - obj_group: HDF5 group for this object
    - lc_dir: Directory to save lightcurve plots
    - cutout_dir: Directory to save cutout plots
    """
    # Extract DESIRT data
    desirt_data = extract_desirt_data(obj_group)
    
    # Extract ZTF data if available
    ztf_data_list = []
    if 'ztf_crossmatches' in obj_group:
        ztf_group = obj_group['ztf_crossmatches']
        for ztf_obj_name in ztf_group.keys():
            ztf_obj_group = ztf_group[ztf_obj_name]
            ztf_data = extract_ztf_data(ztf_obj_group)
            ztf_data_list.append(ztf_data)
    
    # Plot lightcurve
    lc_path = lc_dir / f"{objid}_lc.png"
    plot_lightcurve(objid, desirt_data, ztf_data_list, lc_path)
    
    # Plot DESIRT cutouts
    desirt_cutout_path = cutout_dir / f"{objid}_cutout_decam.png"
    plot_cutouts_desirt(desirt_data, objid, desirt_cutout_path)
    
    # Plot ZTF cutouts if available
    if ztf_data_list:
        ztf_cutout_path = cutout_dir / f"{objid}_cutout_ztf.png"
        plot_cutouts_ztf(ztf_data_list, objid, ztf_cutout_path)


def main():
    """Main function to plot lightcurves and cutouts."""
    args = argument_parser()
    
    logger.info("="*60)
    logger.info("Starting Lightcurve and Cutout Plotting")
    logger.info("="*60)
    
    # Create output directories
    lc_dir, cutout_dir = create_output_directories(args.output_dir)
    
    # Open database
    logger.info(f"Opening database: {args.database}")
    try:
        db = h5py.File(args.database, 'r')
    except Exception as e:
        logger.error(f"Failed to open database: {e}")
        return
    
    # Get list of objects to plot
    if args.objid:
        if args.objid not in db:
            logger.error(f"Object {args.objid} not found in database")
            db.close()
            return
        objids = [args.objid]
        logger.info(f"Plotting single object: {args.objid}")
    elif args.plot_all:
        objids = list(db.keys())
        logger.info(f"Plotting all {len(objids)} objects in database")
    else:
        logger.error("Must specify either --objid or --plot_all")
        db.close()
        return
    
    # Process each object
    for objid in tqdm(objids, desc="Plotting objects"):
        try:
            obj_group = db[objid]
            process_object(objid, obj_group, lc_dir, cutout_dir)
        except Exception as e:
            logger.error(f"Error processing {objid}: {e}")
            continue
    
    db.close()
    logger.info("Database closed")
    
    logger.info("="*60)
    logger.info("Plotting Complete!")
    logger.info(f"Lightcurves saved to: {lc_dir}")
    logger.info(f"Cutouts saved to: {cutout_dir}")
    logger.info("="*60)


if __name__ == "__main__":
    main()
