#!/usr/bin/env python3
"""
View summary statistics from DESIRT HDF5 database.

Usage:
    python view_summary.py <path_to_hdf5_file>
    python view_summary.py <path_to_hdf5_file> --detailed
    python view_summary.py <path_to_hdf5_file> --object <objid>
"""

import h5py
import numpy as np
import argparse
from pathlib import Path
from collections import defaultdict


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="View summary statistics from DESIRT HDF5 database"
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Path to the HDF5 database file"
    )
    return parser.parse_args()


def get_database_summary(hdf5_file):
    """Get overall database summary statistics."""
    with h5py.File(hdf5_file, 'r') as f:
        num_objects = len(f.keys())
        
        total_observations = 0
        filter_counts = defaultdict(int)
        obs_per_object = []
        ra_list = []
        dec_list = []
        mjd_range = [np.inf, -np.inf]
        
        for objid in f.keys():
            grp = f[objid]
            
            # Get RA/DEC
            if 'ra' in grp.attrs:
                ra_list.append(grp.attrs['ra'])
                dec_list.append(grp.attrs['dec'])
            
            # Get observations
            mjds = grp['mjds'][:]
            filters = grp['filters'][:]
            
            n_obs = len(mjds)
            total_observations += n_obs
            obs_per_object.append(n_obs)
            
            # Update MJD range
            mjd_range[0] = min(mjd_range[0], mjds.min())
            mjd_range[1] = max(mjd_range[1], mjds.max())
            
            # Count filters
            for filt in filters:
                if isinstance(filt, bytes):
                    filt = filt.decode('utf-8')
                filter_counts[filt] += 1
        
        return {
            'num_objects': num_objects,
            'total_observations': total_observations,
            'obs_per_object': np.array(obs_per_object),
            'filter_counts': dict(filter_counts),
            'ra_range': (min(ra_list), max(ra_list)) if ra_list else (None, None),
            'dec_range': (min(dec_list), max(dec_list)) if dec_list else (None, None),
            'mjd_range': mjd_range
        }

def print_summary(hdf5_file, top_n=10):
    """Print overall database summary."""
    print("="*70)
    print(f"DESIRT DATABASE SUMMARY")
    print(f"File: {hdf5_file}")
    print("="*70)
    
    summary = get_database_summary(hdf5_file)
    
    print(f"\nOVERALL STATISTICS")
    print(f"{'─'*70}")
    print(f"  Total Objects:        {summary['num_objects']:,}")
    print(f"  Total Observations:   {summary['total_observations']:,}")
    print(f"  Median Obs per Object: {np.median(summary['obs_per_object']):.1f}")
    print(f"  Min Obs per Object:   {summary['obs_per_object'].min()}")
    print(f"  Max Obs per Object:   {summary['obs_per_object'].max()}")

 
    print("="*70)


def main():
    """Main function."""
    args = parse_args()
    
    # Check if file exists
    if not Path(args.file).exists():
        print(f"Error: File not found: {args.file}")
        return
    
    print_summary(args.file, top_n=10)

if __name__ == "__main__":
    main()
