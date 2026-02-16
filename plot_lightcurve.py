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


def load_master_database(database: str) -> dict:

    # data is a h5 file, still need to look at columns to return a dict


    return placeholder_master_database


def plot_ztf_alerts()

def plot_desirt_alerts()


def plot_ztf_cutouts()

def plot_desirt_cutouts()


def main_plot()



def main()




if __name__ == "__main__":
    main()
