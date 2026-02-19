#!/usr/bin/env python3
"""
Test Pipeline Script
====================
End-to-end test of the DESIRT lightcurve pipeline:
  1. Read the master H5 database → extract object_id, RA, Dec
  2. Crossmatch all objects with ZTF via Kowalski
  3. Pick 3 random objects that have ZTF crossmatches
  4. Build a small test master database (with ZTF data baked in)
  5. Plot lightcurves + cutouts  (reuses 02_plot_lightcurves logic)
  6. Create an HTML summary       (reuses 04_create_summary logic)

Usage
-----
    python src/test_pipeline.py \
        --input_database ./results/databases/desirt_master_database_20260217_184644.h5 \
        --kowalski_creds ./utils/kowalski_credentials.json \
        --projections ./utils/ztf_alert_projections.json \
        --search_radius 3.0 \
        --n_random 3 \
        --output_dir ./results/test_run
"""

import os
import sys
import argparse
import json
import random
import logging
from pathlib import Path
from datetime import datetime

import numpy as np
import h5py
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Make sure sibling packages are importable when running from repo root
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent          # …/src
PROJECT_DIR = SCRIPT_DIR.parent                        # …/desirt_lightcurves
sys.path.insert(0, str(PROJECT_DIR))

# ---------------------------------------------------------------------------
# Import pipeline modules by file path
# (src/ is not a proper Python package, so we use importlib directly)
# ---------------------------------------------------------------------------
import importlib.util

def _import_module_from_path(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

crossmatch_mod = _import_module_from_path(
    "crossmatch_ztf", str(SCRIPT_DIR / "01_crossmatch_ztf.py")
)
plot_mod = _import_module_from_path(
    "plot_lightcurves", str(SCRIPT_DIR / "02_plot_lightcurves.py")
)
summary_mod = _import_module_from_path(
    "create_summary", str(SCRIPT_DIR / "04_create_summary.py")
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = LOG_DIR / f"log_test_pipeline_{timestamp_str}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)
logger.info(f"Log file: {log_file}")


# ===================================================================
# CLI
# ===================================================================
def argument_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="End-to-end test: crossmatch → subset → plot → summary"
    )
    parser.add_argument(
        "--input_database", type=str, required=True,
        help="Path to the existing DESIRT master database (HDF5).",
    )
    parser.add_argument(
        "--kowalski_creds", type=str,
        default=os.environ.get("KOWALSKI_CREDS", str(PROJECT_DIR / "utils" / "kowalski_credentials.json")),
        help="Path to Kowalski credentials JSON (default: $KOWALSKI_CREDS env var).",
    )
    parser.add_argument(
        "--projections", type=str,
        default=str(PROJECT_DIR / "utils" / "ztf_alert_projections.json"),
        help="Path to ZTF alert projections JSON.",
    )
    parser.add_argument(
        "--search_radius", type=float, default=3.0,
        help="Crossmatch radius in arcseconds (default: 3.0).",
    )
    parser.add_argument(
        "--n_random", type=int, default=3,
        help="Number of random ZTF-matched objects to keep (default: 3).",
    )
    parser.add_argument(
        "--output_dir", type=str,
        default=str(PROJECT_DIR / "results" / "test_run"),
        help="Base output directory for the test run.",
    )
    return parser.parse_args()


# ===================================================================
# Step 1  – Read object_id, RA, Dec from the master database
# ===================================================================
def read_coords_from_database(db_path: str) -> list:
    """
    Return list of (object_id, ra, dec) from the master H5 file.
    """
    coords = []
    with h5py.File(db_path, "r") as f:
        for objid in f.keys():
            grp = f[objid]
            ra = grp.attrs.get("ra")
            dec = grp.attrs.get("dec")
            if ra is not None and dec is not None:
                coords.append((objid, float(ra), float(dec)))
    logger.info(f"Read {len(coords)} objects from {db_path}")
    return coords


# ===================================================================
# Step 2  – Crossmatch with ZTF (early-stop after n_random matches)
# ===================================================================
def crossmatch_with_ztf(coords, kowalski_creds, projections_path, search_radius, n_needed):
    """
    Authenticate with Kowalski and crossmatch coords with ZTF,
    stopping as soon as n_needed matches are found.

    Parameters
    ----------
    coords : list
        List of (object_id, ra, dec) tuples.
    n_needed : int
        Stop after collecting this many matched objects.

    Returns
    -------
    crossmatched_alerts : dict
        {desirt_objid: [list of ZTF alert dicts]}
    """
    kowalski_instance, projections = crossmatch_mod.get_kowalski_instance(
        kowalski_creds, projections_path
    )
    if kowalski_instance is None or projections is None:
        logger.error("Kowalski authentication failed – cannot proceed.")
        sys.exit(1)

    # Shuffle so we don't always test the same objects
    shuffled = coords.copy()
    random.shuffle(shuffled)

    crossmatched = {}
    for objid, ra, dec in tqdm(shuffled, desc="Crossmatching ZTF alerts"):
        alerts = crossmatch_mod._query_kowalski(
            ra, dec, search_radius, projections, kowalski_instance
        )
        if alerts:
            crossmatched[objid] = alerts
            logger.info(f"Found {len(alerts)} ZTF alerts for {objid} "
                        f"({len(crossmatched)}/{n_needed} matches)")
            if len(crossmatched) >= n_needed:
                logger.info(f"Reached {n_needed} matches – stopping early")
                break

    logger.info(
        f"Crossmatch done: {len(crossmatched)} matches found "
        f"(queried {min(len(shuffled), shuffled.index((objid, ra, dec)) + 1) if crossmatched else len(shuffled)} of {len(shuffled)} objects)"
    )
    return crossmatched


# ===================================================================
# Step 3  – Pick N random objects that have ZTF matches
# ===================================================================
def select_random_matched(crossmatched_alerts, n: int = 3) -> list:
    """
    Return up to *n* random DESIRT object IDs that have ZTF crossmatches.
    """
    matched_ids = list(crossmatched_alerts.keys())
    if len(matched_ids) == 0:
        logger.error("No ZTF-matched objects found – nothing to select.")
        sys.exit(1)

    n_select = min(n, len(matched_ids))
    selected = random.sample(matched_ids, n_select)
    logger.info(f"Randomly selected {n_select} objects: {selected}")
    return selected


# ===================================================================
# Step 4  – Build a small test master database
# ===================================================================
def build_test_database(
    input_db_path: str,
    selected_objids: list,
    crossmatched_alerts: dict,
    search_radius: float,
    output_db_path: str,
):
    """
    Copy the selected objects from the master DB into a new test DB,
    then append their ZTF crossmatch data.
    """
    logger.info(f"Building test database → {output_db_path}")

    with h5py.File(input_db_path, "r") as f_in, \
         h5py.File(output_db_path, "w") as f_out:

        for objid in selected_objids:
            if objid not in f_in:
                logger.warning(f"{objid} not in input database – skipping")
                continue

            # Deep-copy the whole group (DESIRT data)
            f_in.copy(f_in[objid], f_out, name=objid)
            logger.info(f"  Copied DESIRT data for {objid}")

    # Now open in append mode and add ZTF data via the pipeline helper
    subset_alerts = {k: crossmatched_alerts[k] for k in selected_objids if k in crossmatched_alerts}

    with h5py.File(output_db_path, "a") as f_out:
        stats = crossmatch_mod.add_ztf_alerts_to_master_database(
            f_out, subset_alerts, search_radius
        )

    logger.info(f"Test database created with {len(selected_objids)} objects")
    logger.info(f"  ZTF alerts added: {stats['total_ztf_alerts_added']}")
    return output_db_path


# ===================================================================
# Step 5  – Plot lightcurves & cutouts
# ===================================================================
def plot_all_objects(test_db_path: str, plots_dir: str):
    """
    Use the logic from 02_plot_lightcurves.py to generate plots.
    """
    lc_dir, cutout_dir = plot_mod.create_output_directories(plots_dir)

    with h5py.File(test_db_path, "r") as db:
        objids = list(db.keys())
        logger.info(f"Plotting {len(objids)} objects …")

        for objid in tqdm(objids, desc="Plotting"):
            try:
                plot_mod.process_object(objid, db[objid], lc_dir, cutout_dir)
            except Exception as e:
                logger.error(f"Error plotting {objid}: {e}")

    logger.info(f"Lightcurves → {lc_dir}")
    logger.info(f"Cutouts     → {cutout_dir}")


# ===================================================================
# Step 6  – Create HTML summary
# ===================================================================
def create_summary(test_db_path: str, plots_dir: str, summaries_dir: str):
    """
    Use the logic from 04_create_summary.py to build an HTML page.
    """
    data = summary_mod.extract_data_from_database(test_db_path, plots_dir)
    output_file = summary_mod.create_html_summary(data, summaries_dir)
    logger.info(f"HTML summary → {output_file}")
    return output_file


# ===================================================================
# Main
# ===================================================================
def main():
    args = argument_parser()

    logger.info("=" * 60)
    logger.info("  DESIRT TEST PIPELINE")
    logger.info("=" * 60)
    logger.info(f"  Input database  : {args.input_database}")
    logger.info(f"  Kowalski creds  : {args.kowalski_creds}")
    logger.info(f"  Projections     : {args.projections}")
    logger.info(f"  Search radius   : {args.search_radius} arcsec")
    logger.info(f"  N random objects: {args.n_random}")
    logger.info(f"  Output dir      : {args.output_dir}")
    logger.info("=" * 60)

    # ---- directory setup ------------------------------------------------
    output_dir   = Path(args.output_dir)
    db_dir       = output_dir / "databases"
    plots_dir    = output_dir / "plots"
    summaries_dir = output_dir / "summaries"

    for d in [db_dir, plots_dir, summaries_dir]:
        d.mkdir(parents=True, exist_ok=True)

    test_db_path = str(
        db_dir / f"test_database_{timestamp_str}.h5"
    )

    # ---- Step 1 ---------------------------------------------------------
    logger.info("\n[STEP 1/6] Reading object coordinates from master database …")
    coords = read_coords_from_database(args.input_database)

    # ---- Step 2 ---------------------------------------------------------
    logger.info("\n[STEP 2/6] Crossmatching with ZTF via Kowalski …")
    crossmatched_alerts = crossmatch_with_ztf(
        coords, args.kowalski_creds, args.projections, args.search_radius, args.n_random
    )

    # ---- Step 3 ---------------------------------------------------------
    logger.info("\n[STEP 3/6] Selecting random ZTF-matched objects …")
    selected = select_random_matched(crossmatched_alerts, n=args.n_random)

    # ---- Step 4 ---------------------------------------------------------
    logger.info("\n[STEP 4/6] Building test master database …")
    build_test_database(
        args.input_database,
        selected,
        crossmatched_alerts,
        args.search_radius,
        test_db_path,
    )

    # ---- Step 5 ---------------------------------------------------------
    logger.info("\n[STEP 5/6] Plotting lightcurves & cutouts …")
    plot_all_objects(test_db_path, str(plots_dir))

    # ---- Step 6 ---------------------------------------------------------
    logger.info("\n[STEP 6/6] Creating HTML summary …")
    summary_file = create_summary(test_db_path, str(plots_dir), str(summaries_dir))

    # ---- Done -----------------------------------------------------------
    logger.info("=" * 60)
    logger.info("  TEST PIPELINE COMPLETE!")
    logger.info("=" * 60)
    logger.info(f"  Test database : {test_db_path}")
    logger.info(f"  Plots         : {plots_dir}")
    logger.info(f"  Summary       : {summary_file}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
