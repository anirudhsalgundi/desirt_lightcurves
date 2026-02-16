#!/bin/bash

mkdir -p logs


cd /ocean/projects/phy250012p/salgundi/desirt_lightcurves
source .venv/bin/activate


time uv run organize_desirt_data.py \
    --csv_path ./data/test_csvs.txt \
    --fits_path ./data/test_fits.txt \
    --n_workers 6