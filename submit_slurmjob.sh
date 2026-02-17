#!/bin/bash
#SBATCH --ntasks-per-node=12
cd /ocean/projects/phy250012p/salgundi/desirt_lightcurves
source .venv/bin/activate

time uv run organize_desirt_data.py \
    --csv_path /ocean/projects/phy250012p/salgundi/desirt_lightcurves/data/candidate_summary_csvs.txt \
    --fits_path /ocean/projects/phy250012p/salgundi/desirt_lightcurves/data/candidate_fits_files.txt \
    --n_workers 12