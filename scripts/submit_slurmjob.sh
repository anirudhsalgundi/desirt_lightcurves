#!/bin/bash
#SBATCH -J desirt_full_run
#SBATCH -p RM
#SBATCH --ntasks-per-node=128
#SBATCH --mem=220G
#SBATCH -A phy250012p
#SBATCH -t 48:00:00
#SBATCH -o ../logs/desirt_full_%j.out
#SBATCH -e ../logs/desirt_full_%j.err

source /ocean/projects/phy250012p/salgundi/desirt_lightcurves/.venv/bin/activate
time uv run python /ocean/projects/phy250012p/salgundi/desirt_lightcurves/src/00_organize_data.py \
    --data /ocean/projects/phy250012p/salgundi/desirt_lightcurves/data/latest_file_paths.txt \
    --n_workers 64 \
    --batch_size 1000 \