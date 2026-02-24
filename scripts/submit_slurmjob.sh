#!/bin/bash
#SBATCH -J desirt_full_run
#SBATCH -p RM
#SBATCH --ntasks-per-node=64
#SBATCH --mem=120G
#SBATCH -A phy250012p
#SBATCH -o logs/desirt_full_%j.out
#SBATCH -e logs/desirt_full_%j.err

cd /ocean/projects/phy250012p/salgundi/desirt_lightcurves
source .venv/bin/activate
cd src
mkdir -p ../logs ../results

time uv run 00_organize_data.py \
    --data /ocean/projects/phy250012p/salgundi/desirt_lightcurves/data/latest_file_paths.txt \
    --n_workers 64 \
    --batch_size 1000 \