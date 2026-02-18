#!/bin/bash
#SBATCH -J desirt_full_run
#SBATCH -p RM
#SBATCH --ntasks-per-node=12
#SBATCH --mem=64G
#SBATCH -A phy250012p
#SBATCH -o logs/desirt_full_%j.out
#SBATCH -e logs/desirt_full_%j.err

cd /ocean/projects/phy250012p/salgundi/desirt_lightcurves
source .venv/bin/activate

mkdir -p logs results

time uv run organize_desirt_data.py \
    --data /ocean/projects/phy250012p/salgundi/desirt_lightcurves/test.txt \
    --n_workers 12 \
    --batch_size 100 \