#!/bin/bash
#SBATCH -J desirt_full_run
#SBATCH -p RM
#SBATCH --ntasks-per-node=64
#SBATCH --mem=220G
#SBATCH -A phy250012p
#SBATCH -t 48:00:00
#SBATCH -o logs/desirt_full_%j.out
#SBATCH -e logs/desirt_full_%j.err

cd /ocean/projects/phy250012p/salgundi/desirt_lightcurves
source .venv/bin/activate

mkdir -p logs results

echo "=========================================="
echo "Starting full DESIRT processing"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Started at: $(date)"
echo "Using 32 workers"
echo "=========================================="

time uv run organize_desirt_data.py \
    --csv_path ./data/candidate_summary_csvs.txt \
    --fits_path ./data/candidate_fits_files.txt \
    --n_workers 64

EXIT_CODE=$?

echo "=========================================="
echo "Finished at: $(date)"
echo "Exit code: ${EXIT_CODE}"
echo "=========================================="

# Check output file size
if [ -f results/desirt_master_database_*.h5 ]; then
    ls -lh results/desirt_master_database_*.h5
fi

exit $EXIT_CODE