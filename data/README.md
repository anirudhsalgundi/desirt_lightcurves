# DESIRT Data Directory

This directory contains scripts and file lists for accessing DESIRT (DECam Infrared Transient) survey data.

## Overview

The DESIRT survey data is stored on the Bridges2 cluster at:
```
/ocean/projects/phy250012p/shared/3DTS/DECAM/DESIRT/SEARCH/3DTS/
```

## Files in this Directory

### Scripts

- **`get_data.py`**: Python script that scans the DESIRT data directory and generates file lists

### Generated File Lists

- **`candidate_summary_csvs.txt`**: Paths to all `DECam_Candidate_Summary.csv` files
- **`candidate_fits_files.txt`**: Paths to all FITS files containing candidate data
- **`latest_summary.txt`**: Summary statistics of the latest scan

### Test Files

- **`test_candidate_summary_csvs.txt`**: Subset of CSV files for testing (first file only)
- **`test_candidate_fits_files.txt`**: Subset of FITS files for testing (first file only)

## Usage

### Generate File Lists

To scan the DESIRT data directory and generate updated file lists:

```bash
cd /ocean/projects/phy250012p/salgundi/desirt_lightcurves/data
python get_data.py
```

This will:
1. Search for all `DECam_Candidate_Summary.csv` files
2. Search for all `*.fits` files in the Candidates directories
3. Write full paths to respective `.txt` files
4. Generate a summary of the counts

### Expected Output

```
Found 6253 candidate summary CSV files.
Found 245593 candidate FITS files.
Files have been written to the data directory.
```

## Integration with Pipeline

These file lists are used by the main pipeline:

```bash
python organize_desirt_data.py \
    --data ./data/candidate_fits_files.txt \
    --batch_size 1000 \
    --n_workers 64
```

## Notes

- File lists are plain text with one absolute path per line
- Paths are valid on the Bridges2 cluster only
- Re-run `get_data.py` periodically to update file lists as new data arrives
- The script uses `pathlib.Path.glob()` with recursive patterns (`*/**/`)