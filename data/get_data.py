import os
import pathlib


DATA_DIR = pathlib.Path("/ocean/projects/phy250012p/shared/3DTS/DECAM/DESIRT/SEARCH/3DTS/")
candidate_summary_csvs = list(DATA_DIR.glob("*/**/DECam_Candidate_Summary.csv"))
candidate_fits_files = list(DATA_DIR.glob("*/**/*.fits"))

print(f"Found {len(candidate_summary_csvs)} candidate summary CSV files.")
print(f"Found {len(candidate_fits_files)} candidate FITS files.")

with open("candidate_summary_csvs.txt", "w") as f:
    for csv_file in candidate_summary_csvs:
        f.write(str(csv_file) + "\n")

with open("candidate_fits_files.txt", "w") as f:
    for fits_file in candidate_fits_files:
        f.write(str(fits_file) + "\n")

# Write just the first file (or a subset) to test files
with open("test_candidate_summary_csvs.txt", "w") as f:
    if candidate_summary_csvs:  # Check if list is not empty
        f.write(str(candidate_summary_csvs[0]) + "\n")

with open("test_candidate_fits_files.txt", "w") as f:
    if candidate_fits_files:  # Check if list is not empty
        f.write(str(candidate_fits_files[0]) + "\n")


with open("latest_summary.txt", "w") as f:
    f.write(f"Found {len(candidate_summary_csvs)} candidate summary CSV files.\n")
    f.write(f"Found {len(candidate_fits_files)} candidate FITS files.\n")

print("Files have been written to the data directory.")