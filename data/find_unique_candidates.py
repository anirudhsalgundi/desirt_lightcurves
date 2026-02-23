import logging
import argparse
from tqdm import tqdm
from collections import defaultdict


"""
Set up logging to both console and timestamped log file.
"""

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



def argument_parser() -> argparse.Namespace:
    """
    Set up the argument parser for the script.
    """

    parser = argparse.ArgumentParser(description="Get stats on the candidates")
    parser.add_argument("--file", required=True, help="File containing paths to the fits files")

    return parser.parse_args()
    

def find_unique_candidates(list_of_files: str) -> dict:
    """
    Read the list of file paths, extract candidate IDs, and group file paths by candidate ID.
    _____
    Parameters:
    list_of_files (str): Path to the file containing the list of file paths.
    _____
    Returns:
    dict: A dictionary where keys are candidate IDs and values are lists of file paths associated with that candidate ID.
    """

    # Initialize a defaultdict to store candidate IDs and their associated file paths
    db = defaultdict(list)
    
    # Read the file containing the list of file paths
    with open(list_of_files, "r") as file:

        # logging
        logger.info(f"Reading lines from {list_of_files}")
        lines = file.readlines()
        
        # Process each line in the file, with sanity checks
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # because why not
            try:
                filename = line.split("/")[-1]
                parts = filename.replace(".fits", "").split("_")
                
                if len(parts) == 2:
                    n_alerts, candidate_id = parts
                    
                    db[candidate_id].append({
                        "file_path": line,
                    })
                else:
                    logger.warning(f"Unexpected filename format (skipping): {filename}")
                    
            except Exception as e:
                logger.info(f"Something went wrong with the line {line}: {e}")
                continue
        
    return dict(db)


def get_latest_file_paths(db: dict) -> list:
    """
    For each unique candidate ID, find the latest file path (the one with the maximum number of detections) and return a list of these latest file paths.
    _____
    Parameters:
    db (dict): A dictionary where keys are candidate IDs and values are lists of file paths associated with that candidate ID.
    _____
    Returns:
    list: A list of the latest file paths for each unique candidate ID.
    """

    # extract the unique candidate IDs from the database
    candidates = db.keys()
    updated_file_paths = []

    # find the full path for the latest file for each candidate ID
    for candidate in candidates:
        temp = db[candidate]
        file_paths = [p["file_path"] for p in temp]
        sorted_file_paths = sorted(file_paths)

        # update the file paths with the latest one, one with max detections
        tqdm.write(f"Finding the latest file path for candidate {candidate} with {len(temp)} file paths")
        updated_file_paths.append(sorted_file_paths[-1])


    logger.info(f"Total unique candidates: {len(candidates)}")
    logger.info(f"Founf {len(updated_file_paths)} latest file paths for the unique candidates")


    return updated_file_paths


def write_to_file(file_paths: list, output_file: str) -> None:
    """
    Write the list of file paths to a text file.
    _____
    Parameters:
    file_paths (list): A list of file paths to write to the text file.
    output_file (str): The name of the output text file.
    """

    logger.info(f"Writing the latest file paths to {output_file}")
    with open(output_file, "w") as f:
        for path in file_paths:
            f.write(f"{path}\n")

    logger.info(f"Done! Latest file paths written to {output_file}")


def main() -> None:
    """
    Main function to execute the script. It parses arguments, finds unique candidates, gets the latest file paths, and writes them to a text file.
    """

    # function calls
    args = argument_parser()
    db = find_unique_candidates(args.file)
    updated_file_paths = get_latest_file_paths(db)
    write_to_file(updated_file_paths, "latest_file_paths.txt")


if __name__ == "__main__":
    main()