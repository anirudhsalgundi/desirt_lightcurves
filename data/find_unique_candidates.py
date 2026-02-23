import logging
import argparse
from tqdm import tqdm
from collections import defaultdict


"""
Set up logging to both console and timestamped log file.
"""

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

logger = logging.getLogger(__name__)



def argument_parser() -> argparse.Namespace:
    """
    Set up the argument parser for the script.
    """
    parser = argparse.ArgumentParser(description="Get stats on the candidates")
    parser.add_argument("--file", required=True, help="File containing paths to the fits files")

    return parser.parse_args()
    


def find_unique_candidates(list_of_files: str) -> dict:

    db = defaultdict(list)
    
    with open(list_of_files, "r") as file:
        logger.info(f"Reading lines from {list_of_files}")
        lines = file.readlines()
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
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



# def get_latest_file_paths(db: dict) -> None:
    
#     candidates = db.keys()
#     updated_file_paths = []

#     for candidate in candidates:
#         temp = db[candidate]
#         file_paths = [p["file_path"] for p in temp]
#         sorted_file_paths = sorted(file_paths)

#         # update the file paths with the latest one, one with max detections
#         tqdm.write(f"Finding the latest file path for candidate {candidate} with {len(file_paths)} file paths")
#         updated_file_paths.append(sorted_file_paths[-1])


#     logger.info(f"Total unique candidates: {len(candidates)}")
#     logger.info(f"Founf {len(updated_file_paths)} latest file paths for the unique candidates")


#     return updated_file_paths

def get_latest_file_paths(db: dict) -> None:
    candidates = db.keys()
    updated_file_paths = []
    for candidate in tqdm(candidates, desc="Finding latest file paths for candidates"):
        temp = db[candidate]
        file_paths = [p["file_path"] for p in temp]
        sorted_file_paths = sorted(file_paths)
        # tqdm.write(f"Finding the latest file path for candidate {candidate} with {len(file_paths)} file paths")
        updated_file_paths.append(sorted_file_paths[-1])
    logger.info(f"Total unique candidates: {len(candidates)}")
    logger.info(f"Found {len(updated_file_paths)} latest file paths for the unique candidates")
    return updated_file_paths

def main():
    args = argument_parser()

    db = find_unique_candidates(args.file)
    get_latest_file_paths(db)


    logger.info(f"Writing the latest file paths to latest_file_paths.txt")
    with open("latest_file_paths.txt", "w") as f:
        for path in get_latest_file_paths(db):
            f.write(f"{path}\n")

    logger.info(f"Done! Latest file paths written to latest_file_paths.txt")



if __name__ == "__main__":
    main()