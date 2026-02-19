#!/usr/bin/env python3
"""
Create HTML Summary

This script reads the DESIRT master database (with ZTF crossmatches) and creates
an HTML summary page with lightcurves and cutouts.

Usage:
    python 04_create_summary.py --database path/to/database.h5
"""

import argparse
import h5py
from pathlib import Path
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
import logging

def setup_logging():
    """Set up logging to both console and timestamped log file."""
    log_dir = Path("./logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"log_create_summary_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Log file created: {log_file}")
    return logger

logger = setup_logging()


def argument_parser() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Create HTML summary from DESIRT master database.")
    parser.add_argument("--database", type=str, required=True,
                       help="Path to the DESIRT master database (HDF5 file).")
    parser.add_argument("--plots_dir", type=str, default="./results/plots",
                       help="Base directory containing plots (default: ./results/plots).")
    parser.add_argument("--output_dir", type=str, default="./results/summaries",
                       help="Output directory for HTML summary (default: ./results/summaries).")
    return parser.parse_args()


def extract_data_from_database(db_path: str, plots_dir: str) -> list:
    """
    Extract data from HDF5 database for HTML summary.
    
    Parameters:
    - db_path: Path to HDF5 database
    - plots_dir: Base directory containing plots
    
    Returns:
    - List of dictionaries with data for each object
    """
    data = []
    plots_path = Path(plots_dir)
    lc_dir = plots_path / "lightcurves"
    cutout_dir = plots_path / "cutouts"
    
    logger.info(f"Opening database: {db_path}")
    with h5py.File(db_path, 'r') as db:
        objids = list(db.keys())
        logger.info(f"Found {len(objids)} objects in database")
        
        for objid in objids:
            obj_group = db[objid]
            
            # Extract RA/Dec
            ra = obj_group.attrs.get('ra', 'N/A')
            dec = obj_group.attrs.get('dec', 'N/A')
            
            # Check for ZTF crossmatch
            has_ztf = 'ztf_crossmatches' in obj_group
            ztf_ids = []
            
            if has_ztf:
                ztf_group = obj_group['ztf_crossmatches']
                for ztf_obj_name in ztf_group.keys():
                    ztf_obj_group = ztf_group[ztf_obj_name]
                    ztf_id = ztf_obj_group.attrs.get('objectId', 'Unknown')
                    ztf_ids.append(ztf_id)
            
            # Construct paths to plots (relative to HTML output location)
            lc_path = f"../plots/lightcurves/{objid}_lc.png"
            desirt_cutout_path = f"../plots/cutouts/{objid}_cutout_decam.png"
            ztf_cutout_path = f"../plots/cutouts/{objid}_cutout_ztf.png" if has_ztf else None
            
            # Check if files actually exist
            lc_exists = (lc_dir / f"{objid}_lc.png").exists()
            desirt_cutout_exists = (cutout_dir / f"{objid}_cutout_decam.png").exists()
            ztf_cutout_exists = (cutout_dir / f"{objid}_cutout_ztf.png").exists() if has_ztf else False
            
            entry = {
                'desirt_id': objid,
                'ra': f"{ra:.6f}" if isinstance(ra, (int, float)) else ra,
                'dec': f"{dec:.6f}" if isinstance(dec, (int, float)) else dec,
                'has_ztf': has_ztf,
                'ztf_ids': ztf_ids,
                'lightcurve': lc_path if lc_exists else None,
                'desirt_cutout': desirt_cutout_path if desirt_cutout_exists else None,
                'ztf_cutout': ztf_cutout_path if ztf_cutout_exists else None,
            }
            
            data.append(entry)
    
    logger.info(f"Extracted data for {len(data)} objects")
    return data


def create_html_summary(data: list, output_dir: str) -> str:
    """
    Create HTML summary from extracted data.
    
    Parameters:
    - data: List of dictionaries with object data
    - output_dir: Directory to save HTML file
    
    Returns:
    - Path to generated HTML file
    """
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Create templates directory
    template_dir = Path("./templates")
    template_dir.mkdir(exist_ok=True)
    
    # Modern, clean, professional HTML template with 2-column layout
    template_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DESIRT × ZTF Crossmatch Summary</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 40px 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            overflow: hidden;
        }
        
        header {
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }
        
        header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            font-weight: 300;
            letter-spacing: 2px;
        }
        
        header p {
            font-size: 1.1em;
            opacity: 0.9;
        }
        
        .stats {
            display: flex;
            justify-content: space-around;
            padding: 30px;
            background: #f8f9fa;
            border-bottom: 1px solid #e9ecef;
        }
        
        .stat-box {
            text-align: center;
        }
        
        .stat-number {
            font-size: 2.5em;
            font-weight: bold;
            color: #2a5298;
        }
        
        .stat-label {
            color: #6c757d;
            font-size: 0.9em;
            margin-top: 5px;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
        }
        
        thead {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        
        th {
            padding: 20px 15px;
            text-align: center;
            font-weight: 600;
            font-size: 0.95em;
            letter-spacing: 0.5px;
            text-transform: uppercase;
        }
        
        td {
            padding: 20px 15px;
            border-bottom: 1px solid #e9ecef;
            vertical-align: top;
        }
        
        td:first-child {
            width: 30%;
            text-align: left;
        }
        
        td:last-child {
            width: 70%;
            text-align: center;
        }
        
        tbody tr {
            transition: all 0.3s ease;
        }
        
        tbody tr:hover {
            background: #f8f9fa;
            transform: scale(1.01);
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
        }
        
        .details-container {
            padding: 10px;
        }
        
        .detail-row {
            margin-bottom: 15px;
            line-height: 1.6;
        }
        
        .detail-label {
            font-weight: 600;
            color: #495057;
            display: block;
            margin-bottom: 4px;
        }
        
        .detail-value {
            font-family: 'Courier New', monospace;
            color: #2a5298;
            font-size: 1.05em;
        }
        
        .badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
            letter-spacing: 0.5px;
            margin-left: 8px;
        }
        
        .badge-yes {
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        
        .badge-no {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
        
        .ztf-info {
            margin-top: 10px;
            padding: 12px;
            background: #f8f9fa;
            border-radius: 8px;
            border-left: 3px solid #667eea;
        }
        
        .ztf-name {
            font-family: 'Courier New', monospace;
            font-weight: 600;
            color: #2a5298;
            font-size: 1.1em;
            margin-bottom: 10px;
        }
        
        .ztf-links {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }
        
        .ztf-link {
            display: inline-block;
            color: white;
            text-decoration: none;
            font-weight: 500;
            font-size: 0.85em;
            padding: 8px 14px;
            border-radius: 6px;
            transition: all 0.3s ease;
        }
        
        .ztf-link.source {
            background: #667eea;
        }
        
        .ztf-link.alerts {
            background: #764ba2;
        }
        
        .ztf-link:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
            opacity: 0.9;
        }
        
        .images-container {
            display: flex;
            flex-direction: column;
            gap: 15px;
            padding: 10px;
        }
        
        .image-wrapper {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 12px;
            border: 2px solid #e9ecef;
        }
        
        .image-wrapper img {
            max-width: 100%;
            height: auto;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
        }
        
        .no-image {
            color: #6c757d;
            font-style: italic;
            padding: 20px;
        }
        
        footer {
            background: #1e3c72;
            color: white;
            text-align: center;
            padding: 20px;
            font-size: 0.9em;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>DESIRT × ZTF Crossmatch Summary</h1>
            <p>Fast Transient Candidates | {{ generation_date }}</p>
        </header>
        
        <div class="stats">
            <div class="stat-box">
                <div class="stat-number">{{ total_objects }}</div>
                <div class="stat-label">Total Objects</div>
            </div>
            <div class="stat-box">
                <div class="stat-number">{{ ztf_matches }}</div>
                <div class="stat-label">ZTF Matches</div>
            </div>
            <div class="stat-box">
                <div class="stat-number">{{ match_rate }}%</div>
                <div class="stat-label">Match Rate</div>
            </div>
        </div>
        
        <table>
            <thead>
                <tr>
                    <th>Candidate Info</th>
                    <th>Lightcurve & Cutouts</th>
                </tr>
            </thead>
            <tbody>
                {% for entry in data %}
                <tr>
                    <td>
                        <div class="details-container">
                            <div class="detail-row">
                                <span class="detail-label">DECam Object ID:</span>
                                <span class="detail-value">{{ entry.desirt_id }}</span>
                            </div>
                            <div class="detail-row">
                                <span class="detail-label">Coordinates [RA, Dec]:</span>
                                <span class="detail-value">({{ entry.ra }}, {{ entry.dec }})</span>
                            </div>
                            <div class="detail-row">
                                <span class="detail-label">ZTF Crossmatch:</span>
                                {% if entry.has_ztf %}
                                <span class="badge badge-yes">YES</span>
                                {% for ztf_id in entry.ztf_ids %}
                                <div class="ztf-info">
                                    <div class="ztf-name">{{ ztf_id }}</div>
                                    <div class="ztf-links">
                                        <a href="https://fritz.science/source/{{ ztf_id }}" 
                                           class="ztf-link source" 
                                           target="_blank">
                                            Fritz Source Page
                                        </a>
                                        <a href="https://fritz.science/alerts/ztf/{{ ztf_id }}" 
                                           class="ztf-link alerts" 
                                           target="_blank">
                                            Fritz Alerts Page
                                        </a>
                                    </div>
                                </div>
                                {% endfor %}
                                {% else %}
                                <span class="badge badge-no">NO</span>
                                {% endif %}
                            </div>
                        </div>
                    </td>
                    <td>
                        <div class="images-container">
                            {% if entry.lightcurve %}
                            <div class="image-wrapper">
                                <img src="{{ entry.lightcurve }}" alt="Lightcurve">
                            </div>
                            {% endif %}
                            
                            {% if entry.desirt_cutout %}
                            <div class="image-wrapper">
                                <img src="{{ entry.desirt_cutout }}" alt="DECam Cutouts">
                            </div>
                            {% endif %}
                            
                            {% if entry.ztf_cutout %}
                            <div class="image-wrapper">
                                <img src="{{ entry.ztf_cutout }}" alt="ZTF Cutouts">
                            </div>
                            {% endif %}
                            
                            {% if not entry.lightcurve and not entry.desirt_cutout and not entry.ztf_cutout %}
                            <span class="no-image">No images available</span>
                            {% endif %}
                        </div>
                    </td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        
        <footer>
            Generated on {{ generation_date }} | DESIRT Lightcurve Pipeline
        </footer>
    </div>
</body>
</html>
"""
    
    # Write template to file
    template_path = template_dir / "summary_template.html"
    with open(template_path, "w") as f:
        f.write(template_content)
    
    logger.info(f"Template created: {template_path}")
    
    # Calculate statistics
    total_objects = len(data)
    ztf_matches = sum(1 for entry in data if entry['has_ztf'])
    match_rate = int((ztf_matches / total_objects * 100)) if total_objects > 0 else 0
    
    # Render template
    env = Environment(loader=FileSystemLoader(str(template_dir)))
    template = env.get_template("summary_template.html")
    
    output_html = template.render(
        data=data,
        total_objects=total_objects,
        ztf_matches=ztf_matches,
        match_rate=match_rate,
        generation_date=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )
    
    # Write HTML to file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = output_path / f"desirt_summary_{timestamp}.html"
    with open(output_file, "w") as f:
        f.write(output_html)
    
    logger.info(f"HTML summary generated: {output_file}")
    return str(output_file)


def main():
    """Main function to create HTML summary."""
    args = argument_parser()
    
    logger.info("="*60)
    logger.info("Creating HTML Summary")
    logger.info("="*60)
    
    # Extract data from database
    data = extract_data_from_database(args.database, args.plots_dir)
    
    # Create HTML summary
    output_file = create_html_summary(data, args.output_dir)
    
    logger.info("="*60)
    logger.info("Summary Creation Complete!")
    logger.info(f"Output: {output_file}")
    logger.info("="*60)


if __name__ == "__main__":
    main()