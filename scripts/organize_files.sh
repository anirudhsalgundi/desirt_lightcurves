#!/usr/bin/env bash
# cleanup.sh — applies final structural cleanup
# Run from the root of desirt_lightcurves/

set -euo pipefail

echo "==> Moving organize_files.sh to scripts/..."
[ -f organize_files.sh ] && mv organize_files.sh scripts/organize_files.sh

echo "==> Moving html_generator.py to utils/..."
[ -f src/html_generator.py ] && mv src/html_generator.py utils/html_generator.py

echo "==> Moving view_summary.py from results/ to utils/..."
[ -f results/view_summary.py ] && mv results/view_summary.py utils/view_summary.py

echo "==> Adding .gitkeep to empty tracked directories..."
touch logs/.gitkeep
touch notebooks/.gitkeep
touch results/databases/.gitkeep
touch results/plots/.gitkeep

echo "==> Creating placeholder _temp files for future scripts..."
touch src/03_filter_candidates_temp.py
touch utils/io_temp.py        # real shared I/O helpers go here
touch utils/plot_utils_temp.py  # real shared plotting helpers go here

echo "==> Updating .gitignore..."
cat >> .gitignore << 'EOF'

# desirt pipeline outputs
results/databases/*
results/plots/*
results/summaries/*
!results/**/.gitkeep
logs/*
!logs/.gitkeep
EOF

echo ""
echo "Done! Final structure:"
tree -I "__pycache__|*.pyc|.git"
