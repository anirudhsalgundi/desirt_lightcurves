from jinja2 import Environment, FileSystemLoader
import os

# Define the data for the table
data = [
    {
        "ra": "10.123",
        "dec": "-20.456",
        "desirt_id": "DESIRT001",
        "ztf_id": "ZTF001",
        "lightcurve": "path/to/lightcurve1.png",
        "ztf_cutout": "path/to/ztf_cutout1.png",
        "desirt_cutout": "path/to/desirt_cutout1.png",
    },
    {
        "ra": "11.789",
        "dec": "-21.654",
        "desirt_id": "DESIRT002",
        "ztf_id": "ZTF002",
        "lightcurve": "path/to/lightcurve2.png",
        "ztf_cutout": "path/to/ztf_cutout2.png",
        "desirt_cutout": "path/to/desirt_cutout2.png",
    },
    # Add more entries as needed
]

# Create the MODERN HTML template
template_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DESIRT Lightcurves & Cutouts</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }

        header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }

        header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            font-weight: 700;
        }

        header p {
            font-size: 1.1em;
            opacity: 0.9;
        }

        .controls {
            padding: 20px 30px;
            background: #f8f9fa;
            border-bottom: 2px solid #e9ecef;
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            align-items: center;
        }

        .search-box {
            flex: 1;
            min-width: 250px;
        }

        .search-box input {
            width: 100%;
            padding: 12px 20px;
            border: 2px solid #dee2e6;
            border-radius: 8px;
            font-size: 1em;
            transition: all 0.3s;
        }

        .search-box input:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }

        .stats {
            display: flex;
            gap: 20px;
            color: #6c757d;
            font-size: 0.9em;
        }

        .table-container {
            overflow-x: auto;
            padding: 30px;
        }

        table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
        }

        th {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 15px;
            text-align: left;
            font-weight: 600;
            font-size: 0.95em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            cursor: pointer;
            user-select: none;
            position: sticky;
            top: 0;
            z-index: 10;
        }

        th:hover {
            background: linear-gradient(135deg, #5568d3 0%, #653a8b 100%);
        }

        th::after {
            content: ' ⇅';
            opacity: 0.5;
            font-size: 0.8em;
        }

        td {
            padding: 15px;
            border-bottom: 1px solid #e9ecef;
            font-size: 0.95em;
            vertical-align: top;
        }

        tr {
            transition: background-color 0.2s;
        }

        tr:hover {
            background-color: #f8f9fa;
        }

        .coord-cell {
            font-family: 'Courier New', monospace;
            color: #495057;
        }

        .id-cell {
            font-weight: 600;
            color: #667eea;
        }

        .images-cell {
            min-width: 400px;
        }

        .image-grid {
            display: grid;
            gap: 15px;
        }

        .image-item {
            background: #f8f9fa;
            padding: 10px;
            border-radius: 8px;
            transition: transform 0.2s, box-shadow 0.2s;
        }

        .image-item:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }

        .image-label {
            font-size: 0.85em;
            font-weight: 600;
            color: #6c757d;
            margin-bottom: 8px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .image-item img {
            width: 100%;
            height: auto;
            border-radius: 6px;
            cursor: pointer;
            display: block;
            border: 2px solid transparent;
            transition: border-color 0.2s;
        }

        .image-item img:hover {
            border-color: #667eea;
        }

        /* Modal for full-size images */
        .modal {
            display: none;
            position: fixed;
            z-index: 1000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0,0,0,0.9);
            animation: fadeIn 0.3s;
        }

        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }

        .modal-content {
            margin: auto;
            display: block;
            max-width: 90%;
            max-height: 90%;
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
        }

        .close {
            position: absolute;
            top: 30px;
            right: 50px;
            color: #f1f1f1;
            font-size: 40px;
            font-weight: bold;
            cursor: pointer;
            transition: color 0.3s;
        }

        .close:hover {
            color: #667eea;
        }

        .no-results {
            text-align: center;
            padding: 60px 20px;
            color: #6c757d;
        }

        .no-results svg {
            width: 80px;
            height: 80px;
            margin-bottom: 20px;
            opacity: 0.3;
        }

        footer {
            padding: 20px;
            text-align: center;
            color: #6c757d;
            font-size: 0.9em;
            border-top: 2px solid #e9ecef;
        }

        /* Loading animation */
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        .loading {
            animation: pulse 1.5s ease-in-out infinite;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔭 DESIRT Survey Results</h1>
            <p>Lightcurves and Cutout Images Analysis</p>
        </header>

        <div class="controls">
            <div class="search-box">
                <input type="text" id="searchInput" placeholder="🔍 Search by RA, DEC, DESIRT ID, or ZTF ID...">
            </div>
            <div class="stats">
                <span>Total Objects: <strong id="totalCount">{{ data|length }}</strong></span>
                <span>Showing: <strong id="visibleCount">{{ data|length }}</strong></span>
            </div>
        </div>

        <div class="table-container">
            <table id="dataTable">
                <thead>
                    <tr>
                        <th onclick="sortTable(0)">RA</th>
                        <th onclick="sortTable(1)">DEC</th>
                        <th onclick="sortTable(2)">DESIRT ID</th>
                        <th onclick="sortTable(3)">ZTF ID</th>
                        <th>Lightcurve & Cutouts</th>
                    </tr>
                </thead>
                <tbody id="tableBody">
                    {% for entry in data %}
                    <tr>
                        <td class="coord-cell">{{ entry.ra }}</td>
                        <td class="coord-cell">{{ entry.dec }}</td>
                        <td class="id-cell">{{ entry.desirt_id }}</td>
                        <td class="id-cell">{{ entry.ztf_id }}</td>
                        <td class="images-cell">
                            <div class="image-grid">
                                <div class="image-item">
                                    <div class="image-label">📈 Lightcurve</div>
                                    <img src="{{ entry.lightcurve }}" alt="Lightcurve" onclick="openModal(this.src)">
                                </div>
                                <div class="image-item">
                                    <div class="image-label">🌟 ZTF Cutout</div>
                                    <img src="{{ entry.ztf_cutout }}" alt="ZTF Cutout" onclick="openModal(this.src)">
                                </div>
                                <div class="image-item">
                                    <div class="image-label">🔬 DESIRT Cutout</div>
                                    <img src="{{ entry.desirt_cutout }}" alt="DESIRT Cutout" onclick="openModal(this.src)">
                                </div>
                            </div>
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            <div id="noResults" class="no-results" style="display: none;">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <circle cx="11" cy="11" r="8"></circle>
                    <path d="m21 21-4.35-4.35"></path>
                </svg>
                <h3>No results found</h3>
                <p>Try adjusting your search terms</p>
            </div>
        </div>

        <footer>
            <p>Generated on {{ generation_date }} | Total objects: {{ data|length }}</p>
        </footer>
    </div>

    <!-- Modal for full-size images -->
    <div id="imageModal" class="modal" onclick="closeModal()">
        <span class="close" onclick="closeModal()">&times;</span>
        <img class="modal-content" id="modalImage">
    </div>

    <script>
        // Search functionality
        const searchInput = document.getElementById('searchInput');
        const tableBody = document.getElementById('tableBody');
        const visibleCount = document.getElementById('visibleCount');
        const totalCount = document.getElementById('totalCount');
        const noResults = document.getElementById('noResults');

        searchInput.addEventListener('input', function() {
            const searchTerm = this.value.toLowerCase();
            const rows = tableBody.getElementsByTagName('tr');
            let visibleRows = 0;

            for (let row of rows) {
                const text = row.textContent.toLowerCase();
                if (text.includes(searchTerm)) {
                    row.style.display = '';
                    visibleRows++;
                } else {
                    row.style.display = 'none';
                }
            }

            visibleCount.textContent = visibleRows;
            noResults.style.display = visibleRows === 0 ? 'block' : 'none';
            tableBody.style.display = visibleRows === 0 ? 'none' : '';
        });

        // Sorting functionality
        let sortOrder = {};
        function sortTable(columnIndex) {
            const table = document.getElementById('dataTable');
            const tbody = table.getElementsByTagName('tbody')[0];
            const rows = Array.from(tbody.getElementsByTagName('tr'));
            
            // Toggle sort order
            sortOrder[columnIndex] = !sortOrder[columnIndex];
            const isAscending = sortOrder[columnIndex];

            rows.sort((a, b) => {
                const aValue = a.cells[columnIndex].textContent.trim();
                const bValue = b.cells[columnIndex].textContent.trim();
                
                // Try to parse as number for RA/DEC
                const aNum = parseFloat(aValue);
                const bNum = parseFloat(bValue);
                
                if (!isNaN(aNum) && !isNaN(bNum)) {
                    return isAscending ? aNum - bNum : bNum - aNum;
                } else {
                    return isAscending ? 
                        aValue.localeCompare(bValue) : 
                        bValue.localeCompare(aValue);
                }
            });

            // Re-append sorted rows
            rows.forEach(row => tbody.appendChild(row));
        }

        // Modal functionality
        function openModal(src) {
            const modal = document.getElementById('imageModal');
            const modalImg = document.getElementById('modalImage');
            modal.style.display = 'block';
            modalImg.src = src;
        }

        function closeModal() {
            document.getElementById('imageModal').style.display = 'none';
        }

        // Close modal with Escape key
        document.addEventListener('keydown', function(event) {
            if (event.key === 'Escape') {
                closeModal();
            }
        });

        // Lazy loading for images (optional performance boost)
        document.addEventListener('DOMContentLoaded', function() {
            const images = document.querySelectorAll('img');
            images.forEach(img => {
                img.loading = 'lazy';
            });
        });
    </script>
</body>
</html>
"""

# Write the template to a file
template_dir = "templates"
os.makedirs(template_dir, exist_ok=True)
template_path = os.path.join(template_dir, "summary_template.html")
with open(template_path, "w") as f:
    f.write(template_content)

# Render the template with data
from datetime import datetime
env = Environment(loader=FileSystemLoader(template_dir))
template = env.get_template("summary_template.html")
output_html = template.render(
    data=data,
    generation_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
)

# Write the rendered HTML to a file
output_path = "summary.html"
with open(output_path, "w") as f:
    f.write(output_html)

print(f"✓ Modern HTML summary generated: {output_path}")
print(f"  - Total objects: {len(data)}")
print(f"  - Features: Search, Sort, Full-screen images, Responsive design")
