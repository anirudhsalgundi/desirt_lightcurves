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

# Create the HTML template
template_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Lightcurves and Cutouts</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f4f4f9;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: center;
        }
        th {
            background-color: #007BFF;
            color: white;
        }
        tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        img {
            max-width: 100%;
            height: auto;
        }
    </style>
</head>
<body>
    <h1>Lightcurves and Cutouts</h1>
    <table>
        <thead>
            <tr>
                <th>RA</th>
                <th>DEC</th>
                <th>DESIRT ID</th>
                <th>ZTF ID</th>
                <th>Lightcurve and Cutouts</th>
            </tr>
        </thead>
        <tbody>
            {% for entry in data %}
            <tr>
                <td>{{ entry.ra }}</td>
                <td>{{ entry.dec }}</td>
                <td>{{ entry.desirt_id }}</td>
                <td>{{ entry.ztf_id }}</td>
                <td>
                    <div>
                        <img src="{{ entry.lightcurve }}" alt="Lightcurve">
                        <img src="{{ entry.ztf_cutout }}" alt="ZTF Cutout">
                        <img src="{{ entry.desirt_cutout }}" alt="DESIRT Cutout">
                    </div>
                </td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
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
env = Environment(loader=FileSystemLoader(template_dir))
template = env.get_template("summary_template.html")
output_html = template.render(data=data)

# Write the rendered HTML to a file
output_path = "summary.html"
with open(output_path, "w") as f:
    f.write(output_html)

print(f"HTML summary generated: {output_path}")