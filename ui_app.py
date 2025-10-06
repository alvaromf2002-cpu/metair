from flask import Flask, request, render_template_string
import pandas as pd
from etl_functions import run_pipeline

app = Flask(__name__)

TEMPLATE = """
<!doctype html>
<title>ETL Pipeline UI</title>
<h1>Subir archivo JSON</h1>
<form method=post enctype=multipart/form-data>
  <input type=file name=file>
  <input type=submit value=Upload>
</form>
{% if summary %}
  <h2>Resumen</h2>
  <p>Total: {{summary['total']}}</p>
  <p>Usados: {{summary['used']}}</p>
  <p>Porcentaje: {{'%.2f' % (summary['used']/summary['total']*100 if summary['total'] else 0)}}%</p>
  <h2>Primeras filas</h2>
  {{table|safe}}
{% endif %}
"""

@app.route("/", methods=["GET", "POST"])
def upload_file():
    summary = None
    table = None
    if request.method == "POST":
        file = request.files['file']
        if file:
            path = "/tmp/upload.json"
            file.save(path)
            summary = run_pipeline(path)
            df = pd.read_sql("SELECT * FROM aircraft_metrics LIMIT 10", f"sqlite:///{summary['db_path']}")
            table = df.to_html()
    return render_template_string(TEMPLATE, summary=summary, table=table)

if __name__ == "__main__":
    app.run(debug=True)
