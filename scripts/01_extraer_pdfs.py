from pathlib import Path
import subprocess
import tempfile
import csv
from collections import Counter
BASE = Path(__file__).resolve().parent.parent
PDF_ROOT = BASE / "data/raw/pdf"
TXT_ROOT = BASE / "data/extracted/txt"
EXCEL_ROOT = BASE / "data/raw/excel"
LOG = BASE / "logs/inventario_fuentes.csv"
registros = []
pdfs = sorted(
    p for p in PDF_ROOT.rglob("*")
    if p.is_file() and p.suffix.lower() == ".pdf"
)
print(f"PDF encontrados: {len(pdfs)}")
for pdf in pdfs:
    relativa = pdf.relative_to(PDF_ROOT)
    txt = TXT_ROOT / relativa.with_suffix(".txt")
    txt.parent.mkdir(parents=True, exist_ok=True)
    fuente = (
        relativa.parts[0]
        if len(relativa.parts) > 1
        else "osiptel_regulatorio"
    )
    if txt.exists() and txt.read_text(
        encoding="utf-8", errors="replace"
    ).strip():
        estado = "CONSERVADO"
    else:
        try:
            with tempfile.TemporaryDirectory(
                dir=txt.parent
            ) as temporal:
                salida = Path(temporal) / "extraido.txt"
                subprocess.run(
                    ["pdftotext", "-layout", str(pdf), str(salida)],
                    check=True,
                    capture_output=True
                )
                if salida.exists() and salida.read_text(
                    encoding="utf-8", errors="replace"
                ).strip():
                    salida.replace(txt)
                    estado = "EXTRAIDO"
                else:
                    estado = "PENDIENTE_OCR"
        except Exception as error:
            estado = "ERROR"
            print(f"Error en {pdf.name}: {error}")
    registros.append({
        "fuente": fuente,
        "tipo": "PDF",
        "archivo": pdf.name,
        "ruta_original": str(pdf.relative_to(BASE)),
        "ruta_extraida": str(txt.relative_to(BASE)),
        "estado": estado
    })
for excel in sorted(EXCEL_ROOT.rglob("*")):
    if not excel.is_file():
        continue
    if excel.suffix.lower() not in (".xlsx", ".xls", ".csv"):
        continue
    registros.append({
        "fuente": "osiptel_conexiones",
        "tipo": "ESTRUCTURADO",
        "archivo": excel.name,
        "ruta_original": str(excel.relative_to(BASE)),
        "ruta_extraida": "",
        "estado": "PENDIENTE_ETL"
    })
LOG.parent.mkdir(parents=True, exist_ok=True)
with LOG.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "fuente", "tipo", "archivo",
            "ruta_original", "ruta_extraida", "estado"
        ]
    )
    writer.writeheader()
    writer.writerows(registros)
print("\nRESUMEN DE FUENTES")
for estado, cantidad in Counter(
    r["estado"] for r in registros
).items():
    print(f"{estado}: {cantidad}")
print(f"\nInventario: {LOG}")