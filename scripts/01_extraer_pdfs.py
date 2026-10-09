
from pathlib import Path
import subprocess
import csv
BASE = Path(__file__).resolve().parent.parent
ORIGEN = BASE / "data/raw/pdf"
DESTINO = BASE / "data/extracted/txt"
LOG = BASE / "logs/extraccion_pdfs.csv"
DESTINO.mkdir(parents=True, exist_ok=True)
# 1 Primero identifico los pdfs originales
pdfs = sorted(
    p for p in ORIGEN.iterdir()
    if p.is_file() and p.suffix.lower() == ".pdf"
)
print(f"PDF encontrados: {len(pdfs)}")
# 2 Extraemos el texto y registramos los resultados
resultados = {"OK": 0, "VACIO": 0, "ERROR": 0}
with LOG.open("w", newline="", encoding="utf-8") as archivo:
    writer = csv.writer(archivo)
    writer.writerow(["pdf", "txt", "palabras", "estado", "detalle"])
    for pdf in pdfs:
        txt = DESTINO / f"{pdf.stem}.txt"
        try:
            subprocess.run(
                ["pdftotext", "-layout", str(pdf), str(txt)],
                check=True,
                capture_output=True,
                text=True
            )
            palabras = len(txt.read_text(
                encoding="utf-8", errors="replace"
            ).split())
            estado = "OK" if palabras > 0 else "VACIO"
            detalle = ""
        except Exception as error:
            palabras = 0
            estado = "ERROR"
            detalle = str(error)
        resultados[estado] += 1
        writer.writerow([pdf.name, txt.name, palabras, estado, detalle])
# 3 Luego generamos el resumen
print("\nRESULTADO DE EXTRACCION")
for estado, cantidad in resultados.items():
    print(f"{estado}: {cantidad}")
print(f"\nRegistro guardado en: {LOG}")