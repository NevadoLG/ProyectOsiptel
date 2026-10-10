from pathlib import Path
import subprocess
import tempfile
import csv
import time
BASE = Path(__file__).resolve().parent.parent
INVENTARIO = BASE / "logs/inventario_fuentes.csv"
LOG = BASE / "logs/ocr_pdfs.csv"
with INVENTARIO.open(encoding="utf-8", newline="") as f:
    pendientes = [
        fila for fila in csv.DictReader(f)
        if fila["estado"] == "PENDIENTE_OCR"
    ]
print(f"Documentos pendientes de OCR: {len(pendientes)}")
for fila in pendientes:
    pdf = BASE / fila["ruta_original"]
    txt = BASE / fila["ruta_extraida"]
    if txt.exists() and txt.read_text(
        encoding="utf-8", errors="replace"
    ).strip():
        print(f"CONSERVADO: {pdf.name}")
        continue
    inicio = time.perf_counter()
    estado = "OK"
    detalle = ""
    paginas = 0
    palabras = 0
    try:
        info = subprocess.run(
            ["pdfinfo", str(pdf)],
            capture_output=True, text=True, check=True
        )
        paginas = int(next(
            linea.split(":", 1)[1].strip()
            for linea in info.stdout.splitlines()
            if linea.startswith("Pages:")
        ))
        textos = []
        with tempfile.TemporaryDirectory() as carpeta:
            imagen = Path(carpeta) / "pagina"
            imagen_png = Path(str(imagen) + ".png")
            for numero in range(1, paginas + 1):
                subprocess.run([
                    "pdftoppm",
                    "-f", str(numero),
                    "-l", str(numero),
                    "-r", "180",
                    "-gray", "-singlefile", "-png",
                    str(pdf), str(imagen)
                ], check=True, capture_output=True)
                resultado = subprocess.run([
                    "tesseract", str(imagen_png),
                    "stdout", "-l", "spa"
                ], check=True, capture_output=True, text=True)
                textos.append(
                    f"--- PAGINA {numero} ---\n"
                    + resultado.stdout
                )
                imagen_png.unlink()
        contenido = "\n".join(textos)
        palabras = len(contenido.split())
        if palabras:
            txt.parent.mkdir(parents=True, exist_ok=True)
            temporal = txt.with_suffix(".ocr.tmp")
            temporal.write_text(contenido, encoding="utf-8")
            temporal.replace(txt)
        else:
            estado = "VACIO"
    except Exception as error:
        estado = "ERROR"
        detalle = str(error)
    duracion = round(time.perf_counter() - inicio, 2)
    nuevo = not LOG.exists() or LOG.stat().st_size == 0
    with LOG.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if nuevo:
            writer.writerow([
                "pdf", "paginas", "palabras",
                "estado", "duracion_seg", "detalle"
            ])
        writer.writerow([
            str(pdf.relative_to(BASE)), paginas, palabras,
            estado, duracion, detalle
        ])
    print(f"{pdf.name}: {estado} ({palabras} palabras)")
print("Proceso OCR finalizado.")