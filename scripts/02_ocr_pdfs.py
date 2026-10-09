from pathlib import Path
import subprocess
import tempfile
import csv
import time
BASE = Path(__file__).resolve().parent.parent
ORIGEN = BASE / "data/raw/pdf"
DESTINO = BASE / "data/extracted/txt"
REGISTRO = BASE / "logs/extraccion_pdfs.csv"
LOG = BASE / "logs/ocr_pdfs.csv"
# Lo priomero es identificar automaticamente los pdfs que nos salieron vacios
with REGISTRO.open(encoding="utf-8", newline="") as f:
    pendientes = [
        fila["pdf"]
        for fila in csv.DictReader(f)
        if fila["estado"] == "VACIO"
    ]
print(f"PDF identificados para OCR: {len(pendientes)}")
procesados = 0
omitidos = 0
errores = 0
for indice, nombre in enumerate(pendientes, 1):
    pdf = ORIGEN / nombre
    txt = DESTINO / f"{pdf.stem}.txt"
    # Evitamos repetir los documentos que ya procesados
    if txt.exists() and txt.read_text(
        encoding="utf-8", errors="replace"
    ).strip():
        print(f"[{indice}/{len(pendientes)}] OMITIDO: {nombre}")
        omitidos += 1
        continue
    print(f"[{indice}/{len(pendientes)}] OCR: {nombre}", flush=True)
    inicio = time.perf_counter()
    estado = "OK"
    detalle = ""
    palabras = 0
    procesadas = 0
    paginas = 0
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
        with tempfile.TemporaryDirectory() as temporal:
            imagen = Path(temporal) / "pagina"
            for numero in range(1, paginas + 1):
                archivo_imagen = Path(str(imagen) + ".png")
                subprocess.run([
                    "pdftoppm",
                    "-f", str(numero),
                    "-l", str(numero),
                    "-r", "180",
                    "-gray",
                    "-singlefile",
                    "-png",
                    str(pdf),
                    str(imagen)
                ], check=True, capture_output=True)
                resultado = subprocess.run([
                    "tesseract",
                    str(archivo_imagen),
                    "stdout", "-l", "spa"
                ], capture_output=True, text=True, check=True)
                textos.append(
                    f"\n--- PAGINA {numero} ---\n"
                    + resultado.stdout
                )
                archivo_imagen.unlink()
                procesadas += 1
        contenido = "\n".join(textos)
        palabras = len(contenido.split())
        if palabras > 0:
            temporal_txt = txt.with_suffix(".ocr.tmp")
            temporal_txt.write_text(contenido, encoding="utf-8")
            temporal_txt.replace(txt)
            procesados += 1
        else:
            estado = "VACIO"
    except Exception as error:
        estado = "ERROR"
        detalle = str(error)
        errores += 1
    duracion = round(time.perf_counter() - inicio, 2)
    nuevo = not LOG.exists() or LOG.stat().st_size == 0
    with LOG.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if nuevo:
            writer.writerow([
                "pdf", "paginas", "paginas_procesadas",
                "palabras", "estado", "duracion_seg",
                "detalle"
            ])
        writer.writerow([
            nombre, paginas, procesadas,
            palabras, estado, duracion, detalle
        ])
    print(
        f"  Estado: {estado} | "
        f"Palabras: {palabras} | Tiempo: {duracion}s",
        flush=True
    )
print("\nRESUMEN FINAL")
print(f"Procesados: {procesados}")
print(f"Omitidos: {omitidos}")
print(f"Errores: {errores}")
print(f"Registro: {LOG}")