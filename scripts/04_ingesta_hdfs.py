from collections import Counter
from datetime import datetime
from hashlib import sha256
from pathlib import Path
import csv
import re
import shutil
import subprocess
import tempfile
import unicodedata
BASE = Path(__file__).resolve().parent.parent
HDFS_BASE = "/datalake/osiptel_proyecto"
FUENTES = (
    (BASE / "data/raw/excel", HDFS_BASE + "/raw/excel"),
    (BASE / "data/raw/pdf", HDFS_BASE + "/raw/pdf"),
    (BASE / "data/raw/zip", HDFS_BASE + "/raw/zip"),
    (BASE / "data/extracted/txt", HDFS_BASE + "/staging/txt"),
    (BASE / "data/extracted/tablas", HDFS_BASE + "/staging/tablas"),
)
EXTENSIONES = {".xlsx", ".xls", ".csv", ".zip", ".pdf", ".txt"}
SEGURO = re.compile(r"^[a-zA-Z0-9._-]+$")
def hdfs(*argumentos, verificar=True):
    return subprocess.run(
        ["hdfs", "dfs", *map(str, argumentos)],
        text=True,
        capture_output=True,
        check=verificar,
    )
def inventario_remoto():
    """Una sola consulta para obtener rutas y tamaños en HDFS."""
    resultado = hdfs("-ls", "-R", HDFS_BASE)
    archivos = {}
    for linea in resultado.stdout.splitlines():
        campos = linea.split(maxsplit=7)
        if len(campos) == 8 and campos[0].startswith("-"):
            try:
                archivos[campos[7]] = int(campos[4])
            except ValueError:
                pass
    return archivos
def nombre_seguro(nombre, identificador):
    """Nombre estable ASCII, con hash para evitar colisiones."""
    if SEGURO.fullmatch(nombre) and not nombre.startswith("-"):
        return nombre
    original = Path(nombre)
    sin_tildes = unicodedata.normalize("NFKD", original.stem)
    sin_tildes = sin_tildes.encode("ascii", "ignore").decode("ascii")
    base = re.sub(r"[^a-zA-Z0-9._-]+", "_", sin_tildes).strip("._-")
    base = (base or "archivo")[:70]
    identificador_corto = sha256(identificador.encode("utf-8")).hexdigest()[:12]
    return f"{base}__{identificador_corto}{original.suffix.lower()}"
def main():
    (BASE / "logs").mkdir(parents=True, exist_ok=True)
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    log = BASE / "logs" / f"ingesta_hdfs_{fecha}.csv"
    estados = Counter()
    anteriores = inventario_remoto()
    directorios_creados = set()
    with log.open("w", newline="", encoding="utf-8") as salida:
        writer = csv.writer(salida)
        writer.writerow([
            "origen", "destino_hdfs", "nombre_original", "bytes",
            "estado", "detalle"
        ])
        def registrar(origen, destino, estado, detalle=""):
            estados[estado] += 1
            writer.writerow([
                str(origen.relative_to(BASE)), destino, origen.name,
                origen.stat().st_size, estado, detalle
            ])
            salida.flush()
        for carpeta, raiz_destino in FUENTES:
            if not carpeta.exists():
                print(f"AVISO: no existe {carpeta.relative_to(BASE)}, se omite")
                continue
            for origen in sorted(p for p in carpeta.rglob("*") if p.is_file()):
                if origen.suffix.lower() not in EXTENSIONES:
                    continue
                relativo = origen.relative_to(carpeta)
                carpetas = relativo.parts[:-1]
                if any(not SEGURO.fullmatch(p) for p in carpetas):
                    registrar(origen, "", "REVISAR", "Subcarpeta con nombre no seguro")
                    continue
                padre = raiz_destino + ("/" + "/".join(carpetas) if carpetas else "")
                ruta_original = padre + "/" + origen.name
                nombre_hdfs = nombre_seguro(origen.name, str(origen.relative_to(BASE)))
                destino = padre + "/" + nombre_hdfs
                remoto_actual = next(
                    (r for r in (ruta_original, destino) if r in anteriores), None
                )
                if remoto_actual is not None:
                    if anteriores[remoto_actual] == origen.stat().st_size:
                        registrar(origen, remoto_actual, "EXISTENTE")
                    else:
                        registrar(origen, remoto_actual, "CONFLICTO",
                                 "Destino ocupado con otro tamaño; no se sobrescribe")
                    continue
                try:
                    if padre not in directorios_creados:
                        hdfs("-mkdir", "-p", padre)
                        directorios_creados.add(padre)
                    with tempfile.TemporaryDirectory(prefix="osiptel_hdfs_") as temp:
                        local_seguro = Path(temp) / nombre_hdfs
                        shutil.copy2(origen, local_seguro)
                        hdfs("-put", str(local_seguro), destino)
                    estadistica = hdfs("-stat", "%b", destino)
                    remoto_bytes = int(estadistica.stdout.strip())
                    if remoto_bytes != origen.stat().st_size:
                        registrar(origen, destino, "REVISAR",
                                 f"Tamaño distinto: local={origen.stat().st_size}, "
                                 f"HDFS={remoto_bytes}")
                    else:
                        anteriores[destino] = remoto_bytes
                        registrar(origen, destino, "SUBIDO")
                except (OSError, ValueError, subprocess.CalledProcessError) as error:
                    detalle = (error.stderr if isinstance(
                        error, subprocess.CalledProcessError) else str(error)) or str(error)
                    registrar(origen, destino, "REVISAR", detalle.strip()[-700:])
    print("\nRESULTADO INGESTA HDFS")
    for estado in ("SUBIDO", "EXISTENTE", "CONFLICTO", "REVISAR"):
        print(f"{estado}: {estados[estado]}")
    print(f"Registro: {log}")
    if estados["CONFLICTO"] or estados["REVISAR"]:
        raise SystemExit("Hay archivos pendientes: revisar el CSV antes de Spark")
if __name__ == "__main__":
    main()