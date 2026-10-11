from pathlib import Path
from collections import Counter
import csv
import re
import subprocess
import unicodedata
BASE = Path(__file__).resolve().parent.parent
ORIGEN = BASE / 'data/raw/pdf/inei_tic'
SALIDA = BASE / 'data/extracted/tablas/inei_indicadores_bruto.csv'
LOG = BASE / 'logs/inei_indicadores_revision.csv' 
COLUMNAS = ['telefonia_fija_pct', 'telefonia_movil_pct', 'tv_cable_pct', 'computadora_pct', 'internet_pct']
NUMERO = re.compile(r'(?<!\d)\d{1,3},\d(?!\d)')
TRIMESTRE = re.compile(
    r'^\s*((?:I{1,3}|IV)\s+Trimestre|Ene-Feb-Mar|Abr-May-Jun|'
    r'Jul-Ago-Se(?:p(?:t)?|t)|Oct-Nov-Dic\.?)\b', re.I
)
def normalizar(texto):
    texto = unicodedata.normalize('NFD', texto.lower())
    return ''.join(c for c in texto if unicodedata.category(c) != 'Mn')
def valores(linea):
    numeros = NUMERO.findall(linea)
    if len(numeros) != 5:
        return None
    v = [float(x.replace(',', '.')) for x in numeros]
    return dict(zip(COLUMNAS, v)) if all(0 <= x <= 100 for x in v) else None
def numero_trimestre(etiqueta):
    s = normalizar(etiqueta)
    if 'trimestre' in s:
        return {'i': 1, 'ii': 2, 'iii': 3, 'iv': 4}[s.split()[0]]
    return 1 if s.startswith('ene') else 2 if s.startswith('abr') else 3 if s.startswith('jul') else 4
def extraer_inei():
    registros, revision = [], []
    for pdf in sorted(ORIGEN.glob('*.pdf')):
        try:
            resultado = subprocess.run(
                ['pdftotext', '-layout', str(pdf), '-'],
                capture_output=True, text=True, check=True
            )
            paginas = resultado.stdout.split('\f')
            seleccion = next((
                (numero, pagina)
                for numero, pagina in enumerate(paginas, 1)
                if 'hogares con acceso a servicios y bienes' in normalizar(pagina)
                and 'indicadores trimestrales' in normalizar(pagina)
                and 'internet' in normalizar(pagina)
            ), None)
            if seleccion is None:
                revision.append([pdf.name, 'SIN_TABLA_COMPATIBLE', 0])
                continue
            numero_pagina, texto = seleccion
            seccion, anio, cantidad = '', None, 0
            for linea in texto.splitlines():
                linea_normal = normalizar(linea)
                if 'indicadores anuales' in linea_normal:
                    seccion = 'anual'
                    continue
                if 'indicadores trimestrales' in linea_normal:
                    seccion = 'trimestral'
                    continue
                if 'variacion absoluta' in linea_normal or 'fuente:' in linea_normal:
                    break
                if seccion == 'anual':
                    m = re.match(r'^\s*(20\d{2})\s+', linea)
                    datos = valores(linea[m.end():]) if m else None
                    if datos:
                        registros.append({
                            'fuente_pdf': pdf.name, 'pagina': numero_pagina,
                            'tipo_periodo': 'anual', 'anio': int(m.group(1)),
                            'trimestre': '', **datos
                        })
                        cantidad += 1
                elif seccion == 'trimestral':
                    m_anio = re.match(r'^\s*(20\d{2})(?:\s+P/)?\s*$', linea)
                    if m_anio:
                        anio = int(m_anio.group(1))
                        continue
                    m = TRIMESTRE.match(linea)
                    datos = valores(linea[m.end():]) if m else None
                    if m and anio and datos:
                        registros.append({
                            'fuente_pdf': pdf.name, 'pagina': numero_pagina,
                            'tipo_periodo': 'trimestral', 'anio': anio,
                            'trimestre': numero_trimestre(m.group(1)), **datos
                        })
                        cantidad += 1
            revision.append([pdf.name, 'OK' if cantidad else 'SIN_FILAS', cantidad])
        except Exception as e:
            revision.append([pdf.name, 'ERROR: ' + str(e), 0])
    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    with SALIDA.open('w', newline='', encoding='utf-8') as f:
        campos = ['fuente_pdf', 'pagina', 'tipo_periodo', 'anio', 'trimestre', *COLUMNAS]
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        writer.writerows(registros)
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['pdf', 'estado', 'filas_extraidas'])
        writer.writerows(revision)

    print('PDF revisados:', len(revision))
    print('Filas extraidas:', len(registros))
    print('Estados:', dict(Counter(r[1] for r in revision)))
    print('CSV:', SALIDA)
    print('Control:', LOG)
def extraer_indicadores_reportes():
    import hashlib
    import io
    import json
    carpeta = BASE / 'data/extracted/txt/osiptel_reportes'
    if not carpeta.is_dir():
        raise FileNotFoundError(f'No existe la carpeta TXT de reportes OSIPTEL: {carpeta}')
    reglas = [
        ('8653216-reporte-estadistico-n-04.txt', 2026, 1, 'participacion_fibra',
         r'fibra optica.{0,240}?representar el\s+(\d{1,3}[.,]\d{2})\s*%', 'porcentaje'),
        ('8653216-reporte-estadistico-n-04.txt', 2026, 1, 'conexiones_residenciales',
         r'segmento de internet residencial.{0,180}?alcanzo\s+(\d{1,3}(?:\s+\d{3}){1,2})\s+conexiones', 'conexiones'),
        ('8653216-reporte-estadistico-n-04.txt', 2026, 1, 'conexiones_comerciales',
         r'segmento de internet comercial.{0,200}?sumo\s+(\d{1,3}(?:\s+\d{3}){1,2})\s+conexiones', 'conexiones'),
        ('7802468-reporte-estadistico-n-01.txt', 2025, 4, 'participacion_fibra',
         r'fibra optica.{0,180}?representa el\s+(\d{1,3}[.,]\d{2})\s*%', 'porcentaje'),
        ('7802468-reporte-estadistico-n-01.txt', 2025, 4, 'conexiones_residenciales',
         r'segmento de internet residencial.{0,180}?alcanzaron las\s+(\d{1,3}(?:\s+\d{3}){1,2})\s+conexiones', 'conexiones'),
        ('7802468-reporte-estadistico-n-01.txt', 2025, 4, 'conexiones_comerciales',
         r'segmento de internet comercial.{0,200}?sumo\s+(\d{1,3}(?:\s+\d{3}){1,2})\s+conexiones', 'conexiones'),
        ('7802440-reporte-estadistico-n-07.txt', 2025, 3, 'conexiones_fijas',
         r'al cierre del tercer trimestre de 2025.{0,180}?registrar\s+(\d{1,3}(?:\s+\d{3}){1,2})\s+conexiones de internet fijo', 'conexiones'),
    ]
    def normalizar_con_mapa(texto):
        chars, indices = [], []
        for i, letra in enumerate(texto):
            for c in unicodedata.normalize('NFKD', letra.casefold()):
                if unicodedata.category(c) != 'Mn':
                    chars.append(c)
                    indices.append(i)
        return ''.join(chars), indices
    cache = {}
    filas = []
    for nombre, anio, trimestre, indicador, patron, unidad in reglas:
        if nombre not in cache:
            archivo = carpeta / nombre
            if not archivo.is_file():
                raise FileNotFoundError(f'No se encuentra el TXT requerido: {archivo}')
            original = ' '.join(archivo.read_text(encoding='utf-8', errors='replace').split())
            normalizado, mapa = normalizar_con_mapa(original)
            cache[nombre] = (original, normalizado, mapa)
        original, texto, mapa = cache[nombre]
        encontrados = list(re.finditer(patron, texto, flags=re.I))
        if not encontrados:
            raise ValueError(f'Sin coincidencia contextual: {nombre} / {indicador}')
        valores = {c.group(1) for c in encontrados}
        if len(valores) != 1:
            raise ValueError(f'Cifras contradictorias para {indicador} en {nombre}: {valores}')
        c = encontrados[0]
        cadena = c.group(1)
        valor = (float(cadena.replace(',', '.')) if unidad == 'porcentaje'
                 else int(re.sub(r'\D', '', cadena)))
        if unidad == 'porcentaje' and not 0 <= valor <= 100:
            raise ValueError(f'Porcentaje fuera de rango: {nombre}: {valor}')
        inicio = mapa[max(0, c.start() - 90)]
        fin = mapa[min(len(mapa) - 1, c.end() + 90)] + 1
        filas.append({
            'anio': anio, 'trimestre': trimestre, 'indicador': indicador,
            'valor': valor, 'unidad': unidad,
            'fuente_pdf': Path(nombre).with_suffix('.pdf').name,
            'fuente_txt': nombre, 'fragmento_fuente': original[inicio:fin],
            'metodo_extraccion': 'patron_contextual_verificado',
        })
    for anio, trimestre, nombre in [
        (2026, 1, '8653216-reporte-estadistico-n-04.txt'),
        (2025, 4, '7802468-reporte-estadistico-n-01.txt'),
    ]:
        segmentos = [f for f in filas if f['anio'] == anio and
                     f['trimestre'] == trimestre and
                     f['indicador'] in ('conexiones_residenciales', 'conexiones_comerciales')]
        if len(segmentos) != 2:
            raise ValueError(f'No se pudieron derivar los dos segmentos: {anio}-T{trimestre}')
        filas.append({
            'anio': anio, 'trimestre': trimestre, 'indicador': 'conexiones_fijas',
            'valor': sum(int(f['valor']) for f in segmentos),
            'unidad': 'conexiones',
            'fuente_pdf': Path(nombre).with_suffix('.pdf').name,
            'fuente_txt': nombre,
            'fragmento_fuente': ' | '.join(f['fragmento_fuente'] for f in segmentos),
            'metodo_extraccion': 'derivado_suma_segmentos',
        })

    claves = [(f['anio'], f['trimestre'], f['indicador']) for f in filas]
    if len(set(claves)) != len(claves):
        raise ValueError('Indicadores documentales repetidos por periodo')

    campos = ['anio', 'trimestre', 'indicador', 'valor', 'unidad',
              'fuente_pdf', 'fuente_txt', 'fragmento_fuente', 'metodo_extraccion']
    salida = io.StringIO(newline='')
    writer = csv.DictWriter(salida, fieldnames=campos, lineterminator='\n')
    writer.writeheader()
    writer.writerows(filas)
    contenido = salida.getvalue().encode('utf-8')
    digest = hashlib.sha256(contenido).hexdigest()
    nombre_csv = f'osiptel_reportes_indicadores_{digest[:12]}.csv'
    archivo_csv = SALIDA.parent / nombre_csv
    if archivo_csv.exists():
        if hashlib.sha256(archivo_csv.read_bytes()).hexdigest() != digest:
            raise RuntimeError(f'Conflicto de hash en el archivo: {archivo_csv}')
    else:
        archivo_csv.write_bytes(contenido)
    manifest = BASE / 'logs/osiptel_reportes_actual.json'
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps({
        'archivo_csv': nombre_csv, 'sha256': digest,
        'filas': len(filas), 'documentos': sorted(cache.keys()),
        'nota': 'Extraccion curada de documentos narrativos; no representa los 105 PDF',
    }, ensure_ascii=False, indent=2), encoding='utf-8')

    print('\nREPORTES OSIPTEL - ESTADISTICAS NARRATIVAS')
    for fila in sorted(filas, key=lambda r: (r['anio'], r['trimestre'], r['indicador'])):
        print(f"{fila['anio']}-T{fila['trimestre']} | {fila['indicador']} | "
              f"{fila['valor']} {fila['unidad']} [{fila['metodo_extraccion']}]")
    print('Documentos analizados:', len(cache))
    print('Indicadores estructurados:', len(filas))
    print('CSV versionado:', archivo_csv)
    print('Manifiesto:', manifest)
if __name__ == '__main__':
    extraer_inei()
    extraer_indicadores_reportes()