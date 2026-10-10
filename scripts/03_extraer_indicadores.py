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