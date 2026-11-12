# historico_aire.py — carga ÚNICA del histórico horario de aire 2016–2022 ─
#
# Descarga los dos recursos CKAN del portal de datos abiertos de València
# (opendata.vlci.valencia.es) y los guarda en Parquet.
#
# Diferencias con el recolector:
#   - Es una descarga PUNTUAL: no se programa con cron, se ejecuta una vez.
#   - NO TOCA LOS VALORES: guarda TODO como texto tal cual llega la API,
#     porque los dos tramos tienen formato de fecha/hora distinto y hay
#     fechas con el día y el mes intercambiados. Arreglar eso es trabajo
#     del ETL (Bloque 4); aquí solo traemos el material.
#
# Lo único que se transforma son los NOMBRES de columna: minúsculas, sin
# tildes ni espacios (PM2.5 -> pm2_5), para que Spark no se queje.
#
# Idempotente: si los ficheros ya existen, NO repite la descarga.
# Para rehacer a la fuerza:  python3 historico_aire.py --forzar
#
# Uso:    python3 historico_aire.py            # descarga lo que falte
#         python3 historico_aire.py --forzar   # rehace todo
#         python3 historico_aire.py --solo 2021_2022

import argparse
import re
import sys
import time
import unicodedata
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

# ── Configuración ────────────────────────────────────────────────────────

BASE_URL = "https://opendata.vlci.valencia.es/api/3/action/datastore_search"

# Los dos tramos del histórico, verificados el 29/10/2026 en el portal:
#   2016–2021 · 449.026 filas · paquete "Datos horarios de calidad del aire desde 2016"
#   2021–2022 · ~201.000 filas · paquete "Datos por horas calidad aire 2021-2022"
RECURSOS = {
    "2016_2021": "4be7248b-9597-4017-89af-82a9b6e2382f",
    "2021_2022": "19b5a1a7-5888-4d3e-b69b-3c1436d64e6e",
}

# Filas por página: CKAN pagina el datastore y hay que pedir lote a
# lote; 10.000 es una cifra cómoda para no disparar miles de peticiones
LIMIT = 10000

DESTINO = Path("data/raw/calidad_aire/historico")


# ── Normalización de nombres de columna ─────────────────────────────────

def nombre_columna(col: str) -> str:
    """PM2.5 -> pm2_5 ; 'Dia de la semana' -> dia_de_la_semana.

    Minúsculas, tildes eliminadas (NO2 -> no2) y todo lo que no sea
    letra/número se convierte en un guion bajo. Es el nombre con el que
    Spark leerá esta columna en el Bloque 4.
    """
    texto = unicodedata.normalize("NFKD", str(col))   # separa NO2 -> NO2
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = re.sub(r"[^0-9a-z]+", "_", texto.lower())
    return texto.strip("_")


# ── Descarga paginada de un recurso ─────────────────────────────────────

def descargar_recurso(resource_id: str) -> list[dict]:
    """Recorre el datastore entero con limit/offset y devuelve la lista de filas.

    La API paga por páginas: cuando un lote trae menos filas que LIMIT,
    es que no hay más páginas. Cada petición reintenta 2 extra antes de rendirse.
    """
    registros: list[dict] = []
    offset = 0
    while True:
        params = {"resource_id": resource_id, "limit": LIMIT, "offset": offset}
        for intento in range(3):
            try:
                respuesta = requests.get(BASE_URL, params=params, timeout=60)
                respuesta.raise_for_status()
                break
            except requests.RequestException as error:
                if intento == 2:
                    raise
                time.sleep(5 * (intento + 1))   # 5s, 10s
        datos = respuesta.json()["result"]
        lote = datos["records"]
        registros += lote
        print(f"    página descargada: {len(registros):,} filas acumuladas",
              flush=True)
        if len(lote) < LIMIT:
            return registros           # última página: full
        offset += len(lote)


# ── Guardado en Parquet (valores en texto, tal cual) ────────────────────

def guardar_parquet(registros: list[dict], origen: str) -> Path:
    """Escribe <DESTINO>/origen=<tramo>/historico_aire_<tramo>.parquet.

    Todos los valores se emiten en texto, tal cual llegaron de la API
    (los nulos van como cadena vacía). No se convierte ninguna fecha ni
    ningún número: eso lo hará el ETL cuando ya sepa dónde están los feos.
    """
    # clave total: la unión de todas las claves, manteniendo el ORDEN de
    # la primera fila (todas las filas de un mismo datastore usan las
    # mismas claves)
    claves: list[str] = []
    for fila in registros:
        for k in fila:
            if k not in claves:
                claves.append(k)

    # Cada columna se fabrica como lista de str, en el mismo orden de filas
    columnas = {}
    for k in claves:
        columnas[nombre_columna(k)] = pa.array(
            [str(fila.get(k)) if fila.get(k) is not None else "" for fila in registros],
            type=pa.string(),
        )

    tabla = pa.table(columnas)
    carpeta = DESTINO / f"origen={origen}"
    carpeta.mkdir(parents=True, exist_ok=True)
    destino = carpeta / f"historico_aire_{origen}.parquet"
    pq.write_table(tabla, destino, compression="snappy")
    return destino


# ── Programa principal ──────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--forzar", action="store_true",
                        help="redescarga aunque los parquet ya existan")
    parser.add_argument("--solo", choices=sorted(RECURSOS),
                        help="descarga solo este tramo")
    args = parser.parse_args()

    tramos = [args.solo] if args.solo else sorted(RECURSOS)
    for tramo in tramos:
        destino = DESTINO / f"origen={tramo}" / f"historico_aire_{tramo}.parquet"
        if destino.exists() and not args.forzar:
            print(f"[{tramo}] ya existe ({destino}); no descarga de nuevo. "
                  "Usad --forzar para rehacer.")
            continue
        print(f"[{tramo}] descargando recurso {RECURSOS[tramo]}…", flush=True)
        t0 = time.time()
        registros = descargar_recurso(RECURSOS[tramo])
        fichero = guardar_parquet(registros, tramo)
        print(f"[{tramo}] {len(registros):,} filas -> {fichero} "
              f"({time.time() - t0:.0f}s)", flush=True)


if __name__ == "__main__":
    main()
