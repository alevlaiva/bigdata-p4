# recolector.py — captura de datos abiertos urbanos de Valencia (P4) ────
#
# Descarga una "foto" de cada fuente abierta (tráfico, calidad del aire,
# meteo municipal) del Geoportal de Valencia, más la predicción de AEMET
# (fuente "meteo"), y la guarda en Parquet.
# Este script NO importa nada del resto del proyecto: se copia tal cual
# al servidor, que solo necesita Python >= 3.9 con `requests`, `pyarrow`
# y `python-dotenv` (ver requirements.txt).
#
# Uso:    python recolector.py                # recoge TODAS las fuentes
#         python recolector.py espiras aire   # recoge solo esas fuentes
#
# Se ejecuta SIEMPRE desde la raíz del proyecto (las rutas data/, logs/
# son relativas a donde se lanza el comando).

import csv
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

# Carga AEMET_API_KEY (y otras variables) desde el fichero .env, o del
# entorno si la variable ya existe. .env NUNCA va en Git (ver .gitignore).
load_dotenv()

# ── Configuración de fuentes ─────────────────────────────────────────────
#
# Todas las capas del Geoportal se consultan igual; solo cambia el nombre
# de la capa. "ruta" es la carpeta donde se guarda cada foto, por fuente.
GEOPORTAL = "https://geoportal.valencia.es/server/rest/services/OPENDATA"

# AEMET OpenData: no es una capa del Geoportal sino una API con clave,
# por eso su entrada lleva "ruta" pero no "capa".
AEMET_API = "https://opendata.aemet.es/opendata/api"

# Código INE del municipio de Valencia (capital)
MUNICIPIO_VALENCIA = "46250"

FUENTES = {
    "espiras": {"capa": "Trafico/MapServer/208",       "ruta": "trafico/espiras"},
    "tramos":  {"capa": "Trafico/MapServer/188",       "ruta": "trafico/tramos"},
    "estado":  {"capa": "Trafico/MapServer/192",       "ruta": "trafico/estado"},
    "aire":    {"capa": "MedioAmbiente/MapServer/156", "ruta": "calidad_aire/estaciones"},
    "meteo_municipal": {"capa": "MedioAmbiente/MapServer/157", "ruta": "meteo/municipal"},
    "meteo":   {"ruta": "meteo/aemet"},   # Meteorología = AEMET (como en el enunciado)
}

# Nombres antiguos aceptados por compatibilidad (el cron ya instalado usa
# "aemet"): se resuelven al nombre canónico antes de recoger.
ALIAS = {"aemet": "meteo"}

# Carpeta donde cae cada foto, y fichero de registro de ejecuciones.
# Las fotos en Parquet son los datos; el CSV de ejecuciones es la pista
# que usaremos en el Bloque 4 para medir huecos y detectar fallos.
DATA_DIR = Path("data/raw")
REGISTRO = DATA_DIR / "_registro" / "ejecuciones.csv"

log = logging.getLogger("recolector")


# ── Consulta HTTP con reintentos ─────────────────────────────────────────

def get_con_reintentos(url: str, params: dict = None, headers: dict = None,
                       reintentos: int = 3) -> requests.Response:
    """GET con reintentos ante errores transitorios: red, timeouts, 429 y 5xx.

    Solo se reintentan los errores que tienen sentido reintentar (desconexión,
    timeout, 429 de rate limit, 5xx del servidor). Un 404 o un error de
    petición inválida se elevan al momento: reintentar no los arregla.
    `headers` sirve para casos como AEMET, donde la clave viaja en cabecera.
    """
    for intento in range(reintentos):
        try:
            respuesta = requests.get(url, params=params, headers=headers,
                                     timeout=60)
            if respuesta.status_code == 429 or respuesta.status_code >= 500:
                # 429/5xx son transitorios: los convertimos en excepción
                # para que caiga en el mismo reloj de reintentos
                raise requests.HTTPError(f"HTTP {respuesta.status_code}")
            respuesta.raise_for_status()   # otros 4xx: error inmediato
            return respuesta
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as error:
            if intento == reintentos - 1:
                raise                      # último intento: el error sube
            espera = 10 * (intento + 1)    # espera creciente: 10s, 20s, 30s
            log.warning(
                "intento %d/%d falló (%s); reintentando en %ds",
                intento + 1, reintentos, type(error).__name__, espera,
            )
            time.sleep(espera)


# ── Descarga de una capa del Geoportal (con paginación) ─────────────────

def descargar_capa(capa: str) -> list[dict]:
    """Devuelve TODOS los registros (atributos) de una capa ArcGIS.

    El servicio limita el número de registros por respuesta: si la respuesta
    viene con la marca `exceededTransferLimit` en True, hay más páginas.
    Entonces repetimos la consulta desplazando `resultOffset` (cuántas filas
    saltarse) hasta que la marca desaparece y la capa está completa.
    """
    url = f"{GEOPORTAL}/{capa}/query"
    params = {"where": "1=1", "outFields": "*", "f": "json", "resultOffset": 0}
    registros: list[dict] = []
    while True:
        datos = get_con_reintentos(url, params).json()
        # Cada feature viene como {"geometry": ..., "attributes": {...}};
        # para la ingesta batch nos interesan los atributos (la geometría
        # se aplanará en el ETL del Bloque 4 cuando haga falta)
        registros += [f["attributes"] for f in datos.get("features", [])]
        if not datos.get("exceededTransferLimit"):
            return registros          # no hay más páginas: capa completa
        params["resultOffset"] = len(registros)  # siguiente lote


def descargar_aemet() -> list[dict]:
    """Predicción diaria de Valencia vía AEMET OpenData, en DOS pasos.

    PASO 1: GET al endpoint con la cabecera `api_key`. La respuesta NO trae
    la predicción: devuelve un JSON con `estado` y en `datos` una segunda
    URL temporal (caduca y cambia en cada llamada).
    PASO 2: GET a esa URL temporal, que sí contiene la predicción.

    La respuesta es una lista con un elemento; de cada día pronosticado
    (`prediccion.dia[]`) extraemos fecha, temperatura máxima y mínima, y la
    probabilidad de precipitación del día completo (tramo "00-24").
    Devuelve UNA FILA POR DÍA: cada captura genera varias filas.
    """
    api_key = os.getenv("AEMET_API_KEY")
    if not api_key:
        raise RuntimeError("Falta AEMET_API_KEY (crea un .env con esa variable)")

    peticion = (f"{AEMET_API}/prediccion/especifica/municipio/diaria/"
                f"{MUNICIPIO_VALENCIA}")

    # PASO 1: la clave viaja en la cabecera `api_key`, no por parámetro
    meta = get_con_reintentos(peticion, headers={"api_key": api_key}).json()
    if meta.get("estado") != 200:
        raise RuntimeError(f"AEMET devolvió un error: {meta.get('descripcion')}")

    # PASO 2: la URL temporal, ya sin cabecera especial
    pronostico = get_con_reintentos(meta["datos"]).json()

    filas = []
    for dia in pronostico[0]["prediccion"]["dia"]:
        temp = dia.get("temperatura", {})
        # `probPrecipitacion` trae varios tramos horarios ("00-12",
        # "12-24"...); buscamos el del día completo ("00-24")
        prob_dia = next(
            (p.get("value") for p in dia.get("probPrecipitacion", [])
             if p.get("periodo") == "00-24"),
            None,   # si AEMET lo omite algún día, mejor un nulo que inventar
        )
        filas.append({
            # la fecha viene como "2026-10-29T00:00:00": nos quedamos con
            # la parte de fecha (los caracteres 0..10)
            "fecha": dia["fecha"][:10],
            "temperatura_maxima": temp.get("maxima"),
            "temperatura_minima": temp.get("minima"),
            "probabilidad_precipitacion": prob_dia,
        })
    return filas


# ── Guardado y registro de cada captura ─────────────────────────────────

def guardar_parquet(registros: list[dict], ruta: str, fuente: str,
                    captura: datetime) -> Path:
    """Escribe <DATA_DIR>/<ruta>/fecha=AAAA-MM-DD/<fuente>_<...>Z.parquet.

    Particionar por fecha es la idea del enunciado: en el Data Lake, Spark
    podrá filtrar por día leyendo solo la partición que necesita. Dentro
    del particionado, cada fichero lleva la hora de la captura en el
    nombre, así dos capturas del mismo día no se pisan entre sí.
    """
    tabla = pa.Table.from_pylist(registros)
    # Columna con la hora de captura UTC: fija la "foto" en el tiempo
    # aunque el dato en sí no traiga fecha fiable (ocurre en tráfico)
    tabla = tabla.append_column(
        "captura_utc", pa.array([captura] * tabla.num_rows)
    )
    carpeta = DATA_DIR / ruta / f"fecha={captura:%Y-%m-%d}"
    carpeta.mkdir(parents=True, exist_ok=True)
    destino = carpeta / f"{fuente}_{captura:%Y%m%dT%H%M%SZ}.parquet"
    pq.write_table(tabla, destino, compression="snappy")
    return destino


def registrar(captura: datetime, fuente: str, ok: bool, filas: int,
              error: str = "") -> None:
    """Añade una línea a data/raw/_registro/ejecuciones.csv.

    Es el "diario de bitácora" de cada ejecución: si algún día faltan datos
    (p. ej. el servidor se cayó 3 horas), este CSV es la fuente de verdad
    para saber qué pasó y cuánto duró el hueco.
    """
    REGISTRO.parent.mkdir(parents=True, exist_ok=True)
    nuevo = not REGISTRO.exists()
    with REGISTRO.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if nuevo:                     # cabecera solo la primera vez
            w.writerow(["captura_utc", "fuente", "ok", "filas", "error"])
        w.writerow([captura.isoformat(), fuente, int(ok), filas, error[:300]])


# ── Punto de entrada: recoger una fuente entera ─────────────────────────

def recoger(nombre: str) -> None:
    """Descarga, guarda y registra UNA fuente.

    Cada fuente corre dentro de su propio try: si una explota, se registra
    el fallo y el resto de fuentes continúan (fallos independientes), tal
    y como pide el enunciado.
    """
    nombre = ALIAS.get(nombre, nombre)
    captura = datetime.now(timezone.utc)
    try:
        fuente = FUENTES[nombre]
        if nombre == "meteo":
            # AEMET es el caso especial: API propia con clave y dos llamadas
            registros, ruta = descargar_aemet(), fuente["ruta"]
        else:
            registros, ruta = descargar_capa(fuente["capa"]), fuente["ruta"]
        destino = guardar_parquet(registros, ruta, nombre, captura)
        registrar(captura, nombre, True, len(registros))
        log.info("%s: %d filas -> %s", nombre, len(registros), destino)
    except Exception as e:  # sin estrechar: cualquier error se apunta al CSV
        registrar(captura, nombre, False, 0, str(e))
        log.exception("Fallo en %s", nombre)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    # Dame las fuentes pedidas por parámetro; si no hay, todas.
    fuentes = [ALIAS.get(f, f) for f in sys.argv[1:]] or list(FUENTES)
    # Validamos TODOS los nombres primero para no dejar la ejecución a medias
    # por una errata (p. ej. "espira" en vez de "espiras")
    desconocidas = [f for f in fuentes if f not in FUENTES]
    if desconocidas:
        sys.exit(f"Fuentes desconocidas: {', '.join(desconocidas)} "
                 f"(válidas: {', '.join(FUENTES)})")
    for nombre in fuentes:
        recoger(nombre)
