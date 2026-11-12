"""setup_nifi.py — deja listo el flujo NiFi de la P4 sin usar la interfaz.

Con NiFi ya arrancado (docker compose up -d), este script:
  1. espera a que NiFi responda y obtiene un token con admin / NIFI_PASSWORD;
  2. importa nifi/flow.json como Process Group (si ya existe, lo reutiliza);
  3. escribe la contraseña del almacén de confianza en el servicio SSL
     (NiFi no la exporta) y lo habilita;
  4. arranca el grupo (salvo con --no-arrancar).

Uso (desde la raíz del repo):  ./.venv/bin/python nifi/setup_nifi.py

Lee NIFI_PASSWORD del .env y la contraseña del almacén de
data/tmp/trust_geoportal_2026.pass; no imprime ninguna de las dos.
NiFi usa un certificado autofirmado en localhost, por eso se desactiva la
verificación TLS, y solo para esta dirección local.
"""

import argparse
import os
import sys
import time
from pathlib import Path

import requests
import urllib3
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parents[1]
BASE = "https://localhost:8443/nifi-api"   # solo local: se verifica abajo
NOMBRE_GRUPO = "P4 - Ingesta espiras (NiFi)"
# "flow" es el nombre que NiFi da al grupo al importarlo desde la interfaz
NOMBRES_VALIDOS = {NOMBRE_GRUPO, "flow"}
NOMBRE_SERVICIO = "Confianza Geoportal ACCV"

assert BASE.startswith("https://localhost:"), "este script solo es para NiFi local"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def conectar(usuario: str, clave: str, espera: int = 240) -> requests.Session:
    """Reintenta hasta que NiFi arranca y devuelve una sesión con el token."""
    s = requests.Session()
    s.verify = False  # certificado autofirmado de NiFi en localhost
    limite = time.time() + espera
    while True:
        try:
            r = s.post(f"{BASE}/access/token",
                       data={"username": usuario, "password": clave}, timeout=15)
            if r.status_code == 201:
                s.headers["Authorization"] = f"Bearer {r.text}"
                return s
            if r.status_code in (400, 401):
                sys.exit("NiFi rechazó el usuario o la contraseña (revisa NIFI_PASSWORD en .env).")
        except requests.RequestException:
            pass
        if time.time() > limite:
            sys.exit("NiFi no responde en https://localhost:8443 (¿está arrancado el contenedor?).")
        print("esperando a NiFi…", flush=True)
        time.sleep(10)


def grupo_existente(s, raiz: str) -> str | None:
    r = s.get(f"{BASE}/process-groups/{raiz}/process-groups")
    r.raise_for_status()
    for g in r.json()["processGroups"]:
        if g["component"]["name"] in NOMBRES_VALIDOS:
            return g["id"]
    return None


def importar(s, raiz: str, flujo: Path) -> str:
    with flujo.open("rb") as f:
        r = s.post(f"{BASE}/process-groups/{raiz}/process-groups/upload",
                   data={"groupName": NOMBRE_GRUPO, "positionX": "100",
                         "positionY": "100", "clientId": "setup_nifi"},
                   files={"file": ("flow.json", f, "application/json")})
    r.raise_for_status()
    return r.json()["id"]


def preparar_servicio(s, grupo: str, clave_almacen: str) -> None:
    r = s.get(f"{BASE}/flow/process-groups/{grupo}/controller-services")
    r.raise_for_status()
    servicios = [c for c in r.json()["controllerServices"]
                 if c["component"]["name"] == NOMBRE_SERVICIO]
    if not servicios:
        sys.exit(f"No encuentro el servicio '{NOMBRE_SERVICIO}' en el flujo importado.")
    sid = servicios[0]["id"]

    actual = s.get(f"{BASE}/controller-services/{sid}").json()
    if actual["component"]["state"] != "ENABLED":
        r = s.put(f"{BASE}/controller-services/{sid}", json={
            "revision": actual["revision"],
            "component": {"id": sid,
                          "properties": {"Truststore Password": clave_almacen}}})
        r.raise_for_status()
        rev = r.json()["revision"]
        r = s.put(f"{BASE}/controller-services/{sid}/run-status",
                  json={"revision": rev, "state": "ENABLED"})
        r.raise_for_status()
    for _ in range(30):
        c = s.get(f"{BASE}/controller-services/{sid}").json()["component"]
        if c["state"] == "ENABLED":
            print(f"servicio '{NOMBRE_SERVICIO}': ENABLED ({c.get('validationStatus')})")
            return
        time.sleep(1)
    sys.exit("El servicio SSL no llegó a habilitarse (revisa la contraseña del almacén).")


def arrancar(s, grupo: str) -> None:
    time.sleep(2)  # deja que NiFi revalide los procesadores con el servicio ya activo
    r = s.put(f"{BASE}/flow/process-groups/{grupo}",
              json={"id": grupo, "state": "RUNNING"})
    r.raise_for_status()
    print("flujo arrancado: InvokeHTTP se ejecuta ahora y luego cada hora.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--flujo", type=Path, default=RAIZ / "nifi" / "flow.json")
    ap.add_argument("--clave-almacen", type=Path,
                    default=RAIZ / "data" / "tmp" / "trust_geoportal_2026.pass")
    ap.add_argument("--no-arrancar", action="store_true",
                    help="importa y habilita el servicio, pero no arranca el flujo")
    args = ap.parse_args()

    load_dotenv(RAIZ / ".env")
    clave = os.getenv("NIFI_PASSWORD")
    if not clave:
        sys.exit("Falta NIFI_PASSWORD en .env")
    if not args.clave_almacen.exists():
        sys.exit(f"No existe {args.clave_almacen} (contraseña del almacén de confianza).")

    s = conectar(os.getenv("NIFI_USER", "admin"), clave)
    raiz = s.get(f"{BASE}/flow/process-groups/root").json()["processGroupFlow"]["id"]

    grupo = grupo_existente(s, raiz)
    if grupo:
        print("el flujo ya está importado en NiFi: se reutiliza")
    else:
        grupo = importar(s, raiz, args.flujo)
        print(f"flujo importado como grupo '{NOMBRE_GRUPO}'")

    preparar_servicio(s, grupo, args.clave_almacen.read_text().strip())
    if not args.no_arrancar:
        arrancar(s, grupo)


if __name__ == "__main__":
    main()
