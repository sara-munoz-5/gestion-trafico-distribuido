"""
sensor_camara.py — Sensor de Cámara de Tráfico (PC1)
Genera EVENTO_LONGITUD_COLA (volumen en espera + velocidad promedio).

Uso:
    python sensor_camara.py --idx 0
    python sensor_camara.py --idx 2 --intervalo 5
"""
import zmq
import json
import time
import random
import argparse
import signal
import sys
from datetime import datetime, timezone

import os; ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from shared.modelos import ts_now

VOLUMEN_MIN   = 0
VOLUMEN_MAX   = 20
VELOCIDAD_MIN = 5.0
VELOCIDAD_MAX = 50.0

mensajes_enviados = 0


def generar_evento(sensor_id: str, interseccion: str) -> dict:
    """Genera una muestra de cámara con cola y velocidad promedio simuladas."""
    volumen = random.randint(VOLUMEN_MIN, VOLUMEN_MAX)
    vel_max = max(VELOCIDAD_MIN, VELOCIDAD_MAX - volumen * 2.0)
    velocidad = round(random.uniform(VELOCIDAD_MIN, vel_max), 1)
    return {
        "sensor_id":          sensor_id,
        "tipo_sensor":        "camara",
        "interseccion":       interseccion,
        "volumen":            volumen,
        "velocidad_promedio": velocidad,
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "ts_origen_ns":       time.perf_counter_ns(),
    }


def main():
    """Inicializa el sensor de cámara y publica mediciones periódicas por PUB."""
    parser = argparse.ArgumentParser(description="Sensor Cámara de Tráfico")
    parser.add_argument("--config",    default="../config.json")
    parser.add_argument("--idx",       type=int, default=0)
    parser.add_argument("--intervalo", type=int, default=None)
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)

    s            = cfg["sensores"]["camaras"][args.idx]
    sensor_id    = s["sensor_id"]
    interseccion = s["interseccion"]
    intervalo    = args.intervalo or s["intervalo_seg"]
    broker_ip    = cfg["red"]["pc1_ip"]
    broker_port  = cfg["red"]["broker_pub_port"]

    ctx    = zmq.Context()
    socket = ctx.socket(zmq.PUB)
    socket.connect(f"tcp://{broker_ip}:{broker_port}")
    time.sleep(0.5)

    def apagar(sig, frame):
        print(f"\n[{sensor_id}] Detenido.")
        socket.close(); ctx.term(); sys.exit(0)

    signal.signal(signal.SIGINT, apagar)
    print(f"[{sensor_id}] Iniciado | intersección={interseccion} | intervalo={intervalo}s")

    global mensajes_enviados
    while True:
        evento = generar_evento(sensor_id, interseccion)
        socket.send_string(f"camara {json.dumps(evento)}")
        mensajes_enviados += 1
        print(f"[{sensor_id}] {ts_now()} | seq={mensajes_enviados} | "
              f"Cola={evento['volumen']} veh | Vel={evento['velocidad_promedio']} km/h")
        time.sleep(intervalo)


if __name__ == "__main__":
    main()
