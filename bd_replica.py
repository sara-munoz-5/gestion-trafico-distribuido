"""
sensor_gps.py — Sensor GPS (PC1)
Genera EVENTO_DENSIDAD_DE_TRAFICO.

Nivel de congestión (según enunciado):
    ALTA   → velocidad_promedio < 10 km/h
    NORMAL → 10 ≤ velocidad_promedio ≤ 39 km/h
    BAJA   → velocidad_promedio > 40 km/h

Uso:
    python sensor_gps.py --idx 0
    python sensor_gps.py --idx 1 --intervalo 5
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


mensajes_enviados = 0


def nivel_congestion(velocidad: float) -> str:
    """Clasifica el nivel de congestión a partir de la velocidad estimada."""
    if velocidad < 10:
        return "ALTA"
    elif velocidad <= 39:
        return "NORMAL"
    else:
        return "BAJA"


def generar_evento(sensor_id: str, interseccion: str) -> dict:
    """Genera una lectura GPS sintética para una intersección."""
    velocidad = round(random.uniform(5.0, 60.0), 1)
    return {
        "sensor_id":          sensor_id,
        "tipo_sensor":        "gps",
        "interseccion":       interseccion,
        "nivel_congestion":   nivel_congestion(velocidad),
        "velocidad_promedio": velocidad,
        "timestamp":          datetime.now(timezone.utc).isoformat(),
        "ts_origen_ns":       time.perf_counter_ns(),
    }


def main():
    """Inicializa el sensor GPS y publica lecturas en el tópico gps."""
    parser = argparse.ArgumentParser(description="Sensor GPS")
    parser.add_argument("--config",    default="../config.json")
    parser.add_argument("--idx",       type=int, default=0)
    parser.add_argument("--intervalo", type=int, default=None)
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)

    s            = cfg["sensores"]["gps"][args.idx]
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
        socket.send_string(f"gps {json.dumps(evento)}")
        mensajes_enviados += 1
        print(f"[{sensor_id}] {ts_now()} | seq={mensajes_enviados} | "
              f"Nivel={evento['nivel_congestion']} | Vel={evento['velocidad_promedio']} km/h")
        time.sleep(intervalo)


if __name__ == "__main__":
    main()
