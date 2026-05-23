"""
sensor_espira.py — Sensor de Espira Inductiva (PC1)
Genera EVENTO_CONTEO_VEHICULAR y lo publica al Broker ZMQ.

Uso:
    python sensor_espira.py --idx 0          # primer sensor del config
    python sensor_espira.py --idx 1 --intervalo 5
"""
import zmq
import json
import time
import random
import argparse
import signal
import sys
import os
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from shared.modelos import ts_now

VEHICULOS_MIN = 2
VEHICULOS_MAX = 30
VENTANA_SEG   = 30   # ventana de conteo fija según el enunciado

# Contador global de mensajes enviados (para validación de integridad)
mensajes_enviados = 0


def generar_evento(sensor_id: str, interseccion: str) -> dict:
    """Genera una muestra sintética de conteo vehicular para una intersección."""
    ts_ini = datetime.now(timezone.utc).isoformat()
    # perf_counter_ns: timestamp de alta precisión en nanosegundos para latencia
    return {
        "sensor_id":          sensor_id,
        "tipo_sensor":        "espira_inductiva",
        "interseccion":       interseccion,
        "vehiculos_contados": random.randint(VEHICULOS_MIN, VEHICULOS_MAX),
        "intervalo_segundos": VENTANA_SEG,
        "timestamp_inicio":   ts_ini,
        "timestamp_fin":      ts_ini,
        "ts_origen_ns":       time.perf_counter_ns(),  # precisión nanosegundos
    }


def main():
    """Inicializa el sensor, publica eventos periódicos y maneja apagado seguro."""
    parser = argparse.ArgumentParser(description="Sensor Espira Inductiva")
    parser.add_argument("--config",      default="../config.json")
    parser.add_argument("--idx",         type=int, default=0,
                        help="Índice del sensor en config.sensores.espiras")
    parser.add_argument("--intervalo",   type=int, default=None,
                        help="Sobreescribe el intervalo del config (segundos)")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)

    s           = cfg["sensores"]["espiras"][args.idx]
    sensor_id   = s["sensor_id"]
    interseccion = s["interseccion"]
    intervalo   = args.intervalo or s["intervalo_seg"]
    broker_ip   = cfg["red"]["pc1_ip"]
    broker_port = cfg["red"]["broker_pub_port"]

    ctx    = zmq.Context()
    socket = ctx.socket(zmq.PUB)
    socket.connect(f"tcp://{broker_ip}:{broker_port}")
    time.sleep(0.5)   # warm-up de conexión ZMQ

    def apagar(sig, frame):
        print(f"\n[{sensor_id}] Detenido.")
        socket.close(); ctx.term(); sys.exit(0)

    signal.signal(signal.SIGINT, apagar)
    print(f"[{sensor_id}] Iniciado | intersección={interseccion} | intervalo={intervalo}s")

    global mensajes_enviados
    while True:
        evento = generar_evento(sensor_id, interseccion)
        socket.send_string(f"espira {json.dumps(evento)}")
        mensajes_enviados += 1
        # Log estructurado: [timestamp] | sensor | seq | dato clave
        print(f"[{sensor_id}] {ts_now()} | seq={mensajes_enviados} | "
              f"{evento['vehiculos_contados']} vehículos contados")
        time.sleep(intervalo)


if __name__ == "__main__":
    main()
