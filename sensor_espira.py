"""
broker.py — Broker ZeroMQ (PC1)
Recibe eventos de los sensores (XSUB) y los redistribuye al Servicio de Analítica (XPUB).
Usa zmq.proxy() para operar como intermediario sin lógica de negocio.

Uso:
    python broker.py
    python broker.py --config ../../config.json
"""
import zmq
import json
import argparse
import sys
import signal
from datetime import datetime

import os; ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def main():
    """Carga configuración, levanta sockets XSUB/XPUB y ejecuta el proxy ZMQ."""
    parser = argparse.ArgumentParser(description="Broker ZMQ")
    parser.add_argument("--config", default="../config.json")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)
    red = cfg["red"]

    ctx = zmq.Context()

    # XSUB: los sensores se conectan aquí como PUB
    xsub = ctx.socket(zmq.XSUB)
    xsub.bind(f"tcp://*:{red['broker_pub_port']}")

    # XPUB: el Servicio de Analítica se conecta aquí como SUB
    xpub = ctx.socket(zmq.XPUB)
    xpub.bind(f"tcp://*:{red['broker_sub_port']}")

    print(f"[BROKER] {datetime.now().isoformat(timespec='seconds')} — Iniciado")
    print(f"[BROKER] Sensores  → puerto {red['broker_pub_port']}")
    print(f"[BROKER] Analítica → puerto {red['broker_sub_port']}")

    def apagar(sig, frame):
        print("\n[BROKER] Detenido.")
        xsub.close(); xpub.close(); ctx.term()
        sys.exit(0)

    signal.signal(signal.SIGINT, apagar)

    # Proxy bloqueante: reenvía todos los mensajes XSUB → XPUB
    zmq.proxy(xsub, xpub)


if __name__ == "__main__":
    main()
