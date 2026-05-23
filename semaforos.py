"""
bd_replica.py — Base de Datos Réplica (PC2)

Recibe eventos vía PULL del Servicio de Analítica y los persiste en SQLite.
Cuando PC3 está caído, también atiende consultas REP del Servicio de Monitoreo.

Uso:
    python bd_replica.py
    python bd_replica.py --config ../../config.json --db ./replica.sqlite
"""
import zmq
import json
import sqlite3
import threading
import argparse
import signal
import sys
import os
import time

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)
from shared.modelos import ts_now


def normalizar_interseccion(valor: str) -> str:
    """Normaliza identificadores de cruce para aceptar A1 e INT_A1."""
    txt = (valor or "").strip().upper()
    if not txt:
        return ""
    if txt.startswith("INT_"):
        return txt
    if len(txt) == 2 and txt[0].isalpha() and txt[1].isdigit():
        return f"INT_{txt}"
    return txt


def init_db(path: str) -> sqlite3.Connection:
    """Abre la BD réplica y asegura la creación del esquema requerido."""
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS eventos (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo             TEXT,
            topico           TEXT,
            interseccion     TEXT,
            estado_detectado TEXT,
            timestamp        TEXT,
            ts_origen_ns     INTEGER,
            ts_entrada_ns    INTEGER,
            ts_salida_ns     INTEGER,
            lat_total_us     REAL,
            datos_json       TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS priorizaciones (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo         TEXT,
            fila         TEXT,
            interseccion TEXT,
            accion       TEXT,
            timestamp    TEXT,
            ts_cmd_ns    INTEGER,
            origen       TEXT
        )
    """)
    conn.commit()
    return conn


class ServicioBDReplica:
    """Servicio de réplica con ingestión PULL y consultas de contingencia."""

    def __init__(self, cfg: dict, db_path: str = "bd_replica.sqlite"):
        """Inicializa conexión SQLite, sockets ZeroMQ y estado de ejecución."""
        self.cfg   = cfg
        self.red   = cfg["red"]
        self.conn  = init_db(db_path)
        self._lock = threading.Lock()

        self.ctx  = zmq.Context()

        # PULL: recibe datos de Analítica
        self.pull = self.ctx.socket(zmq.PULL)
        self.pull.bind(f"tcp://*:{self.red['bd_replica_port']}")

        # REP: atiende consultas cuando PC3 está caído
        self.rep = self.ctx.socket(zmq.REP)
        self.rep.bind(f"tcp://*:{self.red['bd_replica_rep_port']}")

        self.activo = True
        print(f"[BD-REPLICA] {ts_now()} — Iniciada en {db_path}")

    def insertar(self, datos: dict) -> None:
        """Inserta eventos/priorizaciones replicados desde Analítica."""
        ts_llegada_ns = time.perf_counter_ns()
        with self._lock:
            if datos.get("tipo") == "evento_sensor":
                ts_origen = datos.get("ts_origen_ns")
                inter = normalizar_interseccion(datos.get("interseccion", ""))
                lat_us = ((ts_llegada_ns - ts_origen) / 1000) if ts_origen else None
                self.conn.execute(
                    "INSERT INTO eventos "
                    "(tipo,topico,interseccion,estado_detectado,timestamp,"
                    "ts_origen_ns,ts_entrada_ns,ts_salida_ns,lat_total_us,datos_json) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (datos["tipo"], datos.get("topico",""),
                     inter, datos.get("estado_detectado",""),
                     datos.get("timestamp",""),
                     ts_origen,
                     datos.get("ts_entrada_ns"),
                     datos.get("ts_salida_ns"),
                     lat_us,
                     json.dumps(datos.get("datos",{})))
                )
            elif datos.get("tipo") == "priorizacion":
                inter = normalizar_interseccion(datos.get("interseccion", ""))
                self.conn.execute(
                    "INSERT INTO priorizaciones "
                    "(tipo,fila,interseccion,accion,timestamp,ts_cmd_ns,origen) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (datos["tipo"], datos.get("fila",""),
                     inter, datos.get("accion",""),
                     datos.get("timestamp",""),
                     datos.get("ts_cmd_ns"),
                     datos.get("origen",""))
                )
            self.conn.commit()
        print(f"[BD-REPLICA] ✔ {datos.get('tipo')} @ {datos.get('interseccion','')}")

    def consultar(self, cmd: dict) -> dict:
        """Resuelve consultas para fallback cuando PC3 no está disponible."""
        tipo = cmd.get("tipo")

        if tipo == "PING":
            return {"status": "PONG", "fuente": "REPLICA"}

        elif tipo == "HISTORICO":
            ini = cmd.get("inicio", "")
            fin = cmd.get("fin",    "9999")
            with self._lock:
                rows = self.conn.execute(
                    "SELECT id,topico,interseccion,estado_detectado,timestamp "
                    "FROM eventos WHERE timestamp BETWEEN ? AND ? ORDER BY timestamp",
                    (ini, fin)
                ).fetchall()
            return {"status": "OK", "fuente": "REPLICA",
                    "total": len(rows), "eventos": [list(r) for r in rows]}

        elif tipo == "INTERSECCION":
            inter = normalizar_interseccion(cmd.get("interseccion", ""))
            alt = inter.replace("INT_", "", 1) if inter.startswith("INT_") else inter
            with self._lock:
                row = self.conn.execute(
                    "SELECT * FROM eventos "
                    "WHERE UPPER(TRIM(interseccion)) IN (?, ?) "
                    "ORDER BY timestamp DESC LIMIT 1", (inter, alt)
                ).fetchone()
            return {"status": "OK", "fuente": "REPLICA",
                    "dato": list(row) if row else None}

        elif tipo == "CONGESTIONES":
            with self._lock:
                rows = self.conn.execute(
                    "SELECT id,interseccion,timestamp FROM eventos "
                    "WHERE estado_detectado='CONGESTION' ORDER BY timestamp"
                ).fetchall()
            return {"status": "OK", "congestiones": [list(r) for r in rows]}

        elif tipo == "PRIORIZACIONES":
            with self._lock:
                rows = self.conn.execute(
                    "SELECT * FROM priorizaciones ORDER BY timestamp"
                ).fetchall()
            return {"status": "OK", "priorizaciones": [list(r) for r in rows]}

        elif tipo == "EVENTOS_RECIENTES":
            inter = normalizar_interseccion(cmd.get("interseccion", ""))
            alt = inter.replace("INT_", "", 1) if inter.startswith("INT_") else inter
            with self._lock:
                rows = self.conn.execute(
                    "SELECT * FROM eventos "
                    "WHERE UPPER(TRIM(interseccion)) IN (?, ?) "
                    "ORDER BY timestamp DESC LIMIT 10", (inter, alt)
                ).fetchall()
            return {
                "status": "OK",
                "fuente": "REPLICA",
                "eventos": [list(r) for r in rows],
            }

        return {"status": "ERROR", "detalle": "Tipo desconocido"}

    def hilo_pull(self) -> None:
        """Consume mensajes entrantes de Analítica y los persiste en SQLite."""
        while self.activo:
            try:
                msg   = self.pull.recv_string(flags=zmq.NOBLOCK)
                datos = json.loads(msg)
                self.insertar(datos)
            except zmq.ZMQError:
                time.sleep(0.05)
            except Exception as e:
                print(f"[BD-REPLICA] ERROR insertar: {e}")

    def hilo_rep(self) -> None:
        """Atiende consultas REQ/REP provenientes del monitoreo."""
        while self.activo:
            try:
                msg  = self.rep.recv_string(flags=zmq.NOBLOCK)
                cmd  = json.loads(msg)
                resp = self.consultar(cmd)
                self.rep.send_string(json.dumps(resp))
            except zmq.ZMQError:
                time.sleep(0.05)
            except Exception as e:
                print(f"[BD-REPLICA] ERROR rep: {e}")

    def iniciar(self) -> None:
        """Arranca hilos PULL/REP y mantiene el servicio vivo hasta interrupción."""
        hilos = [
            threading.Thread(target=self.hilo_pull, daemon=True),
            threading.Thread(target=self.hilo_rep,  daemon=True),
        ]
        for h in hilos:
            h.start()

        def apagar(sig, frame):
            print("\n[BD-REPLICA] Deteniendo...")
            self.activo = False
            self.conn.close()
            sys.exit(0)

        signal.signal(signal.SIGINT, apagar)
        for h in hilos:
            h.join()


def main():
    """Punto de entrada CLI para iniciar la BD réplica."""
    parser = argparse.ArgumentParser(description="BD Réplica")
    parser.add_argument("--config", default="../config.json")
    parser.add_argument("--db",     default="bd_replica.sqlite")
    args = parser.parse_args()
    with open(args.config) as f:
        cfg = json.load(f)
    ServicioBDReplica(cfg, args.db).iniciar()


if __name__ == "__main__":
    main()
