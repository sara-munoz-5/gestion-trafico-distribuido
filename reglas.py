"""
analitica.py — Servicio de Analítica (PC2)

Responsabilidades:
  1. Suscribirse a eventos del Broker ZMQ (SUB)
  2. Evaluar reglas de tráfico → detectar NORMAL / CONGESTION
  3. Enviar comandos al Servicio de Semáforos (PUSH)
  4. Persistir eventos en BD Principal (PUSH → PC3) y BD Réplica (PUSH → PC2)
  5. Atender consultas/comandos del Servicio de Monitoreo (REP)
  6. Health check de PC3 cada 5s; si falla 3 veces → usar réplica

Uso:
    python analitica.py
    python analitica.py --config ../../config.json
"""
import zmq
import json
import time
import threading
import argparse
import signal
import sys
import os
from collections import defaultdict

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, ROOT)

from shared.modelos import EstadoTrafico, ComandoSemaforo, ts_now
from reglas import EvaluadorReglas


class ServicioAnalitica:
    """Procesa eventos de sensores, decide estado y orquesta persistencia/control."""

    def __init__(self, cfg: dict):
        """Configura sockets, cache de intersecciones y política de heartbeat."""
        self.cfg       = cfg
        self.red       = cfg["red"]
        self.evaluador = EvaluadorReglas(cfg)

        # Estado de fallo de PC3
        self.pc3_caido  = False
        self.fallos_pc3 = 0
        self.max_fallos = cfg["heartbeat"]["max_fallos"]
        self.hb_intervalo = cfg["heartbeat"]["intervalo_seg"]

        # Cache: últimos valores por intersección para evaluación combinada
        self.cache: dict = defaultdict(dict)

        # ── Sockets ZMQ ────────────────────────────────────────────────────
        self.ctx = zmq.Context()

        # SUB → recibe eventos del Broker
        self.sub = self.ctx.socket(zmq.SUB)
        self.sub.connect(f"tcp://{self.red['pc1_ip']}:{self.red['broker_sub_port']}")
        for topico in ("espira", "camara", "gps"):
            self.sub.setsockopt_string(zmq.SUBSCRIBE, topico)

        # PUSH → Servicio de Control de Semáforos
        self.push_sem = self.ctx.socket(zmq.PUSH)
        self.push_sem.bind(f"tcp://*:{self.red['semaforos_port']}")

        # PUSH → BD Principal (PC3)
        self.push_bdp = self.ctx.socket(zmq.PUSH)
        self.push_bdp.connect(
            f"tcp://{self.red['pc3_ip']}:{self.red['bd_principal_port']}")

        # PUSH → BD Réplica (PC2 local)
        self.push_bdr = self.ctx.socket(zmq.PUSH)
        self.push_bdr.connect(
            f"tcp://127.0.0.1:{self.red['bd_replica_port']}")

        # REP → Servicio de Monitoreo (consultas y comandos)
        self.rep = self.ctx.socket(zmq.REP)
        self.rep.bind(f"tcp://*:{self.red['analitica_rep_port']}")

        self.activo = True
        print(f"[ANALITICA] {ts_now()} — Servicio de Analítica iniciado")

    # ── Persistencia ────────────────────────────────────────────────────────

    def persistir(self, datos: dict) -> None:
        """Envía el registro a las dos BD (o solo réplica si PC3 está caído)."""
        msg = json.dumps(datos)
        if not self.pc3_caido:
            try:
                self.push_bdp.send_string(msg, zmq.NOBLOCK)
            except zmq.ZMQError:
                print("[ANALITICA] WARN: No se pudo enviar a BD Principal")
        try:
            self.push_bdr.send_string(msg, zmq.NOBLOCK)
        except zmq.ZMQError:
            print("[ANALITICA] WARN: No se pudo enviar a BD Réplica")

    # ── Control de semáforos ────────────────────────────────────────────────

    def cmd_semaforo(self, interseccion: str, accion: str,
                     duracion: int = 15, origen: str = "ANALITICA") -> None:
        """Construye y publica un comando de semáforo al servicio de control."""
        cmd = ComandoSemaforo(interseccion, accion, duracion, origen)
        try:
            self.push_sem.send_string(cmd.to_json(), zmq.NOBLOCK)
        except zmq.ZMQError:
            pass
        print(f"[ANALITICA] → Semáforo {interseccion}: {accion} "
              f"({duracion}s) [{origen}]")

    # ── Procesamiento de eventos ─────────────────────────────────────────────

    def procesar(self, topico: str, payload: str) -> None:
        """Integra lectura de sensor, evalúa reglas y persiste el resultado."""
        # ts_entrada: momento en que Analítica recibe el evento (perf_counter)
        ts_entrada_ns = time.perf_counter_ns()

        try:
            evento = json.loads(payload)
        except json.JSONDecodeError:
            print("[ANALITICA] ERROR: JSON inválido")
            return

        inter        = evento.get("interseccion", "DESCONOCIDA")
        ts_origen_ns = evento.get("ts_origen_ns")   # viene del sensor

        # Log estructurado de entrada
        print(f"[ANALITICA] {ts_now()} | ENTRADA | [{topico.upper()}] {inter}")

        # Actualizar cache de la intersección con los nuevos valores
        c = self.cache[inter]
        if topico == "camara":
            c["Q"]  = evento.get("volumen")
            c["Vp"] = evento.get("velocidad_promedio")
        elif topico == "espira":
            c["V"]  = evento.get("vehiculos_contados")
        elif topico == "gps":
            c["nivel_gps"] = evento.get("nivel_congestion")
            c["Vp"]        = evento.get("velocidad_promedio")

        # Evaluar estado combinado
        estado = self.evaluador.evaluar(
            Q=c.get("Q"), Vp=c.get("Vp"),
            V=c.get("V"), nivel_gps=c.get("nivel_gps"))

        sem = self.cfg["semaforos"]
        if estado == EstadoTrafico.NORMAL:
            print(f"[ANALITICA] {inter} → NORMAL — ciclo estándar 15s")
            self.cmd_semaforo(inter, "VERDE", sem["tiempo_verde_normal_seg"])
        else:
            print(f"[ANALITICA] {inter} → ⚠ CONGESTIÓN — extendiendo verde a 30s")
            self.cmd_semaforo(inter, "EXTENDER_VERDE",
                              sem["tiempo_verde_congestion_seg"])

        # ts_salida: momento en que Analítica termina de procesar
        ts_salida_ns = time.perf_counter_ns()

        # Latencia broker→analítica (solo si el sensor envió ts_origen_ns)
        lat_broker_us = None
        if ts_origen_ns:
            lat_broker_us = (ts_entrada_ns - ts_origen_ns) / 1000  # µs

        print(f"[ANALITICA] {inter} | SALIDA | procesamiento="
              f"{(ts_salida_ns - ts_entrada_ns)//1000}µs"
              + (f" | lat_broker={lat_broker_us:.0f}µs" if lat_broker_us else ""))

        # Persistir en ambas BD con timestamps para cálculo de latencia extremo a extremo
        self.persistir({
            "tipo":             "evento_sensor",
            "topico":           topico,
            "interseccion":     inter,
            "estado_detectado": estado.value,
            "timestamp":        ts_now(),
            "ts_origen_ns":     ts_origen_ns,       # del sensor
            "ts_entrada_ns":    ts_entrada_ns,       # llegada a Analítica
            "ts_salida_ns":     ts_salida_ns,        # fin de procesamiento
            "datos":            evento,
        })

    # ── Manejo de consultas REQ/REP ──────────────────────────────────────────

    def manejar_consulta(self, msg: str) -> dict:
        """Gestiona comandos del operador recibidos por REQ/REP desde Monitoreo."""
        try:
            cmd = json.loads(msg)
        except Exception:
            return {"status": "ERROR", "detalle": "JSON inválido"}

        tipo = cmd.get("tipo")
        ts   = ts_now()

        if tipo == "PING":
            return {"status": "PONG"}

        elif tipo == "FORZAR_VERDE":
            inter = cmd["interseccion"]
            self.cmd_semaforo(inter, "VERDE", 60, "OPERADOR")
            self.persistir({"tipo": "priorizacion", "interseccion": inter,
                            "accion": "VERDE", "timestamp": ts, "origen": "OPERADOR"})
            print(f"[ANALITICA] 🔴→🟢 OPERADOR forzó VERDE en {inter}")
            return {"status": "OK", "interseccion": inter}

        elif tipo == "FORZAR_ROJO":
            inter = cmd["interseccion"]
            self.cmd_semaforo(inter, "ROJO", 60, "OPERADOR")
            self.persistir({"tipo": "priorizacion", "interseccion": inter,
                            "accion": "ROJO", "timestamp": ts, "origen": "OPERADOR"})
            print(f"[ANALITICA] 🟢→🔴 OPERADOR forzó ROJO en {inter}")
            return {"status": "OK", "interseccion": inter}

        elif tipo == "PRIORIZAR_VIA":
            fila      = cmd["fila"].upper()
            columnas  = self.cfg["ciudad"]["columnas"]
            afectados = [f"INT_{fila}{col}" for col in columnas]
            for inter in afectados:
                self.cmd_semaforo(inter, "VERDE", 120, "OPERADOR")
            self.persistir({"tipo": "priorizacion", "fila": fila,
                            "intersecciones": afectados, "timestamp": ts,
                            "origen": "OPERADOR"})
            print(f"[ANALITICA] 🚑 OLA VERDE fila {fila}: {afectados}")
            return {"status": "OK", "fila": fila, "afectados": afectados}

        elif tipo == "RESTABLECER":
            # Volver todos los semáforos al ciclo normal
            filas   = self.cfg["ciudad"]["filas"]
            cols    = self.cfg["ciudad"]["columnas"]
            t_verde = self.cfg["semaforos"]["tiempo_verde_normal_seg"]
            for f in filas:
                for c in cols:
                    self.cmd_semaforo(f"INT_{f}{c}", "VERDE", t_verde, "OPERADOR")
            print("[ANALITICA] ↺ Sistema restablecido a ciclo normal")
            return {"status": "OK", "mensaje": "Sistema restablecido"}

        elif tipo == "ESTADO_CACHE":
            return {"status": "OK", "cache": dict(self.cache)}

        return {"status": "ERROR", "detalle": f"Tipo desconocido: {tipo}"}

    # ── Hilos de ejecución ───────────────────────────────────────────────────

    def hilo_sub(self) -> None:
        """Escucha eventos del Broker."""
        while self.activo:
            try:
                msg = self.sub.recv_string(flags=zmq.NOBLOCK)
                partes = msg.split(" ", 1)
                if len(partes) == 2:
                    self.procesar(partes[0], partes[1])
            except zmq.ZMQError:
                time.sleep(0.05)

    def hilo_rep(self) -> None:
        """Atiende solicitudes REQ/REP del Servicio de Monitoreo."""
        while self.activo:
            try:
                msg  = self.rep.recv_string(flags=zmq.NOBLOCK)
                resp = self.manejar_consulta(msg)
                self.rep.send_string(json.dumps(resp))
            except zmq.ZMQError:
                time.sleep(0.05)

    def hilo_heartbeat(self) -> None:
        """Verifica cada hb_intervalo segundos si PC3 sigue disponible."""
        while self.activo:
            time.sleep(self.hb_intervalo)
            ctx_hb = zmq.Context()
            sock   = ctx_hb.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER,  0)
            sock.setsockopt(zmq.RCVTIMEO, 3000)
            sock.connect(
                f"tcp://{self.red['pc3_ip']}:{self.red['monitoreo_rep_port']}")
            try:
                sock.send_string(json.dumps({"tipo": "PING"}))
                sock.recv_string()
                if self.pc3_caido:
                    print("[ANALITICA] ✅ PC3 recuperado — volviendo a BD Principal")
                    self.pc3_caido  = False
                    self.fallos_pc3 = 0
            except zmq.ZMQError:
                self.fallos_pc3 += 1
                if self.fallos_pc3 >= self.max_fallos and not self.pc3_caido:
                    self.pc3_caido = True
                    print("[ANALITICA] ❌ PC3 CAÍDO — usando BD Réplica como primaria")
                    # Reconectar push_bdp hacia la réplica
                    try:
                        self.push_bdp.disconnect(
                            f"tcp://{self.red['pc3_ip']}:{self.red['bd_principal_port']}")
                    except Exception:
                        pass
                    self.push_bdp.connect(
                        f"tcp://127.0.0.1:{self.red['bd_replica_port']}")
            finally:
                sock.close()
                ctx_hb.term()

    def iniciar(self) -> None:
        """Lanza hilos de suscripción, REP y heartbeat; bloquea hasta terminar."""
        hilos = [
            threading.Thread(target=self.hilo_sub,       daemon=True, name="sub"),
            threading.Thread(target=self.hilo_rep,       daemon=True, name="rep"),
            threading.Thread(target=self.hilo_heartbeat, daemon=True, name="hb"),
        ]
        for h in hilos:
            h.start()

        def apagar(sig, frame):
            print("\n[ANALITICA] Deteniendo...")
            self.activo = False
            sys.exit(0)

        signal.signal(signal.SIGINT, apagar)
        print("[ANALITICA] Hilos activos. Ctrl+C para detener.")
        for h in hilos:
            h.join()


def main():
    """Punto de entrada CLI para ejecutar el servicio de analítica."""
    parser = argparse.ArgumentParser(description="Servicio de Analítica")
    parser.add_argument("--config", default="../config.json")
    args = parser.parse_args()
    with open(args.config) as f:
        cfg = json.load(f)
    ServicioAnalitica(cfg).iniciar()


if __name__ == "__main__":
    main()
