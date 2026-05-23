"""
medir_experimento.py — Script de medición para los experimentos de la Tabla 1
Implementa el protocolo descrito en la sección 11 del informe:
  - Warm-up de 10 segundos
  - Ventana de observación de 120 segundos
  - Cálculo de throughput (registros en BD / 120s)
  - Cálculo de latencia extremo-a-extremo (sensor → BD)
  - Validación de integridad: BD Principal == BD Réplica

Uso:
    python medir_experimento.py --escenario A --diseño base
    python medir_experimento.py --escenario B --diseño multihilo
    python medir_experimento.py --solo-integridad
"""
import zmq
import json
import sqlite3
import time
import argparse
import sys
import os

ROOT = os.path.join(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

WARMUP_SEG   = 10
VENTANA_SEG  = 120
SEP = "─" * 60


def contar_bd(path: str) -> int:
    """Cuenta los registros actuales en una BD SQLite local."""
    try:
        conn = sqlite3.connect(path)
        n = conn.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]
        conn.close()
        return n
    except Exception:
        return -1


def consultar_integridad_remota(cfg: dict, fuente: str = "PRINCIPAL") -> dict:
    """Consulta métricas de integridad y latencia a la BD vía REQ/REP."""
    red = cfg["red"]
    puerto = red["monitoreo_rep_port"]
    ip     = red["pc3_ip"] if fuente == "PRINCIPAL" else "127.0.0.1"

    ctx  = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, 5000)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f"tcp://{ip}:{puerto}")
    try:
        sock.send_string(json.dumps({"tipo": "INTEGRIDAD"}))
        resp = json.loads(sock.recv_string())
    except zmq.ZMQError:
        resp = {"status": "ERROR", "detalle": "Timeout"}
    finally:
        sock.close()
        ctx.term()
    return resp


def medir_latencia_semaforo(cfg: dict) -> float:
    """
    Mide la latencia desde que el operador envía FORZAR_VERDE
    hasta que recibe confirmación de Analítica (REQ/REP).
    Retorna latencia en milisegundos.
    """
    red  = cfg["red"]
    ctx  = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, 8000)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f"tcp://{red['pc2_ip']}:{red['analitica_rep_port']}")

    t0 = time.perf_counter()
    try:
        sock.send_string(json.dumps({"tipo": "FORZAR_VERDE", "interseccion": "INT_B2"}))
        sock.recv_string()
        lat_ms = (time.perf_counter() - t0) * 1000
    except zmq.ZMQError:
        lat_ms = -1
    finally:
        sock.close()
        ctx.term()
    return round(lat_ms, 2)


def ejecutar_experimento(cfg: dict, escenario: str, diseño: str,
                          bd_p_path: str, bd_r_path: str) -> None:
    """Ejecuta protocolo completo de medición y exporta resultados a CSV."""
    print(f"\n{SEP}")
    print(f"  EXPERIMENTO — Escenario: {escenario} | Diseño: {diseño}")
    print(f"{SEP}")

    # ── Warm-up ──────────────────────────────────────────────────────────────
    print(f"  [1/4] Warm-up de {WARMUP_SEG}s...")
    time.sleep(WARMUP_SEG)

    # ── Conteo inicial ────────────────────────────────────────────────────────
    cnt_ini_p = contar_bd(bd_p_path)
    cnt_ini_r = contar_bd(bd_r_path)
    print(f"  [2/4] Inicio: BD_Principal={cnt_ini_p} | BD_Réplica={cnt_ini_r}")
    print(f"  [3/4] Ventana de observación ({VENTANA_SEG}s)...")

    t_inicio = time.perf_counter()

    # Medir latencias de semáforo durante la ventana (cada 20s)
    latencias = []
    for i in range(VENTANA_SEG // 20):
        time.sleep(20)
        lat = medir_latencia_semaforo(cfg)
        if lat > 0:
            latencias.append(lat)
            print(f"       Latencia semáforo muestra {i+1}: {lat} ms")

    # Esperar el resto de la ventana si quedó tiempo
    transcurrido = (time.perf_counter() - t_inicio)
    if transcurrido < VENTANA_SEG:
        time.sleep(VENTANA_SEG - transcurrido)

    # ── Conteo final ──────────────────────────────────────────────────────────
    cnt_fin_p = contar_bd(bd_p_path)
    cnt_fin_r = contar_bd(bd_r_path)

    throughput_p = cnt_fin_p - cnt_ini_p
    throughput_r = cnt_fin_r - cnt_ini_r
    tps_p = round(throughput_p / VENTANA_SEG, 2)

    # Latencia extremo-a-extremo desde la BD
    integridad = consultar_integridad_remota(cfg)

    # ── Resultados ────────────────────────────────────────────────────────────
    print(f"\n{SEP}")
    print(f"  RESULTADOS — {escenario} / {diseño}")
    print(f"{SEP}")
    print(f"  Throughput BD_Principal : {throughput_p} registros en {VENTANA_SEG}s")
    print(f"  Throughput BD_Réplica   : {throughput_r} registros en {VENTANA_SEG}s")
    print(f"  TPS (req/s)             : {tps_p}")
    print(f"  Pérdida de mensajes     : {'0' if throughput_p == throughput_r else abs(throughput_p - throughput_r)} eventos")
    print(f"  Integridad (P==R)       : {'✔ IGUALES' if throughput_p == throughput_r else '✘ DIFERENCIA DETECTADA'}")

    if integridad.get("status") == "OK":
        print(f"  Latencia E2E promedio   : {integridad.get('lat_avg_ms')} ms")
        print(f"  Latencia E2E mínima     : {integridad.get('lat_min_ms')} ms")
        print(f"  Latencia E2E máxima     : {integridad.get('lat_max_ms')} ms")

    if latencias:
        avg = round(sum(latencias) / len(latencias), 2)
        print(f"  Latencia semáforo prom  : {avg} ms")
        print(f"  Latencia semáforo min   : {min(latencias)} ms")
        print(f"  Latencia semáforo max   : {max(latencias)} ms")

    # Guardar CSV para análisis posterior
    csv_path = f"resultados_{escenario}_{diseño}.csv"
    with open(csv_path, "w") as f:
        f.write("escenario,diseño,throughput_2min,tps,lat_e2e_avg_ms,"
                "lat_e2e_min_ms,lat_e2e_max_ms,lat_sem_avg_ms,integridad\n")
        f.write(f"{escenario},{diseño},{throughput_p},{tps_p},"
                f"{integridad.get('lat_avg_ms','')},{integridad.get('lat_min_ms','')},"
                f"{integridad.get('lat_max_ms','')},{avg if latencias else ''},"
                f"{'OK' if throughput_p == throughput_r else 'ERROR'}\n")
    print(f"\n  Resultados guardados en: {csv_path}")
    print(f"{SEP}\n")


def verificar_integridad_solo(bd_p: str, bd_r: str) -> None:
    """Compara BD Principal vs BD Réplica para la prueba de PUSH/PULL."""
    print(f"\n{SEP}")
    print("  VERIFICACIÓN DE INTEGRIDAD BD Principal vs BD Réplica")
    print(f"{SEP}")

    cnt_p = contar_bd(bd_p)
    cnt_r = contar_bd(bd_r)

    print(f"  BD Principal : {cnt_p} eventos")
    print(f"  BD Réplica   : {cnt_r} eventos")
    print(f"  Diferencia   : {abs(cnt_p - cnt_r)}")
    print(f"  Estado       : {'✔ IDÉNTICAS' if cnt_p == cnt_r else '✘ DIFERENCIA DETECTADA'}")

    if cnt_p == cnt_r and cnt_p > 0:
        # Comparar últimos 5 registros
        try:
            conn_p = sqlite3.connect(bd_p)
            conn_r = sqlite3.connect(bd_r)
            rows_p = conn_p.execute(
                "SELECT interseccion,estado_detectado,timestamp FROM eventos "
                "ORDER BY id DESC LIMIT 5").fetchall()
            rows_r = conn_r.execute(
                "SELECT interseccion,estado_detectado,timestamp FROM eventos "
                "ORDER BY id DESC LIMIT 5").fetchall()
            conn_p.close(); conn_r.close()
            coinciden = rows_p == rows_r
            print(f"  Últimos 5 registros: {'✔ Coinciden' if coinciden else '✘ Difieren'}")
        except Exception as e:
            print(f"  Error al comparar: {e}")
    print(f"{SEP}\n")


def main():
    """Punto de entrada CLI para correr medición completa o solo integridad."""
    parser = argparse.ArgumentParser(description="Medición de experimentos Tabla 1")
    parser.add_argument("--config",   default="config.json")
    parser.add_argument("--escenario", choices=["A","B"], default="A",
                        help="A=1 sensor/tipo 10s | B=2 sensores/tipo 5s")
    parser.add_argument("--diseño",   choices=["base","multihilo"], default="base")
    parser.add_argument("--bd-p",     default="pc3/bd_principal.sqlite",
                        help="Ruta local a la BD Principal")
    parser.add_argument("--bd-r",     default="pc2/bd_replica.sqlite",
                        help="Ruta local a la BD Réplica")
    parser.add_argument("--solo-integridad", action="store_true",
                        help="Solo verifica integridad entre BD Principal y Réplica")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)

    if args.solo_integridad:
        verificar_integridad_solo(args.bd_p, args.bd_r)
    else:
        ejecutar_experimento(cfg, args.escenario, args.diseño,
                             args.bd_p, args.bd_r)


if __name__ == "__main__":
    main()
