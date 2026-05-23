"""
reglas.py — Reglas de evaluación del estado de tráfico (PC2)
Implementa los tres estados definidos en el documento del grupo:
  - NORMAL:      Q < 5  AND Vp > 35 AND V < 15
  - CONGESTION:  Q >= 5 OR  Vp <= 20 OR  V >= 20
  - PRIORIZACION: activado únicamente por comando directo del operador
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared.modelos import EstadoTrafico


class EvaluadorReglas:
    """Motor de reglas para clasificar intersecciones en NORMAL o CONGESTION."""

    def __init__(self, cfg: dict):
        """Inicializa umbrales de decisión desde el archivo de configuración."""
        r = cfg["reglas"]
        # Umbrales NORMAL
        self.normal_Q_max  = r["normal"]["Q_max"]    # 5
        self.normal_Vp_min = r["normal"]["Vp_min"]   # 35
        self.normal_V_max  = r["normal"]["V_max"]     # 15
        # Umbrales CONGESTION
        self.cong_Q_min    = r["congestion"]["Q_min"] # 5
        self.cong_Vp_max   = r["congestion"]["Vp_max"]# 20
        self.cong_V_min    = r["congestion"]["V_min"] # 20

    def evaluar(self, Q=None, Vp=None, V=None,
                nivel_gps: str = None,
                forzado: EstadoTrafico = None) -> EstadoTrafico:
        """
        Evalúa el estado con los datos disponibles.
        Si 'forzado' no es None, ese estado tiene prioridad absoluta (comando del operador).

        Parámetros:
            Q         — longitud de cola (cámara, nº vehículos)
            Vp        — velocidad promedio km/h (cámara o GPS)
            V         — conteo vehicular en ventana (espira)
            nivel_gps — "ALTA" / "NORMAL" / "BAJA" (GPS)
            forzado   — EstadoTrafico si el operador forzó un estado
        """
        if forzado is not None:
            return forzado

        # Velocidad del GPS complementa Vp si no viene de cámara
        if Vp is None and nivel_gps:
            Vp = {"ALTA": 8.0, "NORMAL": 25.0, "BAJA": 45.0}.get(nivel_gps)

        # Detectar CONGESTION: basta con que UNA variable supere el umbral
        if Q  is not None and Q  >= self.cong_Q_min:
            return EstadoTrafico.CONGESTION
        if Vp is not None and Vp <= self.cong_Vp_max:
            return EstadoTrafico.CONGESTION
        if V  is not None and V  >= self.cong_V_min:
            return EstadoTrafico.CONGESTION

        # Detectar NORMAL: todas las variables disponibles deben cumplir
        es_normal = True
        if Q  is not None and Q  >= self.normal_Q_max:
            es_normal = False
        if Vp is not None and Vp <= self.normal_Vp_min:
            es_normal = False
        if V  is not None and V  >= self.normal_V_max:
            es_normal = False

        return EstadoTrafico.NORMAL if es_normal else EstadoTrafico.CONGESTION
