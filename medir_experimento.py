{
  "ciudad": {
    "filas": ["A", "B", "C", "D"],
    "columnas": [1, 2, 3, 4]
  },
  "sensores": {
    "espiras": [
      {"sensor_id": "ESP-A1", "interseccion": "INT_A1", "intervalo_seg": 5},
      {"sensor_id": "ESP-B3", "interseccion": "INT_B3", "intervalo_seg": 5},
      {"sensor_id": "ESP-C2", "interseccion": "INT_C2", "intervalo_seg": 5},
      {"sensor_id": "ESP-D4", "interseccion": "INT_D4", "intervalo_seg": 5}
    ],
    "camaras": [
      {"sensor_id": "CAM-A3", "interseccion": "INT_A3", "intervalo_seg": 5},
      {"sensor_id": "CAM-B2", "interseccion": "INT_B2", "intervalo_seg": 5},
      {"sensor_id": "CAM-C4", "interseccion": "INT_C4", "intervalo_seg": 5},
      {"sensor_id": "CAM-D1", "interseccion": "INT_D1", "intervalo_seg": 5}
    ],
    "gps": [
      {"sensor_id": "GPS-A2", "interseccion": "INT_A2", "intervalo_seg": 5},
      {"sensor_id": "GPS-B4", "interseccion": "INT_B4", "intervalo_seg": 5},
      {"sensor_id": "GPS-C1", "interseccion": "INT_C1", "intervalo_seg": 5},
      {"sensor_id": "GPS-D3", "interseccion": "INT_D3", "intervalo_seg": 5}
    ]
  },
  "red": {
    "pc1_ip": "127.0.0.1",
    "pc2_ip": "127.0.0.1",
    "pc3_ip": "127.0.0.1",
    "broker_pub_port": 5555,
    "broker_sub_port": 5556,
    "semaforos_port": 5558,
    "bd_replica_port": 5557,
    "bd_principal_port": 5559,
    "analitica_rep_port": 5560,
    "monitoreo_rep_port": 5561,
    "bd_replica_rep_port": 5562
  },
  "semaforos": {
    "tiempo_verde_normal_seg": 15,
    "tiempo_rojo_normal_seg": 15,
    "tiempo_verde_congestion_seg": 30,
    "tiempo_rojo_congestion_seg": 10
  },
  "reglas": {
    "normal":     {"Q_max": 5,  "Vp_min": 35, "V_max": 15},
    "congestion": {"Q_min": 5,  "Vp_max": 20, "V_min": 20}
  },
  "heartbeat": {
    "intervalo_seg": 5,
    "max_fallos": 3
  }
}
