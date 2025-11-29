#!/usr/bin/env python3
"""
Script parametrizable para insertar métricas predefinidas en InfluxDB (v2) usando line protocol.

Características:
- Parámetros de Influx por defecto (editables en DEFAULT_INFLUX_CONFIG).
- Métricas definidas como diccionario { "NombreMetrica": "line_protocol_sin_timestamp" }.
- Menú interactivo que muestra:
    Metricas a insertar:
    1. Metrica1
    2. Metrica2
- Opción para elegir métrica por CLI (--metric) o enviar todas (--metric all).
- El script agrega automáticamente el epoch actual en nanosegundos al final de cada línea.
"""

from __future__ import annotations

import os
import sys
import time
import argparse
from dataclasses import dataclass
from typing import Dict, List

import requests


# ==========================
# 1) CONFIGURACIÓN POR DEFECTO
# ==========================

DEFAULT_INFLUX_CONFIG = {
    "url": "http://localhost:8086",
    "token": "abcdefghijklmnopqrstuvwxyz1234567890",
    "org": "fpuna",
    "bucket": "ethereum-monitor",
    "timeout_sec": 10,
}

# Variables de entorno opcionales que pueden sobrescribir los defaults
ENV_VARS_MAP = {
    "url": "INFLUX_URL",
    "token": "INFLUX_TOKEN",
    "org": "INFLUX_ORG",
    "bucket": "INFLUX_BUCKET",
    "timeout_sec": "INFLUX_TIMEOUT",
}


@dataclass
class InfluxConfig:
    url: str
    token: str
    org: str
    bucket: str
    timeout_sec: int = 10


def load_influx_config_from_env_and_defaults() -> InfluxConfig:
    """
    Carga la configuración de Influx:
    1) Si existe variable de entorno -> usa esa
    2) Si no, usa DEFAULT_INFLUX_CONFIG
    """
    cfg: Dict[str, str] = {}
    for key, default_value in DEFAULT_INFLUX_CONFIG.items():
        env_var = ENV_VARS_MAP.get(key)
        if env_var and os.getenv(env_var) is not None:
            cfg[key] = os.getenv(env_var)
        else:
            cfg[key] = default_value

    cfg["timeout_sec"] = int(cfg.get("timeout_sec", 10))
    return InfluxConfig(**cfg)


# ==========================
# 2) MÉTRICAS PREDEFINIDAS (LINE PROTOCOL)
# ==========================

# 🔧 AQUÍ EDITÁS LAS MÉTRICAS PREDEFINIDAS
#
# Clave: Nombre de la métrica (se mostrará en el menú y se usa en --metric).
# Valor: Line protocol SIN timestamp (el script le agregará el epoch actual).
#
# Ejemplo de formato:
#   "cpu_usage,host=bastion,env=prod usage_percent=42.5"
#
# No incluyas el epoch al final; el script agregará " <epoch_ns>" automáticamente.
METRICS: Dict[str, str] = {
    "chain_reorg": "beacon_event,event_type=chain_reorg,source=localhost/mockEvent,network=mainnet,endpoint=/eth/v1/events count=1i",
    "proposer_slashing": "beacon_event,event_type=proposer_slashing,source=localhost/mockEvent,network=mainnet,endpoint=/eth/v1/events count=1i",
    "attester_slashing": "beacon_event,event_type=attester_slashing,source=localhost/mockEvent,network=mainnet,endpoint=/eth/v1/events count=1i"
    # Agrega aquí tus propias métricas:
    # "CPU_Usage": "cpu_usage,host=server01 usage_percent=73.2",
    # "Requests": "http_requests_total,service=backend,status=200 value=10i",
}


# ==========================
# 3) PARÁMETROS CLI
# ==========================

def parse_args(default_cfg: InfluxConfig) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inserta métricas predefinidas en InfluxDB usando line protocol."
    )

    parser.add_argument(
        "--url",
        default=default_cfg.url,
        help=f"URL de InfluxDB (por defecto: {default_cfg.url})",
    )
    parser.add_argument(
        "--token",
        default=default_cfg.token,
        help="Token de autenticación para InfluxDB (v2).",
    )
    parser.add_argument(
        "--org",
        default=default_cfg.org,
        help=f"Organización de InfluxDB (por defecto: {default_cfg.org})",
    )
    parser.add_argument(
        "--bucket",
        default=default_cfg.bucket,
        help=f"Bucket de InfluxDB (por defecto: {default_cfg.bucket})",
    )
    parser.add_argument(
        "--metric",
        choices=list(METRICS.keys()) + ["all"],
        help=(
            "Nombre de la métrica a enviar. "
            "Si no se indica, se mostrará un menú interactivo. "
            "Usa 'all' para enviar todas."
        ),
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Número de veces que se enviará cada métrica seleccionada (por defecto: 1).",
    )

    return parser.parse_args()


# ==========================
# 4) ESCRITURA A INFLUX (HTTP API v2)
# ==========================

def build_line_with_timestamp(base_line: str) -> str:
    """
    Recibe una línea en line protocol SIN timestamp y agrega el epoch actual
    en nanosegundos al final, como requiere Influx cuando se especifica timestamp.
    """
    epoch_ns = time.time_ns()
    return f"{base_line} {epoch_ns}"


def write_lines(cfg: InfluxConfig, lines: List[str]) -> None:
    """
    Envía una lista de líneas en line protocol a InfluxDB 2.x usando la API HTTP /api/v2/write
    con precisión en nanosegundos.
    """
    if not lines:
        return

    url = cfg.url.rstrip("/") + "/api/v2/write"
    params = {
        "org": cfg.org,
        "bucket": cfg.bucket,
        "precision": "ns",
    }
    headers = {
        "Authorization": f"Token {cfg.token}",
        "Content-Type": "text/plain; charset=utf-8",
    }
    data = "\n".join(lines)

    resp = requests.post(url, params=params, headers=headers, data=data, timeout=cfg.timeout_sec)

    if not resp.ok:
        print(f"[ERROR] Error al escribir en InfluxDB: {resp.status_code} {resp.text}", file=sys.stderr)
        resp.raise_for_status()
    else:
        print(f"[OK] Enviadas {len(lines)} línea(s) a InfluxDB.")


# ==========================
# 5) MENÚ INTERACTIVO
# ==========================

def show_menu_and_get_selection() -> List[str]:
    """
    Muestra el menú de métricas y devuelve una lista con los nombres de métricas seleccionadas.
    """
    metric_names = list(METRICS.keys())

    print("Métricas a enviar:")
    for idx, name in enumerate(metric_names, start=1):
        print(f"{idx}. {name}")
    print("a. Todas las métricas")
    print("q. Salir")
    print()

    while True:
        choice = input("Selecciona una opción (número, 'a' para todas, 'q' para salir): ").strip().lower()

        if choice == "q":
            print("Saliendo sin enviar métricas.")
            return []

        if choice == "a":
            return metric_names

        # Permitir múltiples números separados por coma: 1,3
        if "," in choice:
            try:
                indices = [int(x.strip()) for x in choice.split(",")]
                selected = []
                for i in indices:
                    if 1 <= i <= len(metric_names):
                        selected.append(metric_names[i - 1])
                    else:
                        raise ValueError
                return selected
            except ValueError:
                print("Entrada inválida. Intenta de nuevo.")
                continue

        # Un solo número
        try:
            idx = int(choice)
            if 1 <= idx <= len(metric_names):
                return [metric_names[idx - 1]]
            else:
                print("Opción fuera de rango. Intenta de nuevo.")
        except ValueError:
            print("Entrada inválida. Intenta de nuevo.")


# ==========================
# 6) LÓGICA PRINCIPAL
# ==========================

def send_selected_metrics(
    cfg: InfluxConfig,
    metric_names: List[str],
    repeat: int = 1,
) -> None:
    """
    Construye y envía las métricas seleccionadas el número de veces indicado.
    Cada envío agrega el epoch actual en nanosegundos.
    """
    if not metric_names:
        return

    for _ in range(repeat):
        lines_to_send: List[str] = []
        for name in metric_names:
            base_line = METRICS.get(name)
            if base_line is None:
                print(f"[WARN] Métrica '{name}' no está definida, se ignora.")
                continue

            full_line = build_line_with_timestamp(base_line)
            lines_to_send.append(full_line)
            print(f"[DEBUG] Construida línea para '{name}': {full_line}")

        if lines_to_send:
            write_lines(cfg, lines_to_send)


def main() -> None:
    # 1) Cargar configuración por defecto (env + constantes)
    default_cfg = load_influx_config_from_env_and_defaults()

    # 2) Parsear argumentos CLI
    args = parse_args(default_cfg)

    # 3) Config final (puedes agregar más campos si lo necesitás)
    cfg = InfluxConfig(
        url=args.url,
        token=args.token,
        org=args.org,
        bucket=args.bucket,
        timeout_sec=default_cfg.timeout_sec,
    )

    # 4) Seleccionar métricas
    if args.metric:
        if args.metric == "all":
            selected_metrics = list(METRICS.keys())
        else:
            selected_metrics = [args.metric]
    else:
        selected_metrics = show_menu_and_get_selection()

    if not selected_metrics:
        return

    # 5) Enviar métricas
    try:
        send_selected_metrics(cfg, selected_metrics, repeat=args.repeat)
    except Exception as e:
        print(f"[ERROR] No se pudieron enviar las métricas: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
