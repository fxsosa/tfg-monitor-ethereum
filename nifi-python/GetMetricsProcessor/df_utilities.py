import requests
import pandas as pd
import logging
import traceback
from pandas import json_normalize
from datetime import datetime, timezone
import re
from typing import Tuple

logger = logging.getLogger(__name__)

def get_url_by_args(base_url: str, endpoint: str, args: dict) -> str:
    """Construye una URL reemplazando placeholders del endpoint con los valores proporcionados.

    Los placeholders en el endpoint deben seguir el formato `{param}` y serán reemplazados
    usando los valores correspondientes en el diccionario `args`.

    Args:
        base_url (str): La URL base del servicio (por ejemplo, "https://api.example.com").
        endpoint (str): El path del recurso, con placeholders opcionales (por ejemplo, "/users/{user_id}").
        args (dict): Diccionario de valores para reemplazar los placeholders en `endpoint`.

    Returns:
        str: La URL completa con los placeholders reemplazados.

    Raises:
        ValueError: Si falta algún parámetro requerido en `args`.

    Example:
        >>> get_url_by_args("https://api.com", "/user/{id}/posts", {"id": 42})
        'https://api.com/user/42/posts'
    """
    try:
        endpoint_filled = endpoint.format(**args)
        base_url = base_url.format(**args)
    except KeyError as e:
        logger.error("Falta parámetro requerido en 'args': %s", e)
        raise ValueError(f"Falta el parámetro requerido en 'args': {e}")

    return f"{base_url.rstrip('/')}/{endpoint_filled.lstrip('/')}"


def get_json_by_url(url: str, timeout: int = 10) -> dict:
    """Realiza una petición GET a una URL y retorna la respuesta JSON como dict.

    Si la petición falla, se loguea la URL del error.

    Args:
        url (str): URL completa a consultar.
        timeout (int, optional): Tiempo máximo de espera en segundos. Por defecto es 10.

    Returns:
        dict: Contenido del JSON recibido o `{}` en caso de error.
    """
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error al obtener JSON desde {url}: {e}")
        logger.debug(traceback.format_exc())
        return {}


def json_to_dataframe(json_data: dict | list) -> pd.DataFrame:
    """Convierte un JSON plano o anidado (dict o lista de dicts) en un DataFrame.

    Si el JSON contiene una clave 'data', se usa esa como fuente. Utiliza pandas.json_normalize
    para aplanar estructuras anidadas de forma automática.

    Args:
        json_data (dict | list): Estructura JSON obtenida de la API.

    Returns:
        pd.DataFrame: DataFrame construido a partir del JSON. Puede estar vacío si no se puede parsear.
    """
    if not json_data:
        logger.warning("JSON vacío o inválido, no se puede construir DataFrame.")
        return pd.DataFrame()

    try:
        # Si tiene clave 'data', usamos esa
        if isinstance(json_data, dict) and "data" in json_data:
            json_data = json_data["data"]

        # Aplanar listas o dicts usando json_normalize
        if isinstance(json_data, list):
            return json_normalize(json_data, sep='.')
        elif isinstance(json_data, dict):
            return json_normalize([json_data], sep='.')
        else:
            logger.error("Estructura no soportada para convertir a DataFrame.")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error al convertir JSON a DataFrame: {e}")
        logger.debug(traceback.format_exc())
        return pd.DataFrame()

def get_df_by_url(base_url: str, endpoint: str, args: dict, timeout: int = 10) -> pd.DataFrame:
    """Obtiene un DataFrame directamente desde una API usando base URL, endpoint y argumentos.

    Args:
        base_url (str): URL base del servicio.
        endpoint (str): Ruta del recurso con placeholders.
        args (dict): Parámetros para completar la URL.
        timeout (int, optional): Tiempo de espera para la petición. Por defecto es 10 segundos.

    Returns:
        pd.DataFrame: DataFrame con los datos obtenidos o vacío si hay error.
    """

    url = get_url_by_args(base_url, endpoint, args)
    json_data = get_json_by_url(url, timeout)
    df = json_to_dataframe(json_data)
    df.replace('', 'UNKNOWN', inplace=True)
    df.replace(' ', 'UNKNOWN', inplace=True)
    df.fillna('UNKNOWN', inplace=True)

    return df

def set_df_tags(df, base_url: str, endpoint: str, measurement: str, network: str, args: dict) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    df["timestamp_df"] = int(now.timestamp() * 1_000_000_000)

    df["measurement"] = measurement
    df["network"] = network
    df["source"] = base_url

    endpoint_filled = endpoint.format(**args)
    df["endpoint"] = endpoint_filled

    return df

def get_status_by_url(url: str, timeout: int = 10) -> int:
    try:
        response = requests.get(url, timeout=timeout)
        return response.status_code
    except Exception as e:
        logger.error(f"Error al obtener JSON desde {url}: {e}")
        logger.debug(traceback.format_exc())
        return 400 

def get_df_status_code(base_url: str, endpoint: str, args: dict, measurement: str, network: str, timeout: int = 10) -> pd.DataFrame:
    url = get_url_by_args(base_url, endpoint, args)
    status_code = get_status_by_url(url, timeout)

    now = datetime.now(timezone.utc)
    timestamp = int(now.timestamp() * 1_000_000_000)

    df = pd.DataFrame([{
        "status_code": status_code,
        "timestamp_df": timestamp,
        "measurement": measurement,
        "network": network,
        "source": base_url,
        "endpoint": endpoint
    }])

    return df

def extract_name_version(value: str) -> Tuple[str, str]:
    """Extrae el nombre y la versión de una cadena tipo 'nombre/vX.Y.Z' o 'nombre/X.Y.Z'.

    Args:
        value (str): La cadena a analizar.

    Returns:
        tuple: (nombre, version), o ('UNKNOWN', 'UNKNOWN') si no coincide.
    """
    if not isinstance(value, str) or value.strip().upper() == 'UNKNOWN':
        return ('UNKNOWN', 'UNKNOWN')

    match = re.search(r'([a-zA-Z0-9_.+-]+)/v?([0-9]+\.[0-9]+(?:\.[0-9]+)?)', value)
    return (match.group(1), f"v{match.group(2)}") if match else ('UNKNOWN', 'UNKNOWN')

def df_to_line_protocol(
    df,
    measurement_col=None,
    tag_cols=None,
    field_cols=None,
    timestamp_col=None,
    default_measurement="default_metric",
    timestamp_unit="ns"
):

    MAX_INT64 = 9223372036854775807
    MIN_INT64 = -9223372036854775808

    lines = []
    for _, row in df.iterrows():
        # 1. Measurement
        measurement = row[measurement_col] if measurement_col else default_measurement

        # 2. Tags
        tags = []
        for col in (tag_cols or []):
            value = str(row[col]).replace(" ", r"\ ").replace(",", r"\,").replace("=", r"\=")
            tags.append(f"{col}={value}")
        tag_str = ",".join(tags)

        # 3. Fields
        fields = []
        field_candidates = field_cols if field_cols is not None else df.columns.difference(
            (tag_cols or []) +
            ([measurement_col] if measurement_col else []) +
            ([timestamp_col] if timestamp_col else [])
        )
        for col in field_candidates:
            original_col = col
            val = row[col]

            if isinstance(val, str):
                val_str = val.strip()
                val_lower = val_str.lower()
                if val_lower in {"true", "false"}:
                    val = val_lower
                else:
                    try:
                        if "." in val_str:
                            val = float(val_str)
                        else:
                            int_val = int(val_str)
                            if MIN_INT64 <= int_val <= MAX_INT64:
                                val = f"{int_val}i"
                            else:
                                col = f"{original_col}_str"
                                val = val_str.strip('"')
                                val = f'"{val}"'
                    except ValueError:
                        val = val_str.strip('"')
                        val = f'"{val}"'

            if isinstance(val, bool):
                val = "true" if val else "false"
            elif isinstance(val, int):
                val = f"{val}i"
            elif isinstance(val, float):
                val = f"{val}"
            elif isinstance(val, str):
                if val.endswith("i") and val[:-1].isdigit():
                    pass  # ya es válido
                elif val in {"true", "false"}:
                    pass  # ya es válido
                else:
                    val = val.strip('"')
                    val = f'"{val}"'

            fields.append(f"{col}={val}")

        field_str = ",".join(fields)

        # 4. Timestamp
        ts = ""
        if timestamp_col:
            ts_val = pd.to_datetime(row[timestamp_col])
            ts = str(int(ts_val.value)) if timestamp_unit == "ns" else str(int(ts_val.timestamp()))

        # 5. Combine
        line = f"{measurement}"
        if tag_str:
            line += f",{tag_str}"
        line += f" {field_str}"
        if ts:
            line += f" {ts}"
        lines.append(line)

    return "\n".join(lines)
