import os
import time
import json
import logging
import requests
import threading
from urllib.parse import urlparse
from dotenv import load_dotenv
from sse_client import SSEClient
import re

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def replace_env_variables_in_config(configs):
    """
    Reemplaza cualquier valor ${ENV_VAR} en los campos string de los diccionarios del JSON con
    la variable de entorno correspondiente.
    """
    env_var_pattern = re.compile(r"\$\{([^}]+)\}")
    for cfg in configs:
        for key, value in cfg.items():
            if isinstance(value, str):
                matches = env_var_pattern.findall(value)
                for var in matches:
                    env_val = os.getenv(var)
                    if env_val:
                        value = value.replace(f"${{{var}}}", env_val)
                cfg[key] = value
    return configs

def load_configs():
    """
    Carga la lista de configuraciones desde un fichero JSON
    o desde la variable de entorno SSE_CONFIGS.
    """
    configs = []
    config_file = os.path.join(os.path.dirname(__file__), 'sse_configs.json')
    if os.path.isfile(config_file):
        with open(config_file, 'r') as f:
            configs = json.load(f)
            logging.info(f"Cargadas {len(configs)} configs desde {config_file}")
    else:
        raw = os.getenv("SSE_CONFIGS", "[]")
        try:
            configs = json.loads(raw)
            logging.info(f"Cargadas {len(configs)} configs desde SSE_CONFIGS")
        except json.JSONDecodeError:
            logging.error("SSE_CONFIGS no es un JSON válido")
    return replace_env_variables_in_config(configs)

configs = load_configs()
NIFI_URL = os.getenv("NIFI_URL", "http://nifi:8080/eth-events")
HEARTBEAT_INTERVAL = int(os.getenv("SSE_HEARTBEAT_INTERVAL", "60"))

# Campos que no queremos incluir en el Protocolo de Línea
#IGNORED_FIELDS = {
    #"aggregation_bits", "sync_committee_bits", "sync_committee_signature",
    #"signature", "committee_bits", "logs_bloom", "execution_optimistic",
    #"execution_extra_data", "block", "parent_root", "state_root",
    #"body_root", "block_hash"
#}
IGNORED_FIELDS = {
    "aggregation_bits",
    "signature",
    "data_source_root",
    "data_target_root",
    "block",
    "state",
    "previous_duty_dependent_root",
    "current_duty_dependent_root",
    "signed_header_1_message_parent_root",
    "signed_header_1_message_state_root",
    "signed_header_1_message_body_root",
    "signed_header_1_signature",
    "signed_header_2_message_parent_root",
    "signed_header_2_message_state_root",
    "signed_header_2_message_body_root",
    "signed_header_2_signature",
    "attestation_1_data_source_root",
    "attestation_1_data_target_root",
    "attestation_1_signature",
    "attestation_2_data_source_root",
    "attestation_2_data_target_root",
    "attestation_2_signature",
    "message_from_bls_pubkey",
    "message_to_execution_address",
    "message_contribution_beacon_block_root",
    "old_head_block",
    "new_head_block",
    "old_head_state",
    "new_head_state",
    "message_contribution_aggregation_bits",
    "message_contribution_signature",
    "message_selection_proof",
    "data_attested_header_beacon_parent_root",
    "data_attested_header_beacon_state_root",
    "data_attested_header_beacon_body_root",
    "data_finalized_header_beacon_parent_root",
    "data_finalized_header_beacon_state_root",
    "data_finalized_header_beacon_body_root",
    "data_sync_aggregate_sync_committee_bits",
    "data_sync_aggregate_sync_committee_signature",
    "data_parent_block_root",
    "data_parent_block_hash",
    "data_payload_attributes_prev_randao",
    "data_payload_attributes_suggested_fee_recipient",
    "block_root",
    "kzg_commitment",
    "versioned_hash"
}


def flatten_json(data, prefix=''):
    items = {}
    if isinstance(data, dict):
        for k, v in data.items():
            key = f"{prefix}_{k}" if prefix else k
            if key in IGNORED_FIELDS:
                continue
            items.update(flatten_json(v, key))
    elif isinstance(data, list):
        pass  # Ignoramos listas
    elif isinstance(data, str):
        if data.startswith("0x"):
            items[prefix] = f'"{data}"'
        elif data.isdigit():
            items[prefix] = f"{int(data)}i"
        else:
            try:
                float_val = float(data)
                items[prefix] = str(float_val)
            except ValueError:
                items[prefix] = f'"{data}"'
    elif isinstance(data, (int, float)):
        items[prefix] = str(data)
    elif isinstance(data, bool):
        items[prefix] = "true" if data else "false"
    else:
        items[prefix] = f'"{str(data)}"'
    return items

def json_event_to_line_protocol(event_type, data_str, source, network):
    try:
        data_json = json.loads(data_str)
    except json.JSONDecodeError:
        return None
    fields = flatten_json(data_json)
    fields["count"] = "1i"
    fields_str = ",".join(f"{k}={v}" for k, v in fields.items())
    timestamp = time.time_ns()
    endpoint = '/eth/v1/events'
    return (
        f"beacon_event,event_type={event_type},"
        f"source={source},network={network},endpoint={endpoint} {fields_str} {timestamp}"
    )

def post_event(event_type, data_str, source, network):
    line = json_event_to_line_protocol(event_type, data_str, source, network)
    if not line:
        logging.warning(f"No se pudo convertir evento a line protocol: {event_type}")
        return
    try:
        res = requests.post(NIFI_URL, data=line.encode("utf-8"), timeout=5)
        if res.status_code != 200:
            logging.warning(f"⚠️ NiFi respondió con código {res.status_code}")
        else:
            logging.debug(f"✅ Evento {event_type} enviado correctamente a NiFi")

    except Exception as e:
        logging.error(f"❌ Error al enviar a NiFi: {e}")

def heartbeat_loop(source, network):
    """
    Envía un evento 'heartbeat' cada HEARTBEAT_INTERVAL segundos.
    """
    while True:
        data = json.dumps({"heartbeat_timestamp": time.time()})
        logging.info(f"[HEARTBEAT] 🫀 {source}/{network} activo a las {time.strftime('%H:%M:%S')}")
        post_event("heartbeat", data, source, network)
        time.sleep(HEARTBEAT_INTERVAL)

def make_sse_loop(cfg):
    """
    Ejecuta un bucle infinito de conexión SSE:
    - Arranca un hilo de heartbeat.
    - Reintenta conexión si falla, enviando aviso de error a NiFi.
    """
    source = cfg.get("source", "unknown")
    network = cfg.get("network", "unknown")
    url = cfg["url"].rstrip("/")
    endpoint = cfg["endpoint"]
    topics = cfg.get("topics", "")

    # Arrancar heartbeat
    hb_thread = threading.Thread(
        target=heartbeat_loop,
        args=(source, network),
        daemon=True
    )
    hb_thread.start()

    client = SSEClient(
        url=url,
        endpoint=endpoint,
        params={"topics": topics},
        on_event=lambda et, data, su: post_event(et, data, source, network)
    )

    while True:
        try:
            logging.info(f"Conectando SSE a {url}{endpoint} (topics={topics})")
            client.getSSEStream()
        except Exception as e:
            # Aviso de fallo
            err_data = json.dumps({"error": str(e), "timestamp": time.time()})
            logging.error(f"Error SSE para '{source}': {e}. Avisando y reintentando en 5s…")
            post_event("sse_error", err_data, source, network)
            time.sleep(5)

# Arrancar un hilo daemon por cada config
for cfg in configs:
    logging.info(f"🧵 Iniciando hilo para fuente '{cfg.get('source')}' en red '{cfg.get('network')}'")
    t = threading.Thread(target=make_sse_loop, args=(cfg,), daemon=True)
    t.start()

# Mantener el proceso principal vivo
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    logging.info("⛔ Interrupción detectada. Saliendo…")
