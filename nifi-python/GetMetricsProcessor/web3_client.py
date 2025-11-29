import time
from web3 import Web3

def create_web3_connection(base_url: str, api_key: str) -> Web3:
    """Crea una conexión Web3 a Ethereum con el nodo especificado."""
    full_url = f"{base_url}{api_key}"
    web3 = Web3(Web3.HTTPProvider(full_url))

    if not web3.is_connected():
        raise ValueError(f"No se pudo conectar al nodo Ethereum en {full_url}")

    return web3

def get_block_metrics(web3: Web3, block_identifier="latest", source="unknown"):
    """Obtiene métricas del bloque especificado en Wei y ETH."""
    block = web3.eth.get_block(block_identifier)
    
    return {
        "measurement": "block_metrics",
        "tags": {
            "source": source,
            "identifier": block_identifier
        },
        "fields": {

            # ✅ Métricas en Wei
            "gas_used": block.gasUsed,
            "gas_limit": block.gasLimit,
            "difficulty": block.difficulty,

            # ✅ Métricas convertidas a ETH
            "gas_used_eth": web3.from_wei(block.gasUsed, "ether"),
            "gas_limit_eth": web3.from_wei(block.gasLimit, "ether"),
            "difficulty_eth": web3.from_wei(block.difficulty, "ether"),

            # ✅ Otras métricas sin conversión
            "transactions_count": len(block.transactions),
            "uncles_count": web3.eth.get_uncle_count(block.number),
            "block_number": block.number,
            "timestamp": block.timestamp,
            "size": block.size,
        },
        "time": time.time()  # Tiempo actual en segundos
    }

def get_gas_metrics(web3: Web3, source="unknown"):
    """Obtiene métricas de gas actuales en Wei y ETH."""
    gas_price_wei = web3.eth.gas_price
    max_priority_fee_wei = web3.eth.max_priority_fee
    blob_base_fee_wei = web3.eth.blob_base_fee

    return {
        "measurement": "gas_metrics",
        "tags": {
            "source": source
        },
        "fields": {
            # ✅ Valores en Wei
            "gas_price": gas_price_wei,
            "max_priority_fee": max_priority_fee_wei,
            "blob_base_fee": blob_base_fee_wei,

            # ✅ Valores en ETH
            "gas_price_eth": web3.from_wei(gas_price_wei, "ether"),
            "max_priority_fee_eth": web3.from_wei(max_priority_fee_wei, "ether"),
            "blob_base_fee_eth": web3.from_wei(blob_base_fee_wei, "ether"),
        },
        "time": time.time()  # Tiempo actual en segundos
    }

def get_metrics_eth(base_url: str, api_key: str, source: str, metric_type: str, block_identifier: str = "latest", influx_format: bool = False):
    """Crea una conexión Web3 y obtiene métricas en formato normal o InfluxDB."""
    try:
        # Crear la conexión Web3
        web3 = create_web3_connection(base_url, api_key)

        # Obtener las métricas según el tipo
        if metric_type == "gas":
            data = get_gas_metrics(web3, source)
        elif metric_type == "block":
            data = get_block_metrics(web3, block_identifier, source)
        else:
            raise ValueError(f"Tipo de métrica inválido: {metric_type}. Usa 'gas' o 'block'.")

        # Retornar en formato InfluxDB si se especifica
        if influx_format:
            tags = ",".join([f"{k}={v}" for k, v in data["tags"].items()])
            fields = ",".join([f"{k}={v}" for k, v in data["fields"].items()])
            return f"{data['measurement']},{tags} {fields} {int(data['time'])}"

        return data

    except Exception as e:
        raise ValueError(f"Error obteniendo métricas: {str(e)}")
