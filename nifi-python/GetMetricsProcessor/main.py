from time import sleep
from get_block_info import get_block_info
from get_config_info import get_config_info
from get_node_info import get_node_info

# Suponiendo que ya definiste estas funciones en el mismo archivo o las importaste:
# - get_block_info
# - get_config_metrics
# - get_node_metrics

def main():
    beacon_api_url = "https://beacon.nodereal.io"  # Cambia si usás otro proveedor
    network = "mainnet"
    block_id = "head"

    try:
        block_line = get_block_info(beacon_api_url, "url", block_id, network)
        print("✅ Beacon block info:")
        print(block_line)
    except Exception as e:
        print(f"❌ Error en get_block_info: {e}")

    try:
        config_line = get_config_info(beacon_api_url, "url", network)
        print("\n✅ Beacon config metrics:")
        print(config_line)
    except Exception as e:
        print(f"❌ Error en get_config_metrics: {e}")

    try:
        node_line = get_node_info(beacon_api_url, "url", network)
        print("\n✅ Beacon node metrics:")
        print(node_line)
    except Exception as e:
        print(f"❌ Error en get_node_metrics: {e}")

if __name__ == "__main__":
    main()
