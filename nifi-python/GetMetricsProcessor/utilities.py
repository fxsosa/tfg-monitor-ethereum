# Importar las funciones de cada modulo
from web3_client import get_metrics_eth
from get_block_info import get_block_info
from get_config_info import get_config_info
from get_node_info import get_node_info

def call_function(function_name, function_arguments, function_sensitive_arguments):
    # Buscar la función en el entorno global
    func = globals().get(function_name)
    
    if func is None:
        raise ValueError(f"Función '{function_name}' no encontrada.")
    
    # Combinar los argumentos
    all_args = {**function_arguments, **function_sensitive_arguments}
    
    # Llamar la función con los argumentos
    return func(**all_args)
