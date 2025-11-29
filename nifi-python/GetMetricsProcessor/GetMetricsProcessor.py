from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.relationship import Relationship
import json
from utilities import call_function

def safe_load_json(property_value, property_name):
    try:
        return json.loads(property_value)
    except json.JSONDecodeError as e:
        raise ValueError(f"Error en el formato de '{property_name}': {str(e)}")

class GetMetricsProcessor(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        description = 'Obtiene métricas de Ethereum vía Web3 y las transforma para NiFi'
        tags = ['Influx', 'Ethereum', 'Metrics']
        dependencies = ['web3', 'requests', 'pandas'] # Se debe agregar las librerias a instalar en el entorno

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__()

        self.function_name = PropertyDescriptor(
            name="Nombre de la funcion",
            description="Se debe especificar el nombre de la funcion a llamar.",
            required=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR],
            allowable_values=["get_metrics_eth", "get_block_info", "get_node_info", "get_config_info"] # Se debe ir agregando cada funcion creada
        )

        self.function_sensitive_params = PropertyDescriptor(
            name="Sensitive function params",
            description="Un diccionario que contiene parametros sensibles como API-keys.",
            required=True,
            sensitive=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        self.function_params = PropertyDescriptor(
            name="Function params",
            description="Un diccionario que contiene parametros para la funcion.",
            required=True,
            validators=[StandardValidators.NON_EMPTY_VALIDATOR]
        )

        self.descriptors = [
            self.function_name,
            self.function_params,
            self.function_sensitive_params
        ]

        self.success = Relationship(name='success', description='Operación exitosa')
        self.failure = Relationship(name='failure', description='Fallo en la operación')
        self.relationships = {self.success, self.failure}

    def getPropertyDescriptors(self):
        return self.descriptors

    def getRelationships(self):
        return self.relationships

    def transform(self, context, flowfile):
        function_name = context.getProperty("Nombre de la funcion").getValue()

        try:
            function_arguments = safe_load_json(context.getProperty("Function params").getValue(), "Function params")
            function_sensitive_arguments = safe_load_json(context.getProperty("Sensitive function params").getValue(), "Sensitive function params")

            self.logger.info(f"Llamando a función '{function_name}' con argumentos: {function_arguments.keys()}")

            result = call_function(function_name, function_arguments, function_sensitive_arguments)

            if not isinstance(result, (str, bytes)):
                contents = json.dumps(result)
            else:
                contents = result

            self.logger.debug(f"Resultado de '{function_name}': {contents[:200]}")  # solo los primeros 200 chars

            return FlowFileTransformResult(
                relationship="success",
                contents=contents,
                attributes={
                    "function_name": function_name,
                    "function_arguments": json.dumps(function_arguments),
                    "metrics_result": contents
                }
            )

        except Exception as e:
            self.logger.error(f"Error al ejecutar '{function_name}': {e}")
            return FlowFileTransformResult(
                relationship="failure",
                contents="{}",
                attributes={"error": str(e)}
            )
