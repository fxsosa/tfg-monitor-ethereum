import requests
import threading
import time
import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)

class SSEClient:
    def __init__(self, url: str, endpoint: str, 
                 params: Optional[dict] = None,
                 on_event: Optional[Callable[[str, str], None]] = None,
                 retry_delay: float = 5.0):
        """
        Cliente SSE para Ethereum Beacon con soporte de eventos nombrados.

        Args:
            url (str): URL base (sin barra al final).
            endpoint (str): Endpoint SSE (con barra inicial).
            params (Optional[dict]): Parámetros de la query.
            on_event (Optional[Callable[[str, str], None]]): Función que recibe (event_type, data).
            retry_delay (float): Tiempo entre reintentos si hay error.
        """
        self.url = url.rstrip("/")
        self.endpoint = endpoint
        self.params = params
        self.on_event = on_event
        self.retry_delay = retry_delay
        self.stop_event = threading.Event()

    def _run_event_function(self, event_type: str, data: str):
        """Ejecuta la función on_event en un hilo paralelo."""
        if self.on_event:
            thread = threading.Thread(target=self.on_event, args=(event_type, data, self.url))
            thread.daemon = True
            thread.start()
    
    def _run_event_function(self, event_type: str, data: str):
        def safe_call():
            try:
                self.on_event(event_type, data, self.url)
            except Exception as e:
                logger.error(f"Error en on_event handler: {e}", exc_info=True)

        if self.on_event:
            thread = threading.Thread(target=safe_call)
            thread.daemon = True
            thread.start()

    def getSSEStream(self):
        """
        Escucha indefinidamente eventos SSE, reconectando automáticamente ante fallos.
        Captura tanto el tipo de evento como los datos.
        """
        full_url = f"{self.url}{self.endpoint}"
        headers = {"Accept": "text/event-stream"}

        while not self.stop_event.is_set():
            try:
                logger.info(f"Conectando a SSE: {full_url}")
                with requests.get(full_url, params=self.params, headers=headers, stream=True, timeout=30) as response:
                    if response.status_code != 200:
                        logger.error(f"Error SSE {response.status_code}: {response.text}")
                        time.sleep(self.retry_delay)
                        continue

                    logger.info("Conexión establecida. Escuchando eventos...")
                    event_type = None

                    for line in response.iter_lines(decode_unicode=True):
                        if self.stop_event.is_set():
                            logger.info("Finalizando escucha SSE.")
                            break

                        if not line:
                            continue

                        if line.startswith("event:"):
                            event_type = line[6:].strip()
                        elif line.startswith("data:") and event_type:
                            data = line[5:].strip()
                            logger.debug(f"Evento: {event_type} | Data: {data}")
                            self._run_event_function(event_type, data)
                            event_type = None  # Reset después de enviar el evento

            except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                logger.warning(f"Error de conexión SSE: {e}")
                logger.info(f"Reintentando en {self.retry_delay} segundos...")
                time.sleep(self.retry_delay)
            except Exception as e:
                logger.critical(f"Excepción no controlada en el stream SSE: {e}", exc_info=True)
                time.sleep(self.retry_delay)

        logger.info("SSE detenido.")

    def stop(self):
        """Detiene la escucha del SSE."""
        self.stop_event.set()
