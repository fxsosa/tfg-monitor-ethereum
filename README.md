# tfg-ethereum-monitor

Sistema de monitoreo para la red de Ethereum utilizando Docker, Grafana, InfluxDB, NiFi y un servidor Nginx.

![Ethereum Monitor](./docs/images/vision-en-detalle-del-proyecto-fig1.png)
## Requisitos

Antes de comenzar, asegúrate de tener instalados los siguientes componentes:

- [Docker](https://docs.docker.com/get-docker/)

## Cómo iniciar el entorno

### 1. Clonar el repositorio

```bash
git clone https://github.com/fxsosa/tfg-monitor-ethereum.git
cd tfg-monitor-ethereum
```

### 2. Levantar los servicios con Docker Compose

```bash
docker compose up
```

Este comando iniciará todos los contenedores definidos en el archivo `docker-compose.yml`.

## Stack utilizado
- NiFi: para la definicion del flujo ETL.
- InfluxDB: para el almacenamiento de las metricas
- Grafana: para la visualizacion de las metricas
