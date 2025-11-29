# tfg-ethereum-monitor

Sistema de monitoreo para la red de Ethereum utilizando Docker, Grafana, InfluxDB, NiFi y un servidor Nginx con certificado autofirmado.

## Requisitos

Antes de comenzar, asegúrate de tener instalados los siguientes componentes:

- [Docker](https://docs.docker.com/get-docker/)
- [OpenSSL](https://www.openssl.org/)

## Cómo iniciar el entorno

### 1. Clonar el repositorio

```bash
git clone https://github.com/fxsosa/tfg-ethereum-monitor.git
cd tfg-ethereum-monitor
```

### 2. Generar un certificado autofirmado

Utiliza OpenSSL para crear el certificado SSL autofirmado necesario para Nginx:

```bash
mkdir -p ./nginx/certs
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout ./nginx/certs/self.key \
  -out ./nginx/certs/self.crt \
  -days 365 \
  -subj "/CN=grafana.local"
```

Esto generará los archivos `self.key` y `self.crt` en la carpeta `nginx/certs`.

### 3. Levantar los servicios con Docker Compose

```bash
docker compose up
```

Este comando iniciará todos los contenedores definidos en el archivo `docker-compose.yml`.
