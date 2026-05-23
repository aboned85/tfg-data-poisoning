# Dashboard de detección de Data Poisoning en pipelines de Big Data mediante indicadores de integridad en tiempo real

Trabajo Final de Grado — Grado en Ingeniería Informática - Área de Business Intelligence

## Descripción

Sistema de monitorización que detecta ataques de data poisoning en pipelines de Big Data mediante KPIs de integridad de datos y dashboards interactivos en tiempo real.

## Stack tecnológico

- Apache Kafka — Ingestión de datos en tiempo real
- Apache Spark — Procesamiento y cálculo de KPIs
- PostgreSQL — Almacenamiento de datos y métricas
- Prometheus — Recolección de métricas
- Grafana — Dashboards y alertas
- Docker Compose — Orquestación de servicios
- Python 3.11 — Scripts de generación y procesamiento

## Estructura del proyecto
```
tfg-data-poisoning/
    docker-compose.yml
    producer/
        producer.py
        requirements.txt
    processor/
        spark_job.py
        requirements.txt
    prometheus/
        prometheus.yml
    grafana/
        dashboards/
            grafana_dashboard.json
    sql/
        init.sql
    README.md
```

## Como ejecutar el código

1. Instala Docker Desktop, Python 3.11, Java 17 y Pyspark
2. Levanta la infraestructura:

docker compose up -d

3. Instala las dependecias:

cd producer && pip install -r requirements.txt
cd ../processor && pip install -r requirements.txt

4. Arranca el producer:

cd producer
python producer.py

5. Arranca el procesador:

cd processor
python spark_job.py

6. Accede a Grafana a través de la url http://localhost:3000
7. Importa el dashboard

## Autor

Alberto Boned Carrasco