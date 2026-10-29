#  Big Data: Análisis de Volatilidad de Precios Agrícolas (SIPSA / DANE)

Procesamiento de datos en tiempo real desarrollada como componente académico en la **UNAD (Universidad Nacional Abierta y a Distancia)**. El sistema simula y procesa flujos de información agrícola utilizando herramientas de Big Data para analizar la volatilidad de los precios de mercado basándose en los boletines estadísticos del **SIPSA / DANE**.

---

## Arquitectura y Flujo del Sistema

El pipeline implementa una arquitectura orientada a eventos en tiempo real:

```text
[ Dataset SIPSA / DANE ] 
       │
       ▼ (Inyección de datos)
[ Productor Python ] ──(Streaming)──> [ Apache Kafka (KRaft) ] ──(Consumo en tiempo real)──> [ PySpark Structured Streaming ] ──> [ Spark Web UI (4040) ]

Datos tomados de https://www.datos.gov.co/dataset/Sistema-de-Informaci-n-de-Precios-y-Abastecimiento/9ka4-3qbp/about_data

Productor (kafka_agricultural_producer.py): Lee y transmite de forma continua los registros de precios agrícolas hacia un canal de mensajería.

Broker de Mensajería (Apache Kafka): Actúa como el bus central de streaming utilizando el modo moderno KRaft (sin dependencia de ZooKeeper) a través del topic sipsa-precios.

Procesamiento Distribuido (PySpark Structured Streaming): Consume los micro-lotes de datos al vuelo, procesando transformaciones analíticas y evaluando métricas de volatilidad.

Monitoreo: Permite la supervisión de la latencia y rendimiento de los jobs mediante la interfaz web de Spark (http://localhost:4040).
