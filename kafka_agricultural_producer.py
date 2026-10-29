import time
import json
import csv
from kafka import KafkaProducer

# Configuración del productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
)

# Ruta de archivo
csv_file_path = 'Insumos_agricolas_BD_SIPSA_I_2026.csv'
topic_name = 'insumos_agricolas_topic'

print(f"Iniciando streaming desde {csv_file_path} al tópico '{topic_name}'...")

try:
    with open(csv_file_path, mode='r', encoding='latin-1') as file:

        reader = csv.DictReader(file, delimiter=';')
        for raw_row in reader:
            #Imprimimos la fila para ver la clave recibida
            row = {k.strip() if k else k: v for k, v in raw_row.items()}
            print("Fila completa leída:", row)
            # Ajusta estas llaves según lo que imprima arriba si tienen espacios o nombres distintos
            articulo = row.get('ARTÍCULO') or row.get('ARTICULO')
            precio = row.get('PRECIO')
            # Enviamos la fila completa como JSON a Kafka
            producer.send(topic_name, value=row)
            print(f"Enviado -> Artículo: {row.get('ARTÍCULO')} | Precio: {row.get('PRECIO')}")
            # Pausa de 1 segundo para simular la llegada en tiempo real
            time.sleep(1)

except FileNotFoundError:
    print(f"Error: No se encontró el archivo '{csv_file_path}'. Verifica el nombre con el comando 'ls'.")
except Exception as e:
    print(f"Ocurrió un error inesperado: {e}")
finally:
    producer.flush()
    producer.close()
