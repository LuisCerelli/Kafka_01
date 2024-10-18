from kafka import KafkaConsumer

# Crear un consumidor que se suscribe al tópico 'test'
consumer = KafkaConsumer('test', bootstrap_servers='localhost:9092')

# Leer mensajes desde el tópico
for message in consumer:
    print(message.value.decode('utf-8'))
