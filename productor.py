from kafka import KafkaProducer

# Crear un productor de Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Enviar mensajes a un tópico llamado 'test'
for i in range(10):
    producer.send('test', b'Hola Kafka %d' % i)

# Cerrar la conexión
producer.close()
