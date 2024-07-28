import logging
import time

import pika
from pika.exceptions import AMQPConnectionError

LOGGER = logging.getLogger(__name__)
rabbit_config = {
    'host': '10.8.0.5',
    'port': 5672,
    'user': 'rmuser',
    'password': 'rmpassword',
    'vhost': 'mos_passes',
    'queue': 'urgent_q'
}

def send_passes_to_rabbitmq(passes):
    try:
        # Установка соединения с RabbitMQ
        credentials = pika.PlainCredentials(rabbit_config['user'], rabbit_config['password'])
        parameters = pika.ConnectionParameters(
            host=rabbit_config['host'],
            port=rabbit_config['port'],
            virtual_host=rabbit_config['vhost'],
            credentials=credentials
        )

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Убедимся, что очередь существует
        channel.queue_declare(queue=rabbit_config['queue'], durable=True)

        # Отправка VIN'ов в очередь
        i = 0
        total_elapsed = len(passes)
        for p in passes:
            channel.basic_publish(
                exchange='',
                routing_key=rabbit_config['queue'],
                body=p,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                ))
            i += 1
            total_elapsed -= 1
            if i >= 1000:
                print(f"Отправлено пропусков: {i} из {len(passes)}, осталось {total_elapsed}")
                i = 0

        connection.close()
    except Exception as e:
        print(f"Ошибка при работе с RabbitMQ: {e}")






if __name__ == '__main__':
    start_n = 1669150
    stop_n = 1
    passes = []
    for i in range(start_n, stop_n, -1):
        s = str(i)
        while len(s) < 7:
            s = '0' + s
        s = f'БА {s}'
        passes.append(s)
    if passes:
        send_passes_to_rabbitmq(passes)
    else:
        print("Ошибка при генерации пропусков")