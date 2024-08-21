import asyncio
import logging
import random
import time
import traceback

from db import get_last_pass
import pika
from pika.exceptions import AMQPConnectionError

from passes import MosPass

LOGGER = logging.getLogger(__name__)
rabbit_config = {
    'host': '10.8.0.5',
    'port': 5672,
    'user': 'rmuser',
    'password': 'rmpassword',
    'vhost': 'mos_passes',
    'queue': 'urgent_q'
}
pmos = MosPass('nixncom@gmail.com', 'qAzWsX159$$$3')

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


async def find_range():
    start_pass = await get_last_pass()
    print(start_pass)
    if not start_pass:
        return None
    current_pass = start_pass

    flag = True
    while flag:
        current_pass += random.randint(450, 550)
        try:
            s = str(current_pass)
            while len(s) < 7:
                s = '0' + s
            s = f'БА {s}'
            print(f'Try {s}')
            res = await pmos.get_pass_info(s)
            if res is None:
                flag = False
        except Exception as e:
            traceback.print_exc()
    stop_pass = current_pass
    passes = []
    for i in range(start_pass, stop_pass):
        s = str(i)
        while len(s) < 7:
            s = '0' + s
        s = f'БА {s}'
        passes.append(s)
    if len(passes) == 0:
        return None
    return passes


if __name__ == '__main__':
    passes = asyncio.run(find_range())
    if passes:
        send_passes_to_rabbitmq(passes)
    else:
        print("Ошибка при генерации пропусков")
