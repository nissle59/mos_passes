import asyncio
import logging
import time

import pika
from pika.exceptions import AMQPConnectionError

from config import MQ
import db
from passes import MosPass

LOGGER = logging.getLogger(__name__)
acc = asyncio.run(db.get_account('nixncom@gmail.com'))
print(acc)
username = acc[0]['login']
password = acc[0]['password']
PMOS = MosPass(username, password)


async def parse(pass_no):
    stat = await PMOS.get_pass_info(pass_no)
    if stat:
        if isinstance(stat, dict):
            await db.set_pass(stat)


def callback(ch, method, properties, body):
    LOGGER = logging.getLogger(__name__ + ".callback")
    asyncio.run(parse(body.decode('utf-8')))
    # Подтверждаем, что сообщение обработано
    ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_messages():
    LOGGER = logging.getLogger(__name__ + ".consume_messages")
    while True:
        try:
            # Установка соединения с RabbitMQ
            credentials = pika.PlainCredentials(MQ.user, MQ.password)
            parameters = pika.ConnectionParameters(
                host=MQ.host,
                port=MQ.port,
                virtual_host=MQ.vhost,
                credentials=credentials
            )

            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            # Убедимся, что очередь существует
            #channel.queue_declare(queue=MQ.queue, durable=True)

            # Настраиваем потребителя
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=MQ.queue, on_message_callback=callback)

            LOGGER.info("Ожидание сообщений. Для выхода нажмите CTRL+C")
            channel.start_consuming()
        except AMQPConnectionError as e:
            LOGGER.error(f"Потеря соединения с RabbitMQ: {e}. Попытка переподключения через 5 секунд...",
                         exc_info=False)
            time.sleep(5)
        except Exception as e:
            LOGGER.error(f"Произошла ошибка: {e}. Попытка переподключения через 5 секунд...", exc_info=False)
            time.sleep(5)
        finally:
            try:
                connection.close()
            except Exception:
                pass


if __name__ == "__main__":
    consume_messages()
