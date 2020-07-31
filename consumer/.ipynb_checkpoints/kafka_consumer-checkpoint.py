import os
import sys
import time
import json
import logging
import confluent_kafka
import variables as var
from datetime import date
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, Producer, OFFSET_BEGINNING


def get_module_logger():
    logger = logging.getLogger()
    stream_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(var.local_log_file, var.logger_file_mode)
    formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - [%(filename)s] - [%(filename)s:%(lineno)d] - %(message)s')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.setLevel(var.logging_level)
    return logger

def consumer_conf(broker, topic_name, group_id, timeout=6000, offset_strategy="smallest", enable_auto_commit="false", from_beginning=False):
    """
    Function that will configure the consumer
    :param broker: String: Kafka Broker string.
    :param topic_name: String: Name of the kafka topic.
    :param group_id: String: Kafka group id.
    :param timeout: Int: Timeout.
    :param offset_strategy: Offset strategy in case of first run or invalid group offset.
    :param enable_auto_commit: Bool: True if we want to commit offset to background.
    :param from_beginning: Bool: True if we want to re-read all topic messages.
    :return consumer: Consumer: Kafka consumer.
    """
    # Set the consumer
    conf = {"bootstrap.servers": broker,
        "group.id": group_id,
        "session.timeout.ms": timeout,
        "default.topic.config": {
        "auto.offset.reset": offset_strategy,
        "enable.auto.commit": enable_auto_commit
        }
    }
    consumer = confluent_kafka.Consumer(**conf)
    if from_beginning:
        consumer.subscribe([topic_name], on_assign=on_assign)
    else:
        consumer.subscribe([topic_name])
    logger.info(f"Kafka consumer configured with: auto.offset.reset: {offset_strategy} and enable.auto.commit: {enable_auto_commit}")
    return consumer


def on_assign(consumer, partitions):
    """
    Function that will reset the offset
    :param consumer: Consumer: Kafka consumer
    :param partitions: List[partition]: List of kafka partitions.
    """
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING
    consumer.assign(partitions)


def consumer(topic_name,
            group_id,
            broker=var.broker_url,
            encoding=var.encoding):
    """
    Function that will consumes data from the Kafka Topic
    :param topic_name: String: Name of the topic.
    :param group_id: String: Group id.
    :param broker: String: Broker url.
    :param encoding: String: Encoding type.
    """
    # Vars:
    hwmark = 0
    msg_error_count = 0
    msg_consumed_count = 0

    # Consumer conf:
    consumer_start = time.time()
    consumer = consumer_conf(broker=broker, topic_name=topic_name, group_id=group_id)

    # Consumer loop:
    while True:
        message = consumer.poll(1)
        if message is None:
                logger.info(f"Awaiting new messages...")
                logger.info(f"Number of messages is: {msg_consumed_count}, Number of error messages is: {msg_error_count}, saved offset is: {hwmark}, duration is: {time.time() - consumer_start}")
                logger.info(f"Commiting offset")
                consumer.commit()
                logger.info(f"Offset commited")
        elif message.error() is not None:
            msg_error_count = msg_error_count + 1
            logger.exception(f"Error from consumer: {message.error()}")
            logger.info(f"Number of messages is: {msg_consumed_count}, Number of error messages is: {msg_error_count}, saved offset is: {hwmark}, duration is: {time.time() - consumer_start}")
        else:
            hwmark = str(message.offset())
            msg_consumed_count += 1
            new_message = message_formatting(message)
            if msg_consumed_count % 1000 == 0:
                logger.info(f"Number of messages is: {msg_consumed_count}, Number of error messages is: {msg_error_count}, saved offset is: {hwmark}, duration is: {time.time() - consumer_start}")
    consumer.close()


def message_formatting(message, encoding=var.encoding):
    """
    Function that will rewrite the message.
    :param message: Kafka message: Message received from kafka.
    :return new_message: Json: New formatted message.
    """
    try:
        message_key = message.key().decode(encoding)
        message_body = json.loads(message.value().decode(encoding))
    except Exception as e:
        message_key = message.key()
        message_body = json.loads(message.value())
    new_message = {"key": message_key, "message": message_body}
    logger.info(new_message)
    return new_message


if __name__ == "__main__":

    try: 
        # Logger:
        get_module_logger()
        logger = logging.getLogger(__name__)
        
        # Start the consumer:
        logger.info(f"Kafka consumer for topic: {var.topic_name} started...")
        consumer(var.topic_name, var.group_id)
        logger.info(f"Kafka consumer for topic: {var.topic_name} finished")
    except KeyboardInterrupt as e:
        logger.exception("Consumer Aborted by user using KeyboardInterrupt")
    except Exception as e:
        logger.exception(f"Consumer crashed due to: {e}")