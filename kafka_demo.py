#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2024/10/23 下午8:15

import os

from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
import json
from kafka.errors import KafkaError

KAKFA_URL = os.getenv("KAKFA_HOST", 'localhost:9092')
env_mark = os.getenv("envMark", "dev")
topic_name = 'test-topic'
print(f"KAKFA_URL: {KAKFA_URL}")


def send_kafka_msg():
    try:
        print("send_kafka_msg env_mark:", env_mark)
        producer = KafkaProducer(bootstrap_servers=[KAKFA_URL])

        # 消息体
        message = {"env_tag": "text"}
        msg_bytes = json.dumps(message).encode('ascii')

        # 在消息头中添加env_mark
        headers = [('env_mark', env_mark.encode('utf-8'))]
        # 发送消息
        future = producer.send(topic_name, value=msg_bytes, headers=headers)

        # 阻塞直到发送完成并获取结果或异常
        result = future.get(timeout=10)
        print("Message sent successfully: %s", result)

        producer.flush()
        return f"current env: {env_mark}"

    except KafkaError as e:
        print("Failed to send message to Kafka: %s", e)
        return "Failed to send message"
    except Exception as e:
        print("An unexpected error occurred: %s", e)
        return "An unexpected error occurred"


def kafka_consumer_thread():
    consumer = KafkaConsumer(topic_name, bootstrap_servers=[KAKFA_URL])
    for message in consumer:
        # 从消息头中获取env_mark
        msg_env_mark = None
        for header in message.headers:
            if header[0] == 'env_mark':
                msg_env_mark = header[1].decode('utf-8')
                break

        if msg_env_mark == env_mark:
            print("kafka_consumer_thread " +
                  "%s:%d:%d: key=%s value=%s" % (
                      message.topic, message.partition, message.offset, message.key, message.value),
                  f"环境符合, 允许消费, 本环境为: {env_mark}, msg_env_mark: {msg_env_mark}")
        else:
            print(f"环境不符合，不允许消费, 本环境为: {env_mark}, msg_env_mark: {msg_env_mark}, message_value: {message.value}")
