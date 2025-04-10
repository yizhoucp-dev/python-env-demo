import os
import threading

from flask import Flask

from kafka_demo import send_kafka_msg, kafka_consumer_thread


# 创建Flask应用
app = Flask(__name__)

# 创建并启动Kafka消费者线程
threading.Thread(target=kafka_consumer_thread, daemon=True).start()


@app.route('/call_method')
def call_method():
    if os.getenv("envMark"):
        return os.getenv("envMark")
    else:
        return "ok"


@app.route('/api/v1/health')
def health():
    return "ok"


@app.route('/send_kafka_msg')
def send_kafka_msg_api():
    send_kafka_msg()
    return "ok"


if __name__ == '__main__':
    RUN_PORT = os.getenv("FLASK_PORT", 5000)
    # 启动Flask应用
    app.run(host='0.0.0.0', port=RUN_PORT)
