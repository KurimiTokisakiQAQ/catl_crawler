import json
from datetime import datetime
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class KafkaDataProducer:
    def __init__(self, kafka_servers: list, topic: str):
        self.kafka_servers = kafka_servers
        self.topic = topic
        self.producer = None
        self.batch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def connect(self) -> bool:
        """连接到Kafka集群"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info(f"成功连接到Kafka集群: {self.kafka_servers}")
            return True
        except Exception as e:
            logger.error(f"连接Kafka失败: {e}")
            return False

    def create_message(self, domain_name: str, dc_name: str, data_json: Dict) -> Dict:
        """创建标准格式的消息"""
        return {
            "domain_name": domain_name,
            "dc_name": dc_name,
            "meta_json": "",
            "data_json": data_json,
            "data_html": "",
            "dc_batch_time": self.batch_time,
            "dc_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    def send_message(self, message: Dict) -> bool:
        """发送单条消息到Kafka"""
        if not self.producer:
            logger.error("Kafka生产者未初始化，请先调用connect()方法")
            return False

        try:
            future = self.producer.send(self.topic, message)
            # 等待消息发送确认
            record_metadata = future.get(timeout=10)
            logger.debug(
                f"消息发送成功: topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")
            return True
        except KafkaError as e:
            logger.error(f"Kafka发送失败: {e}")
            return False
        except Exception as e:
            logger.error(f"发送消息异常: {e}")
            return False

    def send_batch_messages(self, messages: list) -> int:
        """批量发送消息"""
        if not self.producer:
            logger.error("Kafka生产者未初始化，请先调用connect()方法")
            return 0

        success_count = 0
        for message in messages:
            if self.send_message(message):
                success_count += 1

        return success_count

    def close(self):
        """关闭生产者连接"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka生产者连接已关闭")


# 全局Kafka生产者实例
_kafka_producer = None


def init_kafka_producer(kafka_servers: list = None, topic: str = None) -> KafkaDataProducer:
    """初始化全局Kafka生产者"""
    global _kafka_producer

    if kafka_servers is None:
        kafka_servers = ['172.21.87.116:9092', '172.21.87.119:9092', '172.21.84.110:9092']

    if topic is None:
        topic = 'topic_idc_raw_data_base'

    _kafka_producer = KafkaDataProducer(kafka_servers, topic)
    if _kafka_producer.connect():
        return _kafka_producer
    else:
        return None


def get_kafka_producer() -> Optional[KafkaDataProducer]:
    """获取全局Kafka生产者实例"""
    return _kafka_producer


def send_station_list_message(city_data: Dict) -> bool:
    """发送站点列表消息（每个城市一条）"""
    producer = get_kafka_producer()
    if not producer:
        logger.error("Kafka生产者未初始化")
        return False

    message = producer.create_message(
        domain_name="www.chocolateswap.com",
        dc_name="chocolateswap_station_list",
        data_json=city_data
    )

    return producer.send_message(message)


def send_station_detail_message(station_detail: Dict) -> bool:
    """发送站点详情消息（每个站点一条）"""
    producer = get_kafka_producer()
    if not producer:
        logger.error("Kafka生产者未初始化")
        return False

    message = producer.create_message(
        domain_name="www.chocolateswap.com",
        dc_name="chocolateswap_station_detail",
        data_json=station_detail
    )

    return producer.send_message(message)


def close_kafka_producer():
    """关闭全局Kafka生产者"""
    global _kafka_producer
    if _kafka_producer:
        _kafka_producer.close()
        _kafka_producer = None