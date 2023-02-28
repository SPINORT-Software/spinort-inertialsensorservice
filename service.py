import os
from dotenv import load_dotenv
import logging
from localStoragePy import localStoragePy

from consumer import Consumer
from kafka_alert import KafkaAlertApi
from assemblers.mvn_data_assembler import MVNDataAssembler
from udp_consumer import UDPConsumer

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SERVICE_BASE_DIR = os.path.dirname(__file__)


class Service:
    def __init__(self, configuration, confluent_config):
        self.local_storage = localStoragePy(configuration.get_local_storage_workspace_name(),
                                            configuration.get_local_storage_workspace_backend())
        self.kafka_alert = KafkaAlertApi(configuration, self.local_storage)
        self.configuration = configuration
        self.confluent_config = confluent_config

    def start_ipc_consumer_thread(self):
        consumer = Consumer(
            self.configuration.get_kafka_consumer_configuration(),
            self.configuration.get_kafka_ipc_topic(),
            callback_function=self.kafka_alert.accept_record
        )
        consumer.start()

    def start_udp_consumer_thread(self):
        mvn_data_assembler = MVNDataAssembler(self.configuration, self.confluent_config, self.local_storage)
        udp_consumer = UDPConsumer(
            mvn_data_assembler=mvn_data_assembler
        )
        udp_consumer.start()
