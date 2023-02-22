from abc import ABC, abstractmethod
from kafka.consumer.fetcher import ConsumerRecord


class KafkaAssembler(ABC):
    @abstractmethod
    def assemble(self, kafka_message: ConsumerRecord):
        pass
