import os
import signal
from time import time
from confluent_kafka import Producer
from dotenv import load_dotenv
import logging
import random
import json
from faker import Faker

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    level=logging.INFO
)

logger = logging.getLogger(__name__)

load_dotenv(dotenv_path='/app/.env')

fake = Faker()

class TransactionProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.kafka_username = os.getenv('KAFKA_USERNAME')
        self.kafka_password = os.getenv('KAFKA_PASSWORD')
        self.topic = os.getenv('KAFKA_TOPIC', 'transactions')
        self.running = False

        # confluent kafka config
        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client_id': 'transaction-producer',
            'compression.type': 'gzip',
            'linger.ms': 5,
            'batch.size': 15000
            }
        if self.kafka_username and self.kafka_password:
            self.producer_config.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanism': 'PLAIN',
                'sasl.username': self.kafka_username,
                'sasl.password': self.kafka_password
                })
        else:
            self.producer_config({'security.protocol': 'PLAINTEXT'})
        try:
            self.producer = Producer(self.producer_config)
            logger.info('Confluent Kafka producer initialized successfully')
        except Exception as e:
            logger.error(f'Failed to initialize Kafka producer: {e}')
            raise e

        self.compromised_users = set(random.sample(range(1000, 9999), 50)) # 0.5% of users compromised
        self.high_risk_merchants = ['QuickCash', 'FastMoneyX', 'GlobalDigital']
        self.fraud_patterns_weights = {
            'account_takeover': 0.4, # 40% of fraud cases
            'card_testing': 0.3,
            'merchant_collusion': 0.2,
            'geo_anomaly': 0.1
        }
        
        # Configure graceful shutdown
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def send_transaction(self) -> bool:
        """Generate and send a single transaction message to Kafka."""
        try:
            transaction = self.generate_transaction()
            if not transaction:
                return False
        self.producer.produce(
            self.topic,
            key=str(transaction['transaction_id']),
            value=json.dumps(transaction),
        )


    def run_continuous_production(self, interval:float=0.0):
        """Run continuous message production until interrupted. (graceful shutdown on SIGINT/SIGTERM)"""
        self.running = True
        logger.info('Starting continuous transaction production...', self.topic)

        try:
            while self.running:
                if self.send_transaction():
                    time.sleep(interval)
                transaction = self.generate_transaction()
                self.producer.produce(self.topic, key=str(transaction['transaction_id']), value=str(transaction))
                self.producer.poll(0)  # Trigger delivery report callbacks
                if interval > 0:
                    time.sleep(interval)
        except Exception as e:
            logger.error(f'Error during production: {e}')
        finally:
            self.shutdown()


    def shutdown(self, signum=None, frame=None):
        if self.running:
            logger.info('Shutting down producer...')
            self.running = False

            if self.producer:
                self.producer.flush(timeout=30)
            logger.info('Producer shut down successfully.')
        else:
            logger.info('Producer is not running.')

    

if __name__ == "__main__":
    producer = TransactionProducer()
    producer.run_continuous_production()