import click
import json
from confluent_kafka import Producer, Consumer
import time
from pympler import asizeof

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

def setup_opentelemetry():
    # check otel-collector-config.yaml
    # otel collector grpc endpoint: http://localhost:4317
    exporter = OTLPMetricExporter(endpoint="http://localhost:4317", insecure=True)
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=1000)
    resource = Resource.create({"service.name": "kafka-consumer"})
    provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(provider)
    return metrics.get_meter("kafka-client")

meter = setup_opentelemetry()


meter_records_produced = meter.create_counter("kafka_client_records_produced_total", description="Number of records produced on kafka topic") 
meter_bytes_in = meter.create_histogram("kafka_client_bytes_in", description="bytes in during consume")
meter_bytes_out =  meter.create_histogram("kafka_client_bytes_out", description="bytes out during produce")
meter_consumer_latency = meter.create_histogram("kafka_client_consumer_latency", description="Time taken to process a record in ms")

def get_producer_client(config):
    return Producer(config)

def get_consumer_client(config, group_id):
    config.update({"group.id": group_id, "auto.offset.reset": "latest"})
    return Consumer(config)

def create_config(brokers, auth, mechanism, username, password):
    config = {
        "bootstrap.servers": brokers
    }
    if auth:
        config.update({
            "security.protocol": "sasl_ssl",
            "sasl.mechanism": mechanism,
            "sasl.username": username,
            "sasl.password": password
        })
    return config

@click.command()
@click.option('--consume/--produce', help='Choose to consume from or produce to a topic')
@click.option('--brokers', default='localhost:9092', help='Kafka brokers, comma-separated')
@click.option('--topic', required=True, help='Topic to consume from or publish to')
@click.option('--auth', is_flag=True, help='Is auth required?')
@click.option('--mechanism', default='SCRAM-SHA-512', help='Authentication mechanism (e.g., SCRAM-SHA-512)')
@click.option('--username', help='username')
@click.option('--password', help='password')
@click.option('--msg-key', default="test-key", help="Kafka msg's key (for produce only)")
@click.option('--msg-value', default="test-value", help="Kafka msg's value (for produce only)")
@click.option('--group-id', default="test-cg", help="Kafka's consumer group_id")
def main(consume, brokers, topic, auth, mechanism, username, password, msg_key, msg_value, group_id):
    config = create_config(brokers, auth, mechanism, username, password)

    if consume:
        consumer = get_consumer_client(config, group_id=group_id)
        consumer.subscribe([topic])
        print(f"Consuming messages from topic: {topic}")
        while True:
            start_time = time.time()
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            print(f"Received message: {msg.value().decode('utf-8')}")
            consumer.commit(msg)
            meter_consumer_latency.record((time.time()-start_time)*100, {"topic": topic, "cluster": "kafka_docker", "group_id": group_id})
            # confluent_kafka.Message class not method `len`. this is incorrect https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Message.len
            # instead we have to do len(msg). see issue: https://github.com/confluentinc/confluent-kafka-python/issues/859#issuecomment-625059399
            meter_bytes_in.record(len(msg), {"topic": topic, "cluster": "kafka_docker"})
    else:
        producer = get_producer_client(config)
        producer.produce(topic, key=msg_key.encode("utf-8"), value=json.dumps(msg_value).encode("utf-8"), headers={"source": "local".encode("utf-8")})
        producer.flush()
        meter_bytes_out.record(asizeof.asizeof(msg_value), {"topic": topic, "cluster": "kafka_docker"})
        meter_records_produced.add(1, {"topic": topic, "cluster": "kafka_docker"})
        print(f"Message produced to topic: {topic}")

if __name__ == "__main__":
    main()