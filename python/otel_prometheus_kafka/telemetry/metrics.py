from typing import Dict, Any
from enum import Enum
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

METRICS_PREFIX = "pubsub_client"

class ErrorType(Enum):
    SCHEMA_VALIDATION = "schema_validation"
    PRODUCER = "producer"
    CONSUMER = "consumer"


class Metrics:
    def __init__(self, endpoint, driver):
        self.endpoint = endpoint
        self.driver = driver
        self.meter = self.get_metrics_provider(endpoint, driver)
        self._init_common_metrics()
        self._init_driver_metrics()

    def get_metrics_provider(self, endpoint, driver):
        exporter = OTLPMetricExporter(endpoint=endpoint)
        reader = PeriodicExportingMetricReader(exporter, export_interval_millis=5000)
        resource = Resource.create({"driver": driver})
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(provider)
        return metrics.get_meter(METRICS_PREFIX)

    def _init_common_metrics(self):
        """
        <metric_name> - <metric_type> - {<label name>=<label value>, ...}
        # bytes_out - counter - {driver=<>, cluster=<>, queue=<>, service=<>}
        # bytes_in - counter - {driver=<>, cluster=<>, queue=<>, consumer=<>, service=<>}
        # producer_latency_ms - histogram - {driver=<>, cluster=<>, queue=<>, service=<>}
        # consumer_latency_ms - histogram - {driver=<>, cluster=<>, queue=<>, service=<>}
        # record_published - counter - {driver=<>, cluster=<>, queue=<>, service=<>}
        # record_consumed - counter - {driver=<>, cluster=<>, queue=<>, consumer=<>, service=<>}
        # producer_retries - counter - {driver=<>, cluster=<>, queue=<>, service=<>}
        # consumer_retries - counter - {driver=<>, cluster=<>, queue=<>, consumer=<>}
        # schema_validation_error - counter - {driver=<>, cluster=<>, queue=<>, consumer<>, type=<>, service=<>}
        # producer_error - counter - {driver=<>, cluster=<>, queue=<>, type=<>, service=<>}
        # consumer_error - counter - {driver=<>, cluster=<>, queue=<>, type=<>, consumer=<>, service=<>}
        """

        self.bytes_in = self.meter.create_counter(
            f"{METRICS_PREFIX}_bytes_in", description="Bytes in by consumer"
        )
        self.bytes_out = self.meter.create_counter(
            f"{METRICS_PREFIX}_bytes_out", description="Bytes out by producer"
        )
        self.record_published = self.meter.create_counter(
            f"{METRICS_PREFIX}_record_published",
            description="Number of records published",
        )
        self.record_consumed = self.meter.create_counter(
            f"{METRICS_PREFIX}_record_consumed",
            description="Number of records consumed",
        )
        self.producer_retries = self.meter.create_counter(
            f"{METRICS_PREFIX}_producer_retries",
            description="Number of retries by producer",
        )
        self.consumer_retries = self.meter.create_counter(
            f"{METRICS_PREFIX}_consumer_retries",
            description="Number of retries by consumer on consuming single message",
        )
        self.producer_latency = self.meter.create_histogram(
            f"{METRICS_PREFIX}_producer_latency",
            description="Time taken to produce a record (in ms)",
        )
        self.consumer_latency = self.meter.create_histogram(
            f"{METRICS_PREFIX}_consumer_latency",
            description="Time take to consume a record (in ms)",
        )

        self.schema_validation_error = self.meter.create_counter(
            f"{METRICS_PREFIX}_schema_validation_error",
            description="Schema validation error either during producing or consuming",
        )

        self.producer_error = self.meter.create_counter(
            f"{METRICS_PREFIX}_producer_error", description="Errors on producer"
        )

        self.consumer_error = self.meter.create_counter(
            f"{METRICS_PREFIX}_consumer_error", description="Errors on consumer"
        )

    def _init_driver_metrics(self):
        raise NotImplementedError()

    # producer metrics

    def record_bytes_out(self, value: int, attrs: Dict[str, Any]):
        """
        A counter metric to measure bytes_out at producer
        value: length of record.value in bytes
        attrs: {"driver": "kafka", "queue": "blinkit.test-topic", "cluster": "pubsub-kraft-cluster", "consumer": "blinkit.demo-service.sample-consumer"}
        """
        self.bytes_out.add(value, attributes=attrs)

    def record_producer_latency(self, value: float, attrs: Dict[str, Any]):
        """
        A Histogram metric to record time it takes to produce a message
        value: time diff in ms
        attrs:
        {"driver": "kafka","queue": "blinkit.test-topic", "cluster": "pubsub-kraft-cluster", "consumer": "blinkit.demo-service.sample-consumer"}
        """
        self.producer_latency.record(value, attributes=attrs)

    def record_published_records(self, value, attrs: Dict[str, Any]):
        """
        A counter metric to measure record_published at producer
        value: number of record(s)
        attrs: {"driver": "kafka", "queue": "blinkit.test-topic", "cluster": "pubsub-kraft-cluster"}
        """
        self.record_published.add(value, attrs)

    def record_producer_retries(self, value: int, attrs: Dict[str, Any]):
        self.producer_retries.add(value, attrs)

    # consumer metrics
    def record_bytes_in(self, value: int, attrs: Dict[str, Any]):
        """
        A Counter metric to measure bytes_in at consumer
        value: length of record.value in bytes
        attrs: {"driver": "kafka", "queue": "blinkit.test-topic", "cluster": "pubsub-kraft-cluster", "consumer": "blinkit.demo-service.sample-consumer"}
        """
        self.bytes_in.add(value, attributes=attrs)

    def record_consumer_latency(self, value: float, attrs: Dict[str, Any]):
        """
        A histogram metric to record time it takes on consuming a single message
        value: time diff in ms
        attrs:
        {"driver": "kafka","queue": "blinkit.test-topic", "cluster": "pubsub-kraft-cluster", "consumer": "blinkit.demo-service.sample-consumer"}
        """
        self.consumer_latency.record(value, attributes=attrs)

    def record_consumed_records(self, value: int, attrs: Dict[str, Any]):
        """
        A counter metric to measure record_consumed at consumer
        value: number of record(s)
        attrs: {"driver": "kafka", "queue": "blinkit.test-topic", "cluster": "pubsub-kraft-cluster", "consumer": "blinkit.demo-service.sample-consumer"}
        """
        self.record_consumed.add(value, attrs)

    def record_consumer_retries(self, value, attrs: Dict[str, Any]):
        self.consumer_retries.add(value, attrs)

    # error metrics
    def record_error(self, error_type: ErrorType, value: int, attrs: Dict[str, Any]):
        if error_type == ErrorType.PRODUCER:
            self.producer_error.add(value, attrs)
        elif error_type == ErrorType.CONSUMER:
            self.consumer_error.add(value, attrs)
        elif error_type == ErrorType.SCHEMA_VALIDATION:
            self.schema_validation_error.add(value, attrs)


class KafkaMetrics(Metrics):
    def __init__(self, endpoint):
        super().__init__(endpoint, driver="kafka")

    def _init_driver_metrics(self):
        self.rebalances = self.meter.create_counter(f"{METRICS_PREFIX}_rebalances")
        self.rebalancing_time = self.meter.create_histogram(
            f"{METRICS_PREFIX}_rebalancing_time", description="Time took in rebalancing"
        )

    # kafka consumer metrics
    def record_rebalances(self, value: int, attrs: Dict[str, Any]):
        """
        A Counter metric to measure rebalances happening on consumer
        value: rebalance count
        attrs: {"driver": "kafka", "queue": "blinkit.test-topic", "consumer": "blinkit.demo-service.sample-consumer", "partition": "1"}
        """
        self.rebalances.add(value, attributes=attrs)

    def record_rebalancing_time(self, value: float, attrs: Dict[str, Any]):
        """
        A histogram metric to record the time it takes when a consumer leaves the group and joins back i.e rebalancing
        """
        self.rebalancing_time.record(value, attrs)
