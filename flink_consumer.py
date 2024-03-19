import os

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import Time, TumblingEventTimeWindows
from pyflink.datastream.connectors.kafka import (
    KafkaSink, KafkaSource, KafkaRecordSerializationSchema, DeliveryGuarantee, KafkaOffsetsInitializer
)

from settings import BOOTSTRAP_SERVER, INPUT_KAFKA_TOPIC, OUTPUT_AGG_KAFKA_TOPIC, OUTPUT_SUM_KAFKA_TOPIC


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    current_dir = os.path.dirname(os.path.abspath(__file__))
    kafka_connector_jar_path = "file:///" + os.path.join(current_dir, "resources", "flink-sql-connector-kafka-3.1.0-1.18.jar")

    env.add_jars(kafka_connector_jar_path)
    env.set_parallelism(1)

    def extract_timestamp(event):
        return event.timestamp

    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(extract_timestamp)

    kafka_source = KafkaSource.builder().set_bootstrap_servers(
        BOOTSTRAP_SERVER
    ).set_topics(
        INPUT_KAFKA_TOPIC
    ).set_group_id(
        "flink-group"
    ).set_starting_offsets(
        KafkaOffsetsInitializer.latest()
    ).set_value_only_deserializer(
        SimpleStringSchema()
    ).build()

    input_stream = env.from_source(kafka_source, watermark_strategy, "Kafka Source")

    def create_kafka_sink(topic):
        kafka_sink = KafkaSink.builder().set_bootstrap_servers(
            BOOTSTRAP_SERVER
        ).set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ).set_delivery_guarantee(
            DeliveryGuarantee.AT_LEAST_ONCE
        ).build()
        return kafka_sink

    sum_sink = create_kafka_sink(OUTPUT_SUM_KAFKA_TOPIC)
    agg_sum_sink = create_kafka_sink(OUTPUT_AGG_KAFKA_TOPIC)

    sum_func = lambda a, b: str(int(a) + int(b))

    # create flink jobs
    # tumbling event time window is dividing messages based on timestamps from kafka
    input_stream.window_all(TumblingEventTimeWindows.of(Time.seconds(10))).reduce(sum_func).sink_to(sum_sink)
    input_stream.window_all(TumblingEventTimeWindows.of(Time.minutes(15))).reduce(sum_func).sink_to(agg_sum_sink)

    env.execute("Flink Kafka Sum")

if __name__ == '__main__':
    main()
