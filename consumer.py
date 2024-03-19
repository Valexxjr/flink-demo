import os

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.state import ValueStateDescriptor

from settings import BOOTSTRAP_SERVERS, KAFKA_TOPIC

class SumProcessFunction(KeyedProcessFunction):
    def open(self, runtime_context):
        state_desc = ValueStateDescriptor("sum", Types.INT())
        self.sum_state = runtime_context.get_state(state_desc)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        current_sum = self.sum_state.value()
        if current_sum is None:
            current_sum = 0
        current_sum += int(value)
        self.sum_state.update(current_sum)
        print("wait")
        ctx.timer_service().register_event_time_timer(ctx.timestamp() + 100000)

    def on_timer(self, timestamp, ctx: 'KeyedProcessFunction.OnTimerContext'):
        current_sum = self.sum_state.value()
        if current_sum is not None:
            ctx.output(str(current_sum))


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    current_dir = os.path.dirname(os.path.abspath(__file__))

    kafka_connector_jar_path = "file:///" + os.path.join(current_dir, "resources", "flink-sql-connector-kafka-3.1.0-1.18.jar")

    env.add_jars(kafka_connector_jar_path)
    env.set_parallelism(1)

    # Define Kafka consumer properties
    kafka_props = {
        'bootstrap.servers': BOOTSTRAP_SERVERS[0],
        "group.id": "flink-group",
        "auto.offset.reset": "latest"
    }

    # Create Kafka consumer
    kafka_consumer = FlinkKafkaConsumer(KAFKA_TOPIC, SimpleStringSchema(), kafka_props)

    # Create Kafka producer
    kafka_producer = FlinkKafkaProducer("result-topic", SimpleStringSchema(), kafka_props)

    input_stream = env.add_source(kafka_consumer)

    # Вычисляем сумму каждые 10 секунд
    sums = input_stream.key_by(lambda x: "key").process(SumProcessFunction())

    # Помещаем сумму в новый топик
    sums.add_sink(kafka_producer)

    # Запускаем программу
    env.execute("Flink Kafka Sum")

if __name__ == '__main__':
    main()
