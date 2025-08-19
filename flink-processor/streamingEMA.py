import json
import logging
from datetime import datetime
from utils.EMA import EMA
from utils.EMATick import EMATick
from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import JdbcSink
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

KAFKA_HOST = "kafka:29092"
POSTGRES_HOST = "timescaledb:5432"
current_tick_parse = {}
current_tick_detect = {}

def _parse_iso_utc(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):               # e.g. "2021-11-15T06:15:00Z"
        s = s[:-1] + "+00:00"         # -> "2021-11-15T06:15:00+00:00"
    return datetime.fromisoformat(s)  # timezone-aware datetime

def parse_data(data: str) -> Row:
    obj = json.loads(data)
    id_index = obj["id_index"]
    window_start = _parse_iso_utc(obj["window_start"])
    window_end = _parse_iso_utc(obj["window_end"])
    last = float(obj["last"])
    
    if id_index not in current_tick_parse:
        prev38 = None
        prev100 = None
    else:
        prev38 = current_tick_parse[id_index].ema38
        prev100 = current_tick_parse[id_index].ema100
    ema38 = EMA(38, last).calculate(prev38)
    ema100 = EMA(100, last).calculate(prev100)
    current_tick_parse[id_index] = EMATick(ema38, ema100)
    return Row(id_index, window_start, window_end, last, ema38, ema100)

def find_breakout_pattern(data: str) -> str | None:
    obj = json.loads(data)
    id_index = obj["id_index"]
    window_start = obj["window_start"]
    window_end = obj["window_end"]
    last = float(obj["last"])
    
    if id_index not in current_tick_detect:
        prev38 = None
        prev100 = None
    else:
        prev38 = current_tick_detect[id_index].ema38
        prev100 = current_tick_detect[id_index].ema100
    ema38 = EMA(38, last).calculate(prev38)
    ema100 = EMA(100, last).calculate(prev100)
    current_tick_detect[id_index] = EMATick(ema38, ema100)
    if prev38 and prev100:
        if prev100 > prev38 and ema100 < ema38:
            bullish_message = {
                "id_index": id_index,
                "window_start": window_start,
                "window_end": window_end,
                "last": last,
                "alert": "bullish",
                "ema38": ema38,
                "ema100": ema100
            }
            return json.dumps(bullish_message)
        elif prev100 < prev38 and ema100 > ema38:
            bearish_message = {
                "id_index": id_index,
                "window_start": window_start,
                "window_end": window_end,
                "last": last,
                "alert": "bearish",
                "ema38": ema38,
                "ema100": ema100
            }
            return json.dumps(bearish_message)
    return None

def initialize_env() -> StreamExecutionEnvironment:
    """Makes stream execution environment initialization"""
    env = StreamExecutionEnvironment.get_execution_environment()

    # Get current directory
    root_dir_list = __file__.split("/")[:-1]
    root_dir = "/".join(root_dir_list)

    # Adding the jar to the flink streaming environment
    env.add_jars(
        f"file://{root_dir}/lib/flink-connector-jdbc-3.1.2-1.18.jar",
        f"file://{root_dir}/lib/postgresql-42.7.3.jar",
        f"file://{root_dir}/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
    )
    return env


def configure_source(server: str, earliest: bool = False) -> KafkaSource:
    """Makes kafka source initialization"""
    properties = {
        "bootstrap.servers": server,
    }

    offset = KafkaOffsetsInitializer.latest()
    if earliest:
        offset = KafkaOffsetsInitializer.earliest()

    kafka_source = (
        KafkaSource.builder()
        .set_topics("tick-window")
        .set_properties(properties)
        .set_starting_offsets(offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return kafka_source


def configure_postgre_sink(sql_dml: str, type_info: Types) -> JdbcSink:
    """Makes postgres sink initialization. Config params are set in this function."""
    return JdbcSink.sink(
        sql_dml,
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(f"jdbc:postgresql://{POSTGRES_HOST}/database")
        .with_driver_name("org.postgresql.Driver")
        .with_user_name("username")
        .with_password("password")
        .build(),
        JdbcExecutionOptions.builder()
        .with_batch_interval_ms(1000)
        .with_batch_size(200)
        .with_max_retries(5)
        .build(),
    )

def configure_kafka_sink(server: str, topic_name: str) -> KafkaSink:

    return (
        KafkaSink.builder()
        .set_bootstrap_servers(server)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic_name)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )


def main() -> None:
    """Main flow controller"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    # Initialize environment
    logger.info("Initializing environment")
    env = initialize_env()

    # Define source and sinks
    logger.info("Configuring source and sinks")
    kafka_source = configure_source(KAFKA_HOST, earliest=True)
    sql_dml = (
        "INSERT INTO tick_ema_window_data (id_index, window_start, window_end, last, ema38, ema100) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    TYPE_INFO = Types.ROW(
        [
            Types.STRING(),  # id_index
            Types.SQL_TIMESTAMP(),  # window_start
            Types.SQL_TIMESTAMP(),  # window_end
            Types.FLOAT(),  # last
            Types.FLOAT(),  # ema38
            Types.FLOAT(),  # ema100
        ]
    )
    jdbc_sink = configure_postgre_sink(sql_dml, TYPE_INFO)
    kafka_sink = configure_kafka_sink(KAFKA_HOST, "breakout-pattern")
    logger.info("Source and sinks initialized")

    # Create a DataStream from the Kafka source and assign watermarks
    data_stream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka tick ema topic"
    )

    # Make transformations to the data stream
    ema_tick_data = data_stream.map(parse_data, output_type=TYPE_INFO)
    breakout_pattern_data = data_stream.map(
        find_breakout_pattern, output_type=Types.STRING()
    ).filter(lambda x: x is not None)
    logger.info("Defined ema tick data to data stream")

    logger.info("Ready to sink data")
    breakout_pattern_data.print()
    breakout_pattern_data.sink_to(kafka_sink)
    ema_tick_data.add_sink(jdbc_sink)

    # Execute the Flink job
    env.execute("Flink PostgreSQL and Kafka Sink streaming EMA")


if __name__ == "__main__":
    main()