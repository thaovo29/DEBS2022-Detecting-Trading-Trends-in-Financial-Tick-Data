import json
import logging
from datetime import datetime

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
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
        "bootstrap.servers": server
    }

    offset = KafkaOffsetsInitializer.latest()
    if earliest:
        offset = KafkaOffsetsInitializer.earliest()

    kafka_source = (
        KafkaSource.builder()
        .set_topics("tick-test")
        .set_properties(properties)
        .set_starting_offsets(offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return kafka_source


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


def main2() -> None:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    # Initialize environment
    logger.info("Initializing environment")
    env = initialize_env()

    logger.info("Configuring source and sinks")
    settings = EnvironmentSettings.in_streaming_mode()
    tenv = StreamTableEnvironment.create(env, settings)

    src_dll = """
        CREATE TABLE tick_readings (
            id_index VARCHAR,
            sec_type VARCHAR,
            last FLOAT,
            trading_date_time TIMESTAMP_LTZ(3),
            kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
            WATERMARK FOR trading_date_time AS trading_date_time - INTERVAL 5 MINUTE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'tick-test',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',
            'json.timestamp-format.standard' = 'ISO-8601',
            'properties.auto.offset.reset' = 'earliest'
        )
    """
    tenv.execute_sql(src_dll)
    tick_readings_tab = tenv.from_path("tick_readings")

    logger.info("Creating sink table (Kafka)…")
    tenv.execute_sql(f"""
        CREATE TEMPORARY TABLE tick_window (
          id_index STRING,
          window_start TIMESTAMP_LTZ(3),
          window_end   TIMESTAMP_LTZ(3),
          last   FLOAT
        ) WITH (
          'connector' = 'kafka',
          'topic' = 'tick-window',
          'properties.bootstrap.servers' = 'kafka:29092',
          'format' = 'json',
          'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    #  Process a tumbling window
    tumbling_window_sql = """
        INSERT INTO tick_window
        SELECT id_index, window_start, window_end, last
        FROM (
          SELECT
            id_index,
            window_start,
            window_end,
            last,
            ROW_NUMBER() OVER (
              PARTITION BY id_index, window_start, window_end
              ORDER BY trading_date_time DESC, kafka_offset DESC
            ) AS rn
          FROM TABLE(
            TUMBLE(TABLE tick_readings, DESCRIPTOR(trading_date_time), INTERVAL '5' MINUTE)
          )
        ) WHERE rn = 1
    """

    result = tenv.execute_sql(tumbling_window_sql)
    result.wait()


if __name__ == "__main__":
    main2()
