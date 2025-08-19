import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from rich import print
import pandas as pd
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# Config from env (with defaults)
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka")
KAFKA_PORT = os.getenv("KAFKA_PORT", "29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_NAME", "tick-data")

BOOTSTRAP_SERVERS = [f"{KAFKA_SERVER}:{KAFKA_PORT}"]

def to_iso_utc(s: str) -> str:
    """
    '15-11-21 09:15:42.50' -> '2021-11-15T09:15:42.500Z'
    Assumes the input time is already UTC. Always outputs milliseconds + 'Z'.
    """
    s = s.strip()
    for fmt in ("%d-%m-%y %H:%M:%S.%f", "%d-%m-%y %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)           # naive -> treat as UTC
            dt = dt.replace(tzinfo=timezone.utc)     # mark as UTC (no conversion)
            return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        except ValueError:
            continue
    print("Error data: ", s)
    raise ValueError(f"Unrecognized timestamp: {s}")

def wait_for_broker(timeout=60):
    """Wait until Kafka broker is available."""
    start_time = time.time()
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            # Force metadata request to broker
            producer.partitions_for(KAFKA_TOPIC)
            producer.close()
            print(f"Kafka broker available at {BOOTSTRAP_SERVERS}")
            return
        except NoBrokersAvailable:
            elapsed = int(time.time() - start_time)
            if elapsed > timeout:
                raise RuntimeError(f"Kafka not available after {timeout}s")
            print(f"Waiting for Kafka... ({elapsed}s elapsed)")
            time.sleep(2)

def readDataFromDataSet(): 
    days = ['08', '09', '10', '11', '12', '13', '14'] # '08', '09', '10', '11', '12', '13', '14' 
    cols = ["ID", "SecType", "Last", "Trading time"]
    for day in days:
        dd = f'{day}-11-21'
        for chunk in pd.read_csv(
            f'../seed/dataset/debs2022-gc-trading-day-{day}-11-21.csv', 
                sep=',',
                chunksize=10000,
                comment='#',
                index_col=False,          # <-- important
                encoding='utf-8-sig',     # handles possible BOM
                skipinitialspace=True,     # trims spaces after commas
                usecols=cols,
        ):
            for row in chunk.itertuples(index=False, name=None):
                id_, secType, last, trading_time = row
                if pd.isna(last):
                    continue
                dt = datetime.strptime(f'{dd} {trading_time}', "%d-%m-%y %H:%M:%S.%f" if '.' in trading_time else "%d-%m-%y %H:%M:%S")
                if dt.hour == 0 and dt.minute == 0:
                    continue
                last = float(last)
                tick = {
                    "id_index": id_,
                    "sec_type": secType,
                    "last": last,
                    "trading_date_time": to_iso_utc(f'{dd} {trading_time}')
                }
                yield tick


def main():
    wait_for_broker()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print('Producing Kafka messages...')
    for tick in readDataFromDataSet():
        try:
            producer.send(KAFKA_TOPIC, value=tick)
            print(tick)
        except Exception as e:
            print(f"Error sending message to Kafka: {e}")

    producer.flush()
    producer.close()
    


if __name__ == "__main__":
    main()
