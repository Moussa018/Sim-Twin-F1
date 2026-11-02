"""Kafka -> TimescaleDB sink.

A consumer group (f1-sink): start several instances and Kafka splits the
partitions between them. Offsets are committed by hand, only after the batch is
safely in the database (at-least-once). Invalid messages go to telemetry.dlq
with the error in a header instead of blocking the stream.

Env: KAFKA_BROKERS, PG_DSN, GROUP_ID (f1-sink), METRICS_PORT (8003)
"""
import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, start_http_server
from psycopg2.extras import execute_values

log = logging.getLogger("sink")

WRITTEN = Counter("f1_sink_rows_written_total", "Rows written to TimescaleDB.", ["table"])
REJECTED = Counter("f1_sink_dlq_total", "Messages sent to the dead letter topic.", ["topic"])

# topic -> (table, columns, how to read the columns from the JSON payload)
TABLES = {
    "telemetry.raw": ("telemetry",
                      ["time", "driver", "speed", "throttle", "brake", "gear", "rpm", "tyre_temp", "tyre_wear"]),
    "race.laps": ("lap",
                  ["time", "driver", "lap_number", "lap_duration", "sector1", "sector2", "sector3", "is_pit_out"]),
    "race.events": ("race_event",
                    ["time", "category", "flag", "message", "lap_number", "driver", "scope"]),
}
TABLES["telemetry.predictions"] = ("prediction", ["time", "driver", "metric", "value"])
REQUIRED = {"telemetry": ["time", "driver"], "lap": ["time", "driver", "lap_number"], "race_event": ["time"],
            "prediction": ["time", "driver", "metric"]}

DDL = """
CREATE TABLE IF NOT EXISTS lap (
    time TIMESTAMPTZ NOT NULL, driver TEXT NOT NULL, lap_number INT NOT NULL,
    lap_duration DOUBLE PRECISION, sector1 DOUBLE PRECISION, sector2 DOUBLE PRECISION,
    sector3 DOUBLE PRECISION, is_pit_out BOOLEAN);
CREATE TABLE IF NOT EXISTS race_event (
    time TIMESTAMPTZ NOT NULL, category TEXT, flag TEXT, message TEXT,
    lap_number INT, driver TEXT, scope TEXT);
"""


def connect(dsn):
    while True:
        try:
            conn = psycopg2.connect(dsn)
            with conn.cursor() as cur:
                cur.execute(DDL)
            conn.commit()
            return conn
        except psycopg2.OperationalError as e:
            log.warning("database not ready: %s", e)
            time.sleep(3)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    start_http_server(int(os.getenv("METRICS_PORT", "8003")))
    conn = connect(os.getenv("PG_DSN", "host=localhost dbname=f1 user=f1 password=f1pass"))

    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": os.getenv("GROUP_ID", "f1-sink"),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(list(TABLES))
    dlq = Producer({"bootstrap.servers": brokers, "enable.idempotence": True, "acks": "all"})
    log.info("consuming %s", ", ".join(TABLES))

    try:
        while True:
            msgs = consumer.consume(num_messages=500, timeout=1.0)
            rows = {}  # table -> list of tuples
            for m in msgs:
                if m.error():
                    log.warning("kafka: %s", m.error())
                    continue
                table, cols = TABLES[m.topic()]
                try:
                    data = json.loads(m.value())
                    if any(data.get(c) is None for c in REQUIRED[table]):
                        raise ValueError(f"missing {REQUIRED[table]}")
                    rows.setdefault(table, []).append(tuple(data.get(c) for c in cols))
                except Exception as e:
                    REJECTED.labels(m.topic()).inc()
                    dlq.produce("telemetry.dlq", key=m.key(), value=m.value(),
                                headers=[("error", str(e).encode()), ("topic", m.topic().encode())])
            dlq.poll(0)
            if not msgs:
                continue

            try:
                with conn.cursor() as cur:
                    for table, items in rows.items():
                        cols = next(c for t, c in TABLES.values() if t == table)
                        execute_values(cur, f"INSERT INTO {table} ({','.join(cols)}) VALUES %s", items)
                        WRITTEN.labels(table).inc(len(items))
                conn.commit()
            except psycopg2.Error as e:
                # Offsets were not committed: after a restart the group re-reads this batch.
                log.error("database error, exiting so the container restarts: %s", e)
                raise SystemExit(1)
            dlq.flush(5)
            consumer.commit(asynchronous=False)  # only after DB commit and DLQ flush
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
