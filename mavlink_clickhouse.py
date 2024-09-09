#!/usr/bin/env python3
import argparse
import logging
import os.path
import math
from typing import Any, Dict, List, Tuple

import requests
from pymavlink.DFReader import DFMessage, DFReader_binary  # type: ignore

_logger = logging.getLogger('mavlink_clickhouse')

MAX_BATCH_SIZE = 20000

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload dataflash logs to ClickHouse.")
    parser.add_argument('filename', help="Log filename")
    parser.add_argument('--url', default='https://clickhouse.stg.havocai.net:443',
                        help="ClickHouse server URL")
    parser.add_argument('--user', help="ClickHouse username", default='default')
    parser.add_argument('--password', help="ClickHouse password", default='')
    parser.add_argument('--database', default='logs',
                        help="ClickHouse database name")
    parser.add_argument('--table', default='ardupilot',
                        help="ClickHouse table name")
    parser.add_argument('--vehicle',
                        help="Vehicle name (stored in 'vehicle' tag)")
    args = parser.parse_args()

    log = DFReader_binary(args.filename, False)

    common_tags: Dict[str, str] = {
        'filename': os.path.basename(args.filename)
    }
    if args.vehicle:
        common_tags['vehicle'] = args.vehicle

    insert_data: List[Tuple] = []

    # Extract schema from log file
    schema = extract_schema(log)

    # Create table if it doesn't exist
    create_table_if_not_exists(args.url, args.database, args.table, args.user, args.password, schema)

    # Iterate through logfile, process data and import into ClickHouse
    while True:
        entry: DFMessage = log.recv_msg()
        if entry is None:
            _logger.debug("No more log entries, break from processing loop")
            break
        msg_type = entry.fmt.name
        timestamp_ns = int(float(entry._timestamp) * 1000000000)
        fields = {}
        for field_name in entry.get_fieldnames():
            field = getattr(entry, field_name)
            # Skip NaNs
            if isinstance(field, float) and math.isnan(field):
                continue
            # Skip fields that can't be decoded as UTF-8, as the Python client
            # and perhaps ClickHouse itself can't handle it.
            if isinstance(field, bytes):
                try:
                    field.decode('utf-8')
                except UnicodeDecodeError:
                    _logger.debug("skipping non UTF-8 field: %s.%s=%s",
                                  msg_type, field_name, field)
                    continue

            fields[field_name] = field

        tags = {}
        if entry.fmt.instance_field is not None:
            tags['instance'] = fields[entry.fmt.instance_field]

        if not tags:
            tags = {'default_tag': 'default_value'}

        insert_data.append((timestamp_ns, msg_type, fields))

        # Batch writes to ClickHouse, much faster
        if len(insert_data) >= MAX_BATCH_SIZE:
            try:
                upload_to_clickhouse(args.url, args.database, args.table, args.user, args.password, insert_data)
            except Exception as e:
                _logger.error(f"Error writing to ClickHouse: {e}")
            finally:
                insert_data = []  # Clear out after bulk write

    # Flush remaining points
    if len(insert_data) > 0:
        try:
            upload_to_clickhouse(args.url, args.database, args.table, args.user, args.password, insert_data)
        except Exception as e:
            _logger.error(f"Error writing to ClickHouse: {e}")
        finally:
            insert_data = []  # Clear out after bulk write


def extract_schema(log: DFReader_binary) -> Dict[str, str]:
    schema = {}
    while True:
        entry: DFMessage = log.recv_msg()
        if entry is None:
            break
        for field_name in entry.get_fieldnames():
            field = getattr(entry, field_name)
            if field_name not in schema:
                if isinstance(field, int):
                    schema[field_name] = 'Int64'
                elif isinstance(field, float):
                    schema[field_name] = 'Float64'
                elif isinstance(field, str):
                    schema[field_name] = 'String'
                elif isinstance(field, bytes):
                    schema[field_name] = 'String'
                else:
                    schema[field_name] = 'String'
    return schema


def create_table_if_not_exists(url: str, database: str, table: str, user: str, password: str, schema: Dict[str, str]) -> None:
    fields_schema = ', '.join([f"{name} {dtype}" for name, dtype in schema.items()])
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {database}.{table} (
        timestamp UInt64,
        msg_type String,
        {fields_schema}
    ) ENGINE = MergeTree()
    ORDER BY timestamp
    """
    response = requests.post(url, params={'query': create_table_query}, auth=(user, password))
    response.raise_for_status()


def upload_to_clickhouse(url: str, database: str, table: str, user: str, password: str, data: List[Tuple]) -> None:
    values = []
    for entry in data:
        timestamp_ns, msg_type, fields = entry
        fields_str = ', '.join([f"{k}={repr(v)}" for k, v in fields.items()])
        values.append(f"({timestamp_ns}, '{msg_type}', {fields_str})")
    
    query = f"INSERT INTO {database}.{table} (timestamp, msg_type, {', '.join(fields.keys())}) VALUES {', '.join(values)}"
    response = requests.post(url, params={'query': query}, auth=(user, password))
    response.raise_for_status()


if __name__ == "__main__":
    main()