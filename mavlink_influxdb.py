#!/usr/bin/env python3
import argparse
import datetime
import logging
import math
import os.path
from typing import Any, Dict, List, TextIO

import influxdb_client  # type: ignore
from pymavlink.DFReader import DFMessage, DFReader_binary  # type: ignore

_logger = logging.getLogger('mavlink_influxdb')


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload dataflash logs to InfluxDB.")
    parser.add_argument('filename', help="Log filename")
    parser.add_argument('--url', default='http://localhost:8086',
                        help="InfluxDB server url")
    parser.add_argument('--token', help="InfluxDB API token",
                        default="Sisp44nQHVdnYXGnTy3eza515Z6Wrumx62vwd_In5tFBUkHZ9OtWFaFJNex7sl0NdmU9p9lf8eRDviz2YrXbnQ==")
    parser.add_argument('--bucket', default='mav_rocket',
                        help="InfluxDB bucket name")
    parser.add_argument('--vehicle',
                        help="Vehicle name (stored in 'vehicle' tag)")
    args = parser.parse_args()

    log = DFReader_binary(args.filename, False)

    client = influxdb_client.InfluxDBClient(
        url=args.url,
        token=args.token,
        org="HavocAI",
        bucket=args.bucket
        )

    write_api = client.write_api(write_options=influxdb_client.client.write_api.SYNCHRONOUS)

    common_tags: Dict[str, str] = {
        'filename': os.path.basename(args.filename)
    }
    if args.vehicle:
        common_tags['vehicle'] = args.vehicle

    line_protocol_data: List[str] = []

    # Iterate through logfile, process data and import into InfluxDB
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
            # and perhaps InfluxDB itself can't handle it.
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

        fields_str = []
        for k, v in fields.items():
            if isinstance(v, str) and v not in ['t', 'T', 'true', 'True', 'TRUE', 'f', 'F', 'false', 'False', 'FALSE']:
                v = f'"{v}"'
            fields_str.append(f'{k}={v}')

        line_protocol = f"{msg_type},{','.join([f'{k}={v}' for k, v in tags.items()])} {','.join(fields_str)} {timestamp_ns}"

        line_protocol_data.append(line_protocol)

        # Batch writes to influxdb, much faster
        if len(line_protocol_data) > 20000:
            try:
                write_api.write(args.bucket, "HavocAI", line_protocol_data)
            except Exception as e:
                _logger.error(f"Error writing to InfluxDB: {e}")
            finally:
                line_protocol_data = []  # Clear out after bulk write

    # Flush remaining points
    if len(line_protocol_data) > 0:
        try:
            write_api.write(args.bucket, "HavocAI", line_protocol_data)
        except Exception as e:
            _logger.error(f"Error writing to InfluxDB: {e}")
        finally:
            line_protocol_data = []  # Clear out after bulk write


if __name__ == "__main__":
    main()
