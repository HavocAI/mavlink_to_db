#!/usr/bin/env python3
import argparse
import datetime
import logging
import math
import os.path
from typing import Any, Dict, List, TextIO

import influxdb  # type: ignore
from pymavlink.DFReader import DFMessage, DFReader_binary  # type: ignore

_logger = logging.getLogger('mavlink_influxdb')


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload dataflash logs to InfluxDB.")
    parser.add_argument('filename', help="Log filename")
    parser.add_argument('--hostname', required=True,
                        help="InfluxDB server hostname")
    parser.add_argument('--port', type=int, default=8086,
                        help="InfluxDB server port")
    parser.add_argument('--certificate', help="InfluxDB client certificate")
    parser.add_argument('--username',
                        help="InfluxDB username", default='mavlink')
    parser.add_argument('--password-file', type=argparse.FileType('r'),
                        help="File containing InfluxDB password")
    parser.add_argument('--database', default='mavlink',
                        help="InfluxDB database name")
    parser.add_argument('--vehicle',
                        help="Vehicle name (stored in 'vehicle' tag)")
    args = parser.parse_args()

    log = DFReader_binary(args.filename, False)

    password: str
    if args.password_file:
        password_file: TextIO
        with args.password_file as password_file:
            password = password_file.read().rstrip('\r\n')
    else:
        password = 'mavlink'

    client = influxdb.InfluxDBClient(
        host=args.hostname,
        port=args.port,
        database=args.database,
        username=args.username,
        password=password,
        ssl=bool(args.certificate),
        verify_ssl=bool(args.certificate),
        cert=args.certificate)

    tags: Dict[str, str] = {
        'filename': os.path.basename(args.filename)
    }
    if args.vehicle:
        tags['vehicle'] = args.vehicle

    json_points: List[Dict[str, Any]] = []
    counter = 0

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
            # Skip NaNs, otherwise add data to fields
            if isinstance(field, float):
                if not math.isnan(field):
                    fields[field_name] = field
            else:
                fields[field_name] = field

        json_body: Dict[str, Any] = {
            'database': args.database,
            'time_precision': 'ns',
            'measurement': msg_type,
            'time': timestamp_ns,
            'tags': tags,
            'fields': fields
        }
        json_points.append(json_body)
        counter += 1
        # Batch writes to influxdb, much faster
        if counter > 0 and counter % 20000 == 0:
            client.write_points(json_points)
            json_points = []  # Clear out json_points after bulk write


if __name__ == "__main__":
    main()
