#!/usr/bin/env python3
import argparse
import datetime
import logging
import math
import os.path
from typing import Any, Dict, List, TextIO

from pymavlink.DFReader import DFMessage, DFReader_binary  # type: ignore

_logger = logging.getLogger('mavlink_influxdb')


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Print schema of dataflash logs.")
    parser.add_argument('filename', help="Log filename")
    parser.add_argument('--vehicle',
                        help="Vehicle name (stored in 'vehicle' tag)")
    args = parser.parse_args()

    log = DFReader_binary(args.filename, False)

    common_tags: Dict[str, str] = {
        'filename': os.path.basename(args.filename)
    }
    if args.vehicle:
        common_tags['vehicle'] = args.vehicle

    schema: Dict[str, Dict[str, str]] = {}

    # Iterate through logfile, process data and collect schema information
    while True:
        entry: DFMessage = log.recv_msg()
        if entry is None:
            _logger.debug("No more log entries, break from processing loop")
            break
        msg_type = entry.fmt.name
        if msg_type not in schema:
            schema[msg_type] = {}

        for field_name in entry.get_fieldnames():
            field = getattr(entry, field_name)
            field_type = type(field).__name__
            schema[msg_type][field_name] = field_type

    # Print out the schema
    for msg_type, fields in schema.items():
        print(f"Message Type: {msg_type}")
        for field_name, field_type in fields.items():
            print(f"  {field_name}: {field_type}")
        print()


if __name__ == "__main__":
    main()