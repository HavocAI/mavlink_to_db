#!/usr/bin/env python3
import argparse
import csv
import logging
import os.path
from typing import Any, Dict, List
from bisect import bisect_left

from pymavlink.DFReader import DFMessage, DFReader_binary  # type: ignore

_logger = logging.getLogger('mavlink_csv_parser')

MAX_TIME_DIFF = 500 * 1000  # 500 milliseconds in microseconds

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse .bin file and save BAT: Volt, Curr and GPS: Spd fields to CSV.")
    parser.add_argument('filename', help="Log filename")
    parser.add_argument('output', help="Output CSV filename")
    args = parser.parse_args()

    log = DFReader_binary(args.filename, False)

    bat_data: List[Dict[str, Any]] = []
    gps_data: List[Dict[str, Any]] = []

    # Iterate through logfile and extract required fields
    while True:
        entry: DFMessage = log.recv_msg()
        if entry is None:
            _logger.debug("No more log entries, break from processing loop")
            break

        if entry.fmt.name == 'BAT' and entry.Inst == 0:
            bat_data.append({
                'timestamp': entry.TimeUS,
                'Volt': entry.Volt,
                'Curr': entry.Curr
            })
        elif entry.fmt.name == 'GPS':
            gps_data.append({
                'timestamp': entry.TimeUS,
                'Spd': entry.Spd
            })

    # Sort the data by timestamp
    bat_data.sort(key=lambda x: x['timestamp'])
    gps_data.sort(key=lambda x: x['timestamp'])

    # Extract timestamps for binary search
    gps_timestamps = [entry['timestamp'] for entry in gps_data]

    # Join BAT and GPS data on closest timestamp within 500 ms
    joined_data = []
    for bat_entry in bat_data:
        timestamp = bat_entry['timestamp']
        pos = bisect_left(gps_timestamps, timestamp)

        closest_gps_entry = None
        closest_time_diff = float('inf')

        # Check the closest entries around the found position
        for i in range(max(0, pos - 1), min(len(gps_data), pos + 2)):
            time_diff = abs(gps_data[i]['timestamp'] - timestamp)
            if time_diff < closest_time_diff:
                closest_time_diff = time_diff
                closest_gps_entry = gps_data[i]

        if closest_gps_entry and closest_time_diff <= MAX_TIME_DIFF:
            joined_data.append({
                'timestamp': bat_entry['timestamp'],
                'Volt': bat_entry['Volt'],
                'Curr': bat_entry['Curr'],
                'Spd': closest_gps_entry['Spd']
            })

    # Write joined data to CSV
    with open(args.output, 'w', newline='') as csvfile:
        fieldnames = ['timestamp', 'Volt', 'Curr', 'Spd']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for data in joined_data:
            writer.writerow(data)


if __name__ == "__main__":
    main()