# mavlink_influxdb

Script to upload ArduPilot or PX4 (not tested) dataflash logs (`.BIN`) files to InfluxDB, typically for use with a visualization tool such as Grafana.

Each message type maps to the InfluxDB measurement, tagged with the file name and optionally a vehicle name.

This tool was inspired by [Maverick's mavlogd tool](https://goodrobots.github.io/maverick/current/#/modules/analysis), but is standalone and does not make assumptions about the vehicle type. Unlike mavlogd, this tool does not maintain a log index. Instead, this can be done with Grafana table driven by the following [Flux](https://www.influxdata.com/products/flux/) query:
```
data = from(bucket: "mavlink")
  |> range(start: -1)
  |> filter(fn: (r) =>
    r._measurement == "ATT" and
    r._field == "TimeUS")
  |> group(columns: ["filename", "vehicle"])
start = data |> first()
end = data |> last()
join(tables: {start:start, end:end}, on: ["filename", "vehicle"])
  |> map(fn: (r) => ({
    _time: r._time_start,
    start_ms: int(v: r._time_start) / 1000000,
    end_ms: int(v: r._time_end) / 1000000,
    filename: r.filename,
    vehicle: r.vehicle
  }))
  |> group()
  // Safety limit to prevent accidental massive queries
  |> limit(n: 500)
```

The `start_ms` and `end_ms` columns can be used to link to another dashboard with the time range that cooresponds to the chosen log file.