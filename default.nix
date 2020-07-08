{ lib, nixShell ? false, buildPythonApplication, mypy, pymavlink
, influxdb, autopep8, pylint, rope }:

buildPythonApplication {
  name = "mavlink-influxdb";

  # lib.inNixShell can't be used here because it will return a false positive
  # if this package is pulled into a shell
  src = if nixShell then null else lib.cleanSourceWith {
    filter = name: type: let baseName = baseNameOf (toString name); in !(
      # Filter out mypy cache
      (baseName == ".mypy_cache" && type == "directory")
    );
    src = lib.cleanSource ./.;
  };

  nativeBuildInputs = [ mypy ];
  propagatedBuildInputs = [ pymavlink influxdb ];
  # Devlopment dependencies
  buildInputs = lib.optionals nixShell [ autopep8 pylint rope ];

  meta = with lib; {
    description = "Upload MAVLink dataflash logs to InfluxDB";
    license = licenses.mit;
    maintainers = with maintainers; [ lopsided98 ];
  };
}
