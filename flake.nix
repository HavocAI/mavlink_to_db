{
  description = "Upload MAVLink dataflash logs to InfluxDB";

  inputs = {
    utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, utils }:
    with utils.lib;
    eachSystem allSystems (system: let
      pkgs = nixpkgs.legacyPackages.${system};
    in {
      packages.default = pkgs.python3Packages.callPackage ./. { };

      apps.default = mkApp { drv = self.packages.${system}.default; };

      devShell = import ./shell.nix { inherit pkgs; };
    }) //
    eachSystem [ "x86_64-linux" ] (system: {
      hydraJobs.build = self.packages.${system}.default;
    });
}
