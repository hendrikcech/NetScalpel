{
  description = "netmeas and stltrace go applications";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        common = {
          version = "0.1";
          src = ./.;
          vendorHash = "sha256-a7x4WkuuNfgKxeb35wQIP9kp0hRUWErXsElywzStMNU=";
        };

        netmeas = pkgs.buildGoModule (common // {
          pname = "netmeas";
          subPackages = [ "cmd/netmeas" ];
        });

        stltrace = pkgs.buildGoModule (common // {
          pname = "stltrace";
          subPackages = [ "cmd/stltrace" ];
        });
      in
      {
        packages = {
          inherit netmeas stltrace;
          default = netmeas;
        };
      }
    );
}
