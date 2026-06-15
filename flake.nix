{
  description = "NetScalpel: scalpel-run and scalpel-exp go applications";

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

        scalpelRun = pkgs.buildGoModule (common // {
          pname = "scalpel-run";
          subPackages = [ "cmd/scalpel-run" ];
        });

        scalpelExp = pkgs.buildGoModule (common // {
          pname = "scalpel-exp";
          subPackages = [ "cmd/scalpel-exp" ];
        });
      in
      {
        packages = {
          "scalpel-run" = scalpelRun;
          "scalpel-exp" = scalpelExp;
          default = scalpelRun;
        };
      }
    );
}
