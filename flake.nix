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

        scalpel = import ./scalpel.nix {
          inherit pkgs;
          src = ./.;
        };
      in
      {
        packages = {
          "scalpel-run" = scalpel.scalpel-run;
          "scalpel-exp" = scalpel.scalpel-exp;
          default = scalpel.scalpel-run;
        };
      }
    );
}
