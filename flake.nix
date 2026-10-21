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
          provenance = (if self ? rev then { Revision = self.rev; Dirty = "false"; }
            else if self ? dirtyRev then {
              Revision = pkgs.lib.removeSuffix "-dirty" self.dirtyRev;
              Dirty = "true";
            } else { }) // (if self ? lastModifiedDate then {
              Time = let
                d = self.lastModifiedDate;
                part = start: length: builtins.substring start length d;
              in "${part 0 4}-${part 4 2}-${part 6 2}T${part 8 2}:${part 10 2}:${part 12 2}Z";
            } else { });
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
