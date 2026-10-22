# Package definitions shared between the root flake and e2e.
# The e2e flake imports this with src = ../.. instead of declaring the root
# flake as an input: a `path:../..` input would copy the whole working tree
# (including gitignored results) into the store, while the enclosing git
# fetcher only picks up tracked files.
{ pkgs, src, version ? "2.0.0-dev", provenance ? { } }:
let
  common = {
    inherit version;
    ldflags = [ "-X=github.com/hendrikcech/netscalpel/internal/buildinfo.Version=${version}" ]
      ++ pkgs.lib.mapAttrsToList
        (name: value: "-X=github.com/hendrikcech/netscalpel/internal/buildinfo.${name}=${value}")
        provenance;
    inherit src;
    vendorHash = "sha256-gTXzfrIqbBtEcc7sDh/evydV/FEXRdK5MjYECLP6dTM=";
  };
in
{
  scalpel-run = pkgs.buildGo127Module (
    common
    // {
      pname = "scalpel-run";
      subPackages = [ "cmd/scalpel-run" ];
    }
  );

  scalpel-exp = pkgs.buildGo127Module (
    common
    // {
      pname = "scalpel-exp";
      subPackages = [ "cmd/scalpel-exp" ];
    }
  );
}
