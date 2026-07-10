# Package definitions shared between the root flake and e2e.
# The e2e flake imports this with src = ../.. instead of declaring the root
# flake as an input: a `path:../..` input would copy the whole working tree
# (including gitignored results) into the store, while the enclosing git
# fetcher only picks up tracked files.
{ pkgs, src }:
let
  common = {
    version = "0.1";
    inherit src;
    vendorHash = "sha256-a7x4WkuuNfgKxeb35wQIP9kp0hRUWErXsElywzStMNU=";
  };
in
{
  scalpel-run = pkgs.buildGoModule (
    common
    // {
      pname = "scalpel-run";
      subPackages = [ "cmd/scalpel-run" ];
    }
  );

  scalpel-exp = pkgs.buildGoModule (
    common
    // {
      pname = "scalpel-exp";
      subPackages = [ "cmd/scalpel-exp" ];
    }
  );
}
