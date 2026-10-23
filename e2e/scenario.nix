# Shared nixnet lifecycle for correctness tests and measurement benchmarks.
{ pkgs, scalpelRun, clientIP, serverIP, source }:
{
  name,
  netemUL,
  netemDL,
  client,
  check ? "",
  extraNodePackages ? [ ],
  routerSetup ? "",
}:
{
  inherit name;
  workDir = "out/${name}/{run}";
  arp = false;
  arpPrefill = true;
  nodePackages = [ scalpelRun pkgs.coreutils pkgs.python3 ] ++ extraNodePackages;
  nodes = {
    client = {
      networking.interfaces.veth0.ipv4 = {
        addresses = [ { address = clientIP; prefixLength = 24; } ];
        routes = [ { address = "10.0.1.0"; prefixLength = 24; via = "10.0.0.2"; } ];
      };
      scripts.main = {
        exec = ''
          python3 ${source}/measurements/ready.py ${serverIP} 8500 --timeout 15
          scalpel-run --version > build.txt
          sha256sum "$(command -v scalpel-run)" > binary.sha256
          ${client}
        '';
        await = true;
      };
      # Only mapped GIDs are allowed inside the user namespace.
      sysctl."net.ipv4.ping_group_range" = "0 0";
    };
    router = {
      postSetup = routerSetup;
      networking.interfaces = {
        veth0 = {
          ipv4.addresses = [ { address = "10.0.0.2"; prefixLength = 24; } ];
          netem = netemDL;
        };
        veth1 = {
          ipv4.addresses = [ { address = "10.0.1.1"; prefixLength = 24; } ];
          netem = netemUL;
        };
      };
      sysctl."net.ipv4.ip_forward" = true;
    };
    server = {
      networking.interfaces.veth1.ipv4 = {
        addresses = [ { address = serverIP; prefixLength = 24; } ];
        routes = [ { address = "10.0.0.0"; prefixLength = 24; via = "10.0.1.1"; } ];
      };
      scripts.main.exec = ''
        scalpel-run --version > build.txt
        sha256sum "$(command -v scalpel-run)" > binary.sha256
        scalpel-run server --log ./server.log --log-level=-4
      '';
      sysctl."net.ipv4.ping_group_range" = "0 0";
    };
  };
  veths = {
    veth0 = { a.node = "client"; b.node = "router"; };
    veth1 = { a.node = "router"; b.node = "server"; };
  };
  postRun = check;
}
