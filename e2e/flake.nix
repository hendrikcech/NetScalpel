{
  description = "End-to-end validation of NetScalpel measurements against netem ground truth";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixnet.url = "github:birneee/nixnet";
    nixnet.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = inputs.nixnet.supportedSystems;
      perSystem =
        { pkgs, inputs', ... }:
        let
          lib = pkgs.lib;
          nixnet = inputs'.nixnet.legacyPackages;

          # Avoid copying the working tree with all untracked results
          # into the store on every evaluation.
          scalpelRun =
            (import ../scalpel.nix {
              inherit pkgs;
              src = ../.;
            }).scalpel-run;

          clientIP = "10.0.0.1";
          serverIP = "10.0.1.2";

          verify = "${pkgs.python3}/bin/python3 ${./verify.py}";

          # Topology: client -- router -- server, with netem only on the
          # router's egress interfaces. Emulation must not sit on the sender's
          # own interface: kernel software TX timestamps are taken in the
          # driver, after the egress qdisc, so netem delay/loss configured
          # there would be invisible to the sender's timestamps. On the router
          # it is crossed between the sender's TX and the receiver's RX
          # timestamp no matter which timestamp path the code takes.
          #
          # netemUL shapes router->server (uplink), netemDL router->client
          # (downlink). `client` runs measurements against the server and is
          # awaited: the experiment fails if any command in it fails. `check`
          # runs in the testbed context (cwd = out/<scenario>/<run>) after the
          # client script finished and can see all node directories.
          mkScenario =
            {
              name,
              netemUL,
              netemDL,
              client,
              check,
            }:
            {
              inherit name;
              workDir = "out/${name}/{run}";
              # Prefilled ARP tables keep address resolution (which the loss
              # and delay emulation would also hit) out of the measurements.
              arp = false;
              arpPrefill = true;
              nodePackages = [
                scalpelRun
                pkgs.coreutils
              ];
              nodes = {
                client = {
                  networking.interfaces.veth0.ipv4 = {
                    addresses = [
                      {
                        address = clientIP;
                        prefixLength = 24;
                      }
                    ];
                    routes = [
                      {
                        address = "10.0.1.0";
                        prefixLength = 24;
                        via = "10.0.0.2";
                      }
                    ];
                  };
                  scripts.main = {
                    exec = ''
                      sleep 1 # wait for the server to open its RPC socket
                      ${client}
                    '';
                    await = true;
                  };
                  # Let the ICMP test use unprivileged datagram sockets (the
                  # path production measurements take) instead of raw sockets.
                  sysctl."net.ipv4.ping_group_range" = "0 0"; # only mapped GIDs are allowed inside the user namespace
                };
                router = {
                  networking.interfaces = {
                    veth0 = {
                      ipv4.addresses = [
                        {
                          address = "10.0.0.2";
                          prefixLength = 24;
                        }
                      ];
                      netem = netemDL;
                    };
                    veth1 = {
                      ipv4.addresses = [
                        {
                          address = "10.0.1.1";
                          prefixLength = 24;
                        }
                      ];
                      netem = netemUL;
                    };
                  };
                  sysctl."net.ipv4.ip_forward" = true;
                };
                server = {
                  networking.interfaces.veth1.ipv4 = {
                    addresses = [
                      {
                        address = serverIP;
                        prefixLength = 24;
                      }
                    ];
                    routes = [
                      {
                        address = "10.0.0.0";
                        prefixLength = 24;
                        via = "10.0.1.1";
                      }
                    ];
                  };
                  scripts.main.exec = "scalpel-run server --log ./server.log --log-level=-4";
                  sysctl."net.ipv4.ping_group_range" = "0 0"; # only mapped GIDs are allowed inside the user namespace
                };
              };
              veths.veth0 = {
                a.node = "client";
                b.node = "router";
              };
              veths.veth1 = {
                a.node = "router";
                b.node = "server";
              };
              postRun = check;
            };

          configs = {
            # Scenario 1: OWD baseline. Both endpoints share the host clock,
            # so the 20 ms netem delay is exact ground truth for owd_ms.
            owd = mkScenario {
              name = "e2e-owd";
              netemUL = {
                delayMs = 20;
              };
              netemDL = {
                delayMs = 20;
              };
              client = ''
                scalpel-run udp-periodic --ip ${serverIP} --interval 10 --duration 3000 \
                  --out ./owd_ul.csv --log ./owd_ul.log --log-level=-4
                scalpel-run udp-periodic --ip ${serverIP} --direction dl --interval 10 --duration 3000 \
                  --out ./owd_dl.csv --log ./owd_dl.log --log-level=-4
              '';
              check = ''
                ${verify} owd --csv client/owd_ul.csv --delay-ms 20 --min-packets 250
                ${verify} owd --csv client/owd_dl.csv --delay-ms 20 --min-packets 250
                # UL TX timestamps are taken by the client, DL ones by the server
                ${verify} txts --log client/owd_ul.log --log server/server.log
              '';
            };

            # Scenario 2: loss. 2500 packets per direction; the observed loss
            # rate must fall within a binomial acceptance interval around 5%.
            loss = mkScenario {
              name = "e2e-loss";
              netemUL = {
                delayMs = 5;
                lossPercent = 5;
              };
              netemDL = {
                delayMs = 5;
                lossPercent = 5;
              };
              client = ''
                scalpel-run udp-periodic --ip ${serverIP} --interval 1 --duration 2500 \
                  --out ./loss_ul.csv --log ./loss_ul.log --log-level=-4
                scalpel-run udp-periodic --ip ${serverIP} --direction dl --interval 1 --duration 2500 \
                  --out ./loss_dl.csv --log ./loss_dl.log --log-level=-4
              '';
              check = ''
                ${verify} loss --csv client/loss_ul.csv --loss-percent 5 --min-packets 2000
                ${verify} loss --csv client/loss_dl.csv --loss-percent 5 --min-packets 2000
              '';
            };

            # Scenario 3: rate + queue. udp-rate sends ~80 Mbps into a 50 Mbit
            # bottleneck with a 200-packet queue: the receive rate must pin at
            # the netem rate and owd_ms must grow from the bare 10 ms delay to
            # a queue-full plateau bounded by the limit (~56 ms).
            rate = mkScenario {
              name = "e2e-rate";
              netemUL = {
                delayMs = 10;
                rateMbit = 50;
                limit = 200;
              };
              netemDL = {
                delayMs = 10;
                rateMbit = 50;
                limit = 200;
              };
              client = ''
                scalpel-run udp-rate --ip ${serverIP} --rate 80 --duration 3000 \
                  --out ./rate_ul.csv --log ./rate_ul.log --log-level=-4
                scalpel-run udp-rate --ip ${serverIP} --direction dl --rate 80 --duration 3000 \
                  --out ./rate_dl.csv --log ./rate_dl.log --log-level=-4
              '';
              check = ''
                ${verify} rate --csv client/rate_ul.csv --delay-ms 10 --rate-mbit 50 --limit 200
                ${verify} rate --csv client/rate_dl.csv --delay-ms 10 --rate-mbit 50 --limit 200
              '';
            };

            # Scenario 4: asymmetric delay validates the UL and DL code paths
            # independently — a swapped direction shows up as 30 vs 10 ms.
            asym = mkScenario {
              name = "e2e-asym";
              netemUL = {
                delayMs = 30;
              };
              netemDL = {
                delayMs = 10;
              };
              client = ''
                scalpel-run udp-periodic --ip ${serverIP} --interval 10 --duration 3000 \
                  --out ./asym_ul.csv --log ./asym_ul.log --log-level=-4
                scalpel-run udp-periodic --ip ${serverIP} --direction dl --interval 10 --duration 3000 \
                  --out ./asym_dl.csv --log ./asym_dl.log --log-level=-4
              '';
              check = ''
                ${verify} owd --csv client/asym_ul.csv --delay-ms 30 --min-packets 250
                ${verify} owd --csv client/asym_dl.csv --delay-ms 10 --min-packets 250
              '';
            };

            # Scenario 5: ICMP and UDP measurements of the same link must
            # agree on the median OWD.
            icmp-udp = mkScenario {
              name = "e2e-icmp-udp";
              netemUL = {
                delayMs = 20;
              };
              netemDL = {
                delayMs = 20;
              };
              client = ''
                scalpel-run udp-periodic --ip ${serverIP} --interval 10 --duration 3000 \
                  --out ./udp.csv --log ./udp.log --log-level=-4
                scalpel-run icmp --ip ${serverIP} --interval 10 --duration 3000 \
                  --out ./icmp.csv --log ./icmp.log --log-level=-4
              '';
              check = ''
                ${verify} owd --csv client/udp.csv --delay-ms 20 --min-packets 250
                ${verify} owd --csv client/icmp.csv --delay-ms 20 --min-packets 250
                ${verify} compare --csv-a client/udp.csv --csv-b client/icmp.csv --eps-ms 1
              '';
            };

            # Scenario 6: TCP sanity on a 2x20 ms, 50 Mbit path. The
            # 400-packet queue is above the ~173-packet BDP so cubic can reach
            # the line rate.
            tcp = mkScenario {
              name = "e2e-tcp";
              netemUL = {
                delayMs = 20;
                rateMbit = 50;
                limit = 400;
              };
              netemDL = {
                delayMs = 20;
                rateMbit = 50;
                limit = 400;
              };
              client = ''
                scalpel-run tcp --ip ${serverIP} --duration 3000 \
                  --out ./tcp_ul.csv --log ./tcp_ul.log --log-level=-4
                scalpel-run tcp --ip ${serverIP} --direction dl --duration 3000 \
                  --out ./tcp_dl.csv --log ./tcp_dl.log --log-level=-4
              '';
              check = ''
                ${verify} tcp --csv client/tcp_ul.csv --rtt-ms 40 --rate-mbit 50
                ${verify} tcp --csv client/tcp_dl.csv --rtt-ms 40 --rate-mbit 50
              '';
            };
          };
        in
        {
          packages = lib.mapAttrs (_: config: nixnet.mkExperiment config) configs // {
            default = nixnet.mkExperiment configs.owd;
            mermaid = nixnet.mkMermaid configs.owd;
          };
        };
    };
}
