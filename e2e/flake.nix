{
  description = "NetScalpel smoke tests and measurement benchmarks on nixnet";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixnet.url = "github:birneee/nixnet";
    nixnet.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs =
    inputs@{ self, flake-parts, ... }:
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
              provenance = if self ? rev then { Revision = self.rev; Dirty = "false"; }
                else if self ? dirtyRev then {
                  Revision = lib.removeSuffix "-dirty" self.dirtyRev;
                  Dirty = "true";
                } else { };
            }).scalpel-run;

          clientIP = "10.0.0.1";
          serverIP = "10.0.1.2";

          source = ../.;
          verify = "${pkgs.python3}/bin/python3 ${source}/e2e/verify.py";
          report = "${pkgs.python3}/bin/python3 ${source}/benchmarks/report.py";

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
          mkScenario = import ./scenario.nix {
            inherit pkgs scalpelRun clientIP serverIP source;
          };

          owdClient = ''
            scalpel-run udp-periodic --ip ${serverIP} --interval 10 --duration 3000 \
              --out ./owd_ul.csv --log ./owd_ul.log --log-level=-4
            scalpel-run udp-periodic --ip ${serverIP} --direction dl --interval 10 --duration 3000 \
              --out ./owd_dl.csv --log ./owd_dl.log --log-level=-4
          '';

          configs = {
            # Capture after the router qdisc, at the actual DL receiver. Avoid
            # probabilistic expectations: compare every copy against the pcap.
            duplicates = mkDeliveryScenario "duplicates" "delay 20ms duplicate 100%";
            reordering = mkDeliveryScenario "reordering" "delay 50ms reorder 100% gap 5";

            # Same clock removes clock offset, not host/qdisc scheduling jitter.
            owd = mkScenario {
              name = "e2e-owd";
              netemUL = {
                delayMs = 20;
              };
              netemDL = {
                delayMs = 20;
              };
              client = owdClient;
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
          experiments = lib.mapAttrs (name: config: nixnet.mkExperiment config) configs;

          # Identical workload/topology, but no latency acceptance policy.
          # Bad accuracy is data; malformed/incomplete measurements still fail.
          benchmarkOWD = nixnet.mkExperiment (mkScenario {
            name = "benchmark-owd";
            netemUL.delayMs = 20;
            netemDL.delayMs = 20;
            client = owdClient;
            check = ''
              ${report} udp client --out report --expected-delay-ms 20 \
                --backend nixnet --execution-status complete \
                --min-packets 250 --expect-csv owd_ul.csv --expect-csv owd_dl.csv \
                --tx-log client/owd_ul.log --tx-log server/server.log
            '';
          });

          mkDeliveryScenario = name: impairment: mkScenario {
            name = "e2e-${name}";
            netemUL = { };
            netemDL = { };
            extraNodePackages = [ pkgs.python3 pkgs.tcpdump pkgs.iproute2 ];
            routerSetup = ''
              # Only impair the eight-byte measurement payloads (UDP length
              # 16), not empty NAT probes or TCP RPC traffic. Otherwise a
              # reordered data packet can be consumed as the probe reply.
              tc qdisc replace dev veth0 root handle 1: prio bands 3 \
                priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
              tc qdisc add dev veth0 parent 1:3 handle 30: netem limit 10000 ${impairment}
              tc filter add dev veth0 parent 1: protocol ip prio 1 u32 \
                match u8 0x45 0xff at 0 match ip protocol 17 0xff \
                match u16 0x0010 0xffff at 24 flowid 1:3
            '';
            client = ''
              python3 ${source}/e2e/udp_delivery.py run --ip ${serverIP} --interface veth0
            '';
            check = ''
              ${pkgs.python3}/bin/python3 ${source}/e2e/udp_delivery.py check --scenario ${name} \
                --csv client/delivery.csv --pcap client/delivery.pcap --capture-log client/capture.log
              ${verify} txts --log server/server.log
            '';
          };

          # Runs every scenario back to back in one invocation. Each
          # experiment's own workDir keys results by scenario name, so
          # they don't clash; a failure in one scenario doesn't stop the
          # rest from running, but is reflected in the final exit code.
          all = pkgs.writeShellScriptBin "e2e-all" (
            ''
              set -u
              status=0
            ''
            + lib.concatMapStringsSep "\n" (name: ''
              echo "=== ${name} ===" >&2
              ${lib.getExe experiments.${name}} "$@" || status=1
            '') (builtins.attrNames experiments)
            + ''
              exit $status
            ''
          );
        in
        {
          packages = experiments // {
            default = experiments.owd;
            mermaid = nixnet.mkMermaid configs.owd;
            all = all;
            smoke = all;
            benchmark-owd = benchmarkOWD;
          };
        };
    };
}
