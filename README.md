netmeas
----
Run different network tests. Only works on Linux as it used TX and RX kernel timestamps. Requires go (golang) to be built.

The client fetches all information from the server and prints csv data after a test completes.

``` sh
Usage: netmeas <command> [flags]

Flags:
  -h, --help              Show context-sensitive help.
      --ip="0.0.0.0"      Server IP.
      --port=8500         Server port.
      --direction="ul"    Direction: UL or DL.
      --log=STRING        Write debug log to this file.
      --out=STRING        CSV file to write results to (default: stdout).

Commands:
  udp-burst [flags]
    Send a burst of UDP packets.

  udp-rate [flags]
    Send UDP packets at a steady rate.

  udp-periodic [flags]
    Send UDP packets at a constant interval.

  tcp [flags]
    Send TCP at a constant interval. Both duration and bytes can be specified.

  server [flags]
    Run in server mode.

Run "netmeas <command> --help" for more information on a command.
```


## Example
``` sh
# Build netmeas
go build ./cmd/netmeas

# Start the server
$ ./netmeas server
time=2025-08-12T23:07:53.194+02:00 level=INFO msg="Listening on TCP for RPC calls" addr=0.0.0.0:8500

# In a different shell, start the client to send UDP packets for 1 s with a gap of 200 ms
$ ./netmeas udp-periodic --ip 127.0.0.1 --interval=200 --duration=1000
time=2025-08-12T23:13:00.830+02:00 level=INFO msg="Call Client.Run (UL)" type=*pkg.PeriodicSender params="{Interval:200ms Duration:1s Pad:0}" remoteAddr=127.0.0.1:41715
seq,ts_sent,ts_rcvd,owd_ms,size,lost
0,2025-08-12T23:13:01.031176846+02:00,2025-08-12T23:13:01.031180953+02:00,0.004107,8,false
1,2025-08-12T23:13:01.231781232+02:00,2025-08-12T23:13:01.231787423+02:00,0.006191,8,false
2,2025-08-12T23:13:01.431189857+02:00,2025-08-12T23:13:01.431192886+02:00,0.003029,8,false
3,2025-08-12T23:13:01.631625106+02:00,2025-08-12T23:13:01.63162806+02:00,0.002954,8,false
4,2025-08-12T23:13:01.831040549+02:00,2025-08-12T23:13:01.831044604+02:00,0.004055,8,false
1.000s	5 packets sent, 5 rcvd (0.00% lost)	0.00 Mbps

# Tests run in uplink (UL, from client to server by default) direction by default
# this can be changed with --direction dl
```

## UDP Tests
UDP tests provide information about each sent packet in CSV format.
- seq: The sequence number starting at 0.
- ts_sent: The timestamp taken when the packet is sent. This is retrieved from the kernel by requesting software transmission timestamps.
- ts_rcvd: The timestamp when the packet was received by the server (empty if the packet was lost). Linux receive timestamping is used.
- owd_ms: The calculated one-way delay in milliseconds.
- lost: boolean indicating if the packet was lost.

``` csv
seq,ts_sent,ts_rcvd,owd_ms,size,lost
0,2025-08-12T23:13:01.031176846+02:00,2025-08-12T23:13:01.031180953+02:00,0.004107,8,false
1,2025-08-12T23:13:01.231781232+02:00,2025-08-12T23:13:01.231787423+02:00,0.006191,8,false
2,2025-08-12T23:13:01.431189857+02:00,2025-08-12T23:13:01.431192886+02:00,0.003029,8,false
3,2025-08-12T23:13:01.631625106+02:00,2025-08-12T23:13:01.63162806+02:00,0.002954,8,false
4,2025-08-12T23:13:01.831040549+02:00,2025-08-12T23:13:01.831044604+02:00,0.004055,8,false
```

## TCP Tests
Information about TCP connections is sampled every 5 ms.
See `generateTCPResultRows()` in `./pkg/client.go` for more information about each field.

The transfers can be bounded by the number of bytes (`--bytes`) or their duration (`--duration` in milliseconds).
Both flags can be used and the transfer stop once one of the two conditions is met.
Please note that *writing* to the TCP connection stops after `--duration`.
The sender waits until all written data is flushed and all data is ACKed before returning.

```
$ ./netmeas tcp --ip 127.0.0.1 --bytes 100000

Time,State,SenderMSS,ReceiverMSS,RTT,RTTVar,RTO,ATO,LastDataSent,LastDataReceived,LastAckReceived,ReceiverWindow,SenderSSThreshold,ReceiverSSThreshold,SenderWindowBytes,SenderWindowSegs,PathMTU,CAState,Retransmissions,Backoffs,WindowOrKeepAliveProbes,UnackedSegs,SackedSegs,LostSegs,RetransSegs,ForwardAckSegs,ReorderedSegs,ReceiverRTT,TotalRetransSegs,PacingRate,ThruBytesAcked,ThruBytesReceived,SegsOut,SegsIn,NotSentBytes,MinRTT,DataSegsOut,DataSegsIn,BBRMaxBW,BBRMinRTT,BBRPacingGain,BBRCongWindowGain
2025-09-24T14:09:22.809948975+02:00,established,1400,536,15.601,7.8,250,0,4,4,4,14480,2147483647,64088,0,10,1500,open,0,0,0,0,0,0,0,0,3,0,0,1794756,1,0,2,1,0,15.601,0,0,,,,
2025-09-24T14:09:22.810733868+02:00,established,1400,536,15.601,7.8,250,0,0,4,4,14480,2147483647,64088,0,10,1500,open,0,0,0,10,0,0,0,0,3,0,0,1794756,1,0,12,1,86000,15.601,10,0,,,,
2025-09-24T14:09:22.815291558+02:00,established,1400,536,15.601,7.8,250,0,4,7,7,14480,2147483647,64088,0,10,1500,open,0,0,0,10,0,0,0,0,3,0,0,1794756,1,0,12,1,86000,15.601,10,0,,,,
2025-09-24T14:09:22.8204693+02:00,established,1400,536,15.601,7.8,250,0,10,14,14,14480,2147483647,64088,0,10,1500,open,0,0,0,10,0,0,0,0,3,0,0,1794756,1,0,12,1,86000,15.601,10,0,,,,
2025-09-24T14:09:22.825795009+02:00,established,1400,536,15.464,4.622,216.666,0,0,17,0,14480,2147483647,64088,0,14,1500,open,0,0,0,14,0,0,0,0,3,0,0,2534776,5601,0,20,3,74800,14.998,18,0,,,,
2025-09-24T14:09:22.830968035+02:00,established,1400,536,15.844,3.257,216.666,0,4,24,4,14480,2147483647,64088,0,20,1500,open,0,0,0,20,0,0,0,0,3,0,0,3534265,14001,0,32,5,58000,14.998,30,0,,,,
2025-09-24T14:09:22.835158935+02:00,established,1400,536,15.844,3.257,216.666,0,7,27,7,14480,2147483647,64088,0,20,1500,open,0,0,0,20,0,0,0,0,3,0,0,3534265,14001,0,32,5,58000,14.998,30,0,,,,
2025-09-24T14:09:22.840564121+02:00,established,1400,536,15.352,1.674,216.666,0,4,34,4,14480,2147483647,64088,0,28,1500,open,0,0,0,28,0,0,0,0,3,0,0,5106618,25201,0,48,9,35600,14.645,46,0,,,,
2025-09-24T14:09:22.845821618+02:00,established,1400,536,15.745,1.695,216.666,0,0,37,0,14480,2147483647,64088,0,40,1500,open,0,0,0,40,0,0,0,0,3,0,0,7113143,42001,0,72,15,2000,13.949,70,0,,,,
2025-09-24T14:09:22.850024545+02:00,established,1400,536,15.745,1.695,216.666,0,7,44,7,14480,2147483647,64088,0,40,1500,open,0,0,0,40,0,0,0,0,3,0,0,7113143,42001,0,72,15,2000,13.949,70,0,,,,
2025-09-24T14:09:22.855146764+02:00,established,1400,536,15.116,0.745,216.666,0,0,47,0,14480,2147483647,64088,0,50,1500,open,0,0,0,32,0,0,0,0,3,0,0,9261173,56001,0,74,21,0,13.747,72,0,,,,
2025-09-24T14:09:22.860424163+02:00,established,1400,536,16.136,1.141,216.666,0,7,54,4,14480,2147483647,64088,0,61,1500,open,0,0,0,21,0,0,0,0,3,0,0,10584535,71401,0,74,27,0,13.747,72,0,,,,
2025-09-24T14:09:22.865715655+02:00,established,1400,536,18.011,2.566,220,0,10,57,0,14480,2147483647,64088,0,67,1500,open,0,0,0,15,0,0,0,0,3,0,0,10415856,79801,0,74,32,0,13.747,72,0,,,,
2025-09-24T14:09:22.870976509+02:00,established,1400,536,18.611,2.536,220,0,17,64,4,14480,2147483647,64088,0,82,1500,open,0,0,0,0,0,0,0,0,3,0,0,12336541,100001,0,74,40,0,13.747,72,0,,,,
```

### Analysis
The CSV file can be parsed and manipulated with [pandas](https://pandas.pydata.org/) in Python.

``` python
import pandas as pd

def parse_csv(path):
    df = pd.read_csv(path)
    df["Time"] = pd.to_datetime(df.Time, format="ISO8601")
    df["ts"] = df.Time - df.Time.min()
    df["CAState"] = df.CAState.astype("category")
    df["BBRMaxBW"] = df.BBRMaxBW * 8 / 1e6
    return df

def calculate_goodput(df):
    """
    Compute the goodput over a 100 ms window based on the number of acknowledged bytes.
    """
    df["ThruBytesAckedDiff"] = df.ThruBytesAcked - df.ThruBytesAcked.shift()
    df["tsDiffSec"] = (df.ts - df.ts.shift()).dt.total_seconds()
    roll = df.rolling(window="100ms", on="ts")[["tsDiffSec", "ThruBytesAckedDiff"]].sum()
    return roll["ThruBytesAckedDiff"] / roll["tsDiffSec"] * 8 / 1e6

df = parse_csv("tcp.csv")
df["gput"] = calculate_goodput(df)
```

The dataframe provides a close-up view of the changes of a TCP connection, for example:
- The end of the TCP slow start phase (where `SenderSSThreshold < 2147483647`) due to packet loss (increase of `TotalRetransSegs`)
- The ramp up of the congestion window (`SenderWindowSegs` measured in segments/packets of size `SenderMSS` bytes)
- The estimated round-trip time (`RTT` in milliseconds)
- The estimated goodput (`gput` in Mbps)

``` python
(Pdb) df[["tsDiffSec", "TotalRetransSegs", "SenderWindowSegs", "CAState", "SenderSSThreshold", "RTT"]])

                             ts  TotalRetransSegs  SenderWindowSegs   CAState  SenderSSThreshold     RTT       gput
2406           0 days 00:00:00                 0                10      open         2147483647  43.643        NaN
2407 0 days 00:00:00.005177414                 0                10      open         2147483647  43.643   0.000000
2408 0 days 00:00:00.010327897                 0                10      open         2147483647  43.643   0.000000
2409 0 days 00:00:00.015473784                 0                10      open         2147483647  43.643   0.000000
2410 0 days 00:00:00.020615297                 0                10      open         2147483647  43.643   0.000000
2411 0 days 00:00:00.025763294                 0                10      open         2147483647  43.643   0.000000
2412 0 days 00:00:00.030907060                 0                10      open         2147483647  43.643   0.000000
2413 0 days 00:00:00.036053857                 0                10      open         2147483647  43.643   0.000000
2414 0 days 00:00:00.040200899                 0                10      open         2147483647  43.643   0.000000
2415 0 days 00:00:00.045194451                 0                16      open         2147483647  42.876   1.537888
2416 0 days 00:00:00.050338513                 0                20      open         2147483647  44.119   2.301220
2417 0 days 00:00:00.055480573                 0                20      open         2147483647  44.119   2.087938
2418 0 days 00:00:00.060624124                 0                20      open         2147483647  44.119   1.910790
2419 0 days 00:00:00.065767176                 0                20      open         2147483647  44.119   1.761365
2420 0 days 00:00:00.070908868                 0                20      open         2147483647  44.119   1.633646
2421 0 days 00:00:00.076051004                 0                20      open         2147483647  44.119   1.523188
2422 0 days 00:00:00.080192906                 0                20      open         2147483647  44.119   1.444517
2423 0 days 00:00:00.085468547                 0                32      open         2147483647  41.552   2.981775
2424 0 days 00:00:00.090628985                 0                40      open         2147483647  40.889   3.834535
2425 0 days 00:00:00.095770244                 0                40      open         2147483647  40.889   3.628685
2426 0 days 00:00:00.100915154                 0                40      open         2147483647  40.889   3.443685
2427 0 days 00:00:00.106060905                 0                40      open         2147483647  40.889   3.444766
2428 0 days 00:00:00.110205423                 0                40      open         2147483647  40.889   3.308832
2429 0 days 00:00:00.115349345                 0                40      open         2147483647  40.889   3.309038
2430 0 days 00:00:00.120510998                 0                50      open         2147483647  38.463   4.411389
2431 0 days 00:00:00.125680202                 0                68      open         2147483647  40.476   6.394828
2432 0 days 00:00:00.130821908                 0                68      open         2147483647  40.476   6.395211
2433 0 days 00:00:00.135926458                 0                80      open         2147483647  42.302   7.721240
2434 0 days 00:00:00.141069519                 0                85      open         2147483647  36.245   8.613184
2435 0 days 00:00:00.145212609                 0                85      open         2147483647  36.245   7.991509
2436 0 days 00:00:00.150354719                 0                88      open         2147483647  35.697   7.875844
2437 0 days 00:00:00.155498632                 0                88      open         2147483647  35.697   7.875698
2438 0 days 00:00:00.160334495                 0               104      open         2147483647  36.327   9.280111
2439 0 days 00:00:00.165480679                 0               125      open         2147483647  38.548  11.599847
2440 0 days 00:00:00.170624678                 0               125      open         2147483647  38.548  11.599742
2441 0 days 00:00:00.175807792                 0               145      open         2147483647  40.709  13.803764
2442 0 days 00:00:00.180979059                 0               152      open         2147483647  41.700  15.171608
2443 0 days 00:00:00.185056940                 0               170      open         2147483647  45.051  16.570028
2444 0 days 00:00:00.190199760                 0               170      open         2147483647  45.051  15.263759
2445 0 days 00:00:00.195341013                 0               183      open                183  41.215  16.372828
2446 0 days 00:00:00.200482515                 0               183      open                183  41.215  16.372790
2447 0 days 00:00:00.205624525                 0               184      open                183  42.753  18.475214
2448 0 days 00:00:00.210768057                 1               178  recovery                128  43.956  20.043389
2449 0 days 00:00:00.215921393                 1               178  recovery                128  43.956  20.041513
2450 0 days 00:00:00.221064831                 2               171  recovery                128  48.281  18.893124
2451 0 days 00:00:00.225228128                 3               164  recovery                128  50.643  18.141979
2452 0 days 00:00:00.230371189                 3               164  recovery                128  50.643  16.154820
2453 0 days 00:00:00.235514003                 3               161  recovery                128  54.441  16.154649
2454 0 days 00:00:00.240664586                 3               161  recovery                128  54.441  14.820353
2455 0 days 00:00:00.245813514                 3               157  recovery                128  59.795  14.854101
2456 0 days 00:00:00.250965062                 3               150  recovery                128  64.817  14.507296
2457 0 days 00:00:00.255085906                 3               147  recovery                128  65.841  13.936479
2458 0 days 00:00:00.260239121                 3               147  recovery                128  65.841  13.935241
2459 0 days 00:00:00.265391056                 4               143  recovery                128  69.148  12.129085
2460 0 days 00:00:00.270543065                 4               140  recovery                128  70.978   9.812989
2461 0 days 00:00:00.275692860                 4               140  recovery                128  70.978   9.812447
2462 0 days 00:00:00.280844047                 4               128  recovery                128  74.031   7.609715
2463 0 days 00:00:00.285994473                 4               127  recovery                128  78.218   5.049618
2464 0 days 00:00:00.290079777                 4               127  recovery                128  78.218   4.853192
2465 0 days 00:00:00.295229875                 5               112  recovery                128  82.000   7.168991
2466 0 days 00:00:00.300378803                 5               112  recovery                128  82.000   5.183354
2467 0 days 00:00:00.305380161                 5                94  recovery                128  79.078  14.576952
2468 0 days 00:00:00.310533136                 5                79  recovery                128  75.158  12.477450
2469 0 days 00:00:00.315683609                 5                79  recovery                128  75.158  11.703737
2470 0 days 00:00:00.320833492                 5               128      open                128  51.358  32.351960
2471 0 days 00:00:00.325983681                 5               128      open                128  37.362  35.181227
2472 0 days 00:00:00.330068787                 5               128      open                128  35.125  34.141869
2473 0 days 00:00:00.335217982                 5               128      open                128  35.125  34.139871
2474 0 days 00:00:00.340368138                 5               128      open                128  31.150  35.573685
...
```
