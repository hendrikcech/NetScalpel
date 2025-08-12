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

## UDP Output
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

## TCP Output
Information about TCP connections is sampled every 5 ms.
See `generateTCPResultRows()` in `./pkg/client.go` for more information about each field.

``` csv
Time,State,SenderMSS,ReceiverMSS,RTT,RTTVar,RTO,ATO,LastDataSent,LastDataReceived,LastAckReceived,ReceiverWindow,SenderSSThreshold,ReceiverSSThreshold,SenderWindowBytes,SenderWindowSegs,PathMTU,CAState,Retransmissions,Backoffs,WindowOrKeepAliveProbes,UnackedSegs,SackedSegs,LostSegs,RetransSegs,ForwardAckSegs,ReorderedSegs,ReceiverRTT,TotalRetransSegs,PacingRate,ThruBytesAcked,ThruBytesReceived,SegsOut,SegsIn,NotSentBytes,MinRTT,DataSegsOut,DataSegsIn,BBRMaxBW,BBRMinRTT,BBRPacingGain,BBRCongWindowGain
2025-08-12T23:17:19.483024706+02:00,established,32741,536,0.011,0.005,203.333,0,0,0,0,65495,2147483647,65495,0,10,65535,open,0,0,0,0,0,0,0,0,3,0,0,59529090909,1,0,2,1,0,0.011,0,0,,,,
2025-08-12T23:17:19.488196842+02:00,established,32741,536,0.011,0.005,203.333,0,4,4,4,65495,2147483647,65495,0,10,65535,open,0,0,0,0,0,0,0,0,3,0,0,59529090909,1,0,2,1,0,0.011,0,0,,,,
2025-08-12T23:17:19.493351371+02:00,established,32741,536,0.011,0.005,203.333,0,10,10,10,65495,2147483647,65495,0,10,65535,open,0,0,0,0,0,0,0,0,3,0,0,59529090909,1,0,2,1,0,0.011,0,0,,,,
2025-08-12T23:17:19.498534743+02:00,established,32741,536,0.011,0.005,203.333,0,14,14,14,65495,2147483647,65495,0,10,65535,open,0,0,0,0,0,0,0,0,3,0,0,59529090909,1,0,2,1,0,0.011,0,0,,,,
2025-08-12T23:17:19.503684467+02:00,established,32741,536,0.011,0.005,203.333,0,20,20,20,65495,2147483647,65495,0,10,65535,open,0,0,0,0,0,0,0,0,3,0,0,59529090909,1,0,2,1,0,0.011,0,0,,,,
```

`
