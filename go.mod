module github.com/hendrikcech/netscalpel

go 1.25.0

require (
	github.com/alecthomas/kong v1.12.1
	github.com/alistanis/cartesian v0.0.0-20220409094110-a224e60a7f74
	github.com/anacrolix/mmsg v1.1.1
	github.com/google/uuid v1.6.0
	github.com/klauspost/compress v1.18.0
	github.com/mikioh/tcp v0.0.0-20190314235350-803a9b46060c
	github.com/mikioh/tcpinfo v0.0.0-20190314235526-30a79bb1804b
	github.com/quic-go/quic-go v0.54.1
	github.com/samber/slog-channel v1.4.2
	github.com/samber/slog-multi v1.4.0
	go.uber.org/goleak v1.3.0
	golang.org/x/net v0.46.0
	golang.org/x/sync v0.17.0
	golang.org/x/sys v0.37.0
)

require (
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/mikioh/tcpopt v0.0.0-20190314235656-172688c1accc // indirect
	github.com/samber/lo v1.51.0 // indirect
	github.com/samber/slog-common v0.18.1 // indirect
	go.uber.org/mock v0.5.2 // indirect
	golang.org/x/crypto v0.43.0 // indirect
	golang.org/x/mod v0.28.0 // indirect
	golang.org/x/text v0.30.0 // indirect
	golang.org/x/tools v0.37.0 // indirect
)

replace github.com/anacrolix/mmsg => github.com/hendrikcech/mmsg v1.1.1-0.20250620101926-bfaf4885649a
