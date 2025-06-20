module gitlab.lrz.de/cm/starlink/netmeas

go 1.24.1

require (
	github.com/anacrolix/mmsg v1.1.1
	github.com/google/uuid v1.6.0
	github.com/klauspost/compress v1.18.0
	github.com/samber/slog-multi v1.4.0
	golang.org/x/sync v0.15.0
	golang.org/x/sys v0.33.0
)

require (
	github.com/samber/lo v1.51.0 // indirect
	github.com/samber/slog-channel v1.4.2 // indirect
	github.com/samber/slog-common v0.18.1 // indirect
	golang.org/x/text v0.26.0 // indirect
)

replace github.com/anacrolix/mmsg => github.com/hendrikcech/mmsg v1.1.1-0.20250620101926-bfaf4885649a
