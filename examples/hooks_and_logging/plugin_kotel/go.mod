module kotel_hooks

go 1.19

require (
	github.com/twmb/franz-go v1.7.1
	//github.com/twmb/franz-go/plugin/kotel v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/otel v1.9.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.9.0
	go.opentelemetry.io/otel/sdk v1.9.0
)

require (
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.2.0 // indirect
	go.opentelemetry.io/otel/trace v1.9.0 // indirect
	golang.org/x/sys v0.0.0-20220829200755-d48e67d00261 // indirect
)

replace github.com/twmb/franz-go => ../../..
