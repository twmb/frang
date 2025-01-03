# kvictoria

kvictoria is a plug-in package to provide metrics for [VictoriaMetrics](https://victoriametrics.com/) using the [VictoriaMetrics/metrics](https://github.com/VictoriaMetrics/metrics) library through a
[`kgo.Hook`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Hook).

__Note__: this plug-in is intended to be used by users of the VictoriaMetrics database: due to the non-standard [implementation of histograms](https://pkg.go.dev/github.com/VictoriaMetrics/metrics#Histogram) in the `metrics` library using any other timeseries database will mean you get no usable histograms which would cripple your observability. If you need standard histograms you should use the [kprom](../kprom/README.md) plugin.

# Usage

To use this plug-in, do this:

```go
metrics := kvictoria.NewMetrics("namespace")
cl, err := kgo.NewClient(
	kgo.WithHooks(metrics),
	// ...other opts
)
```

The metrics will be automatically exposed when you use the [WritePrometheus](https://pkg.go.dev/github.com/VictoriaMetrics/metrics#WritePrometheus) function in your handler:
```go
http.HandleFunc("/metrics", func(w http.ResponseWriter, req *http.Request) {
    metrics.WritePrometheus(w, false)
})
```

# Details

This package provides the following metrics.

```
{namespace}_{subsystem}_connects_total{node_id="#{node}"}
{namespace}_{subsystem}_connect_errors_total{node_id="#{node}"}
{namespace}_{subsystem}_connect_seconds{node_id="#{node}"}
{namespace}_{subsystem}_disconnects_total{node_id="#{node}"}

{namespace}_{subsystem}_write_errors_total{node_id="#{node}"}
{namespace}_{subsystem}_write_bytes_total{node_id="#{node}"}
{namespace}_{subsystem}_write_wait_seconds{node_id="#{node}"}
{namespace}_{subsystem}_write_time_seconds{node_id="#{node}"}
{namespace}_{subsystem}_read_errors_total{node_id="#{node}"}
{namespace}_{subsystem}_read_bytes_total{node_id="#{node}"}
{namespace}_{subsystem}_read_wait_seconds{node_id="#{node}"}
{namespace}_{subsystem}_read_time_seconds{node_id="#{node}"}

{namespace}_{subsystem}_request_duration_e2e_seconds{node_id="#{node}"}
{namespace}_{subsystem}_request_throttled_seconds{node_id="#{node}"}

{namespace}_{subsystem}_group_manage_error{node_id="#{node}",error_message="#{error_message}"}

{namespace}_{subsystem}_produce_uncompressed_bytes_total{node_id="#{node}",topic="#{topic}",partition="#{partition}"}
{namespace}_{subsystem}_produce_compressed_bytes_total{node_id="#{node}",topic="#{topic}",partition="#{partition}"}
{namespace}_{subsystem}_produce_batches_total{node_id="#{node}",topic="#{topic}",partition="#{partition}"}
{namespace}_{subsystem}_produce_records_total{node_id="#{node}",topic="#{topic}",partition="#{partition}"}

{namespace}_{subsystem}_fetch_uncompressed_bytes_total{node_id="#{node}",topic="#{topic}",partition="#{partition}"}
{namespace}_{subsystem}_fetch_compressed_bytes_total{node_id="#{node}",topic="#{topic}",partition="#{partition}"}
{namespace}_{subsystem}_fetch_batches_total{node_id="#{node}",topic="#{topic}",partition="#{partition}"}
{namespace}_{subsystem}_fetch_records_total{node_id="#{node}",topic="#{topic}",partition="#{partition}"}

{namespace}_{subsystem}_buffered_produce_records_total{client_id="#{client_id}"}
{namespace}_{subsystem}_buffered_produce_bytes_total{client_id="#{client_id}"}
{namespace}_{subsystem}_buffered_fetch_records_total{client_id="#{client_id}"}
{namespace}_{subsystem}_buffered_fetch_bytes_total{client_id="#{client_id}"}
```

Some notes:
* the `subsystem` is optional, if you want to use it you can pass the option `kvictoria.Subsystem("mysubsystem")` when calling `NewMetrics`.
* metrics that are suffixed `_total` are either a counter or a gauge
* metrics that are suffixed `_seconds` are histograms
* the `group_manage_error` metric is a counter incremented any time there's an error that caused the client, operating as a group member, to break out of the group managing loop
