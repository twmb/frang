Plugins
===

This directory contains plug-in support for external libraries to use with a
`kgo.Client`. The metrics here try to take a pragmatic approach to which
metrics are valuable while avoiding too much cardinality explosion, and the
logging packages try to do the right thing. If anything could be improved,
please raise an issue or open a PR, thanks!

Each of these plugins has a corresponding example in [examples](../examples/hooks_and_logging)!

<pre>
<a href="./">plugin</a> — you are here
├── <a href="./kgmetrics">kgmetrics</a> — plug-in go-metrics to use with `kgo.WithHooks`
├── <a href="./kprom">kprom</a> — plug-in prometheus metrics to use with `kgo.WithHooks`
├── <a href="./kvictoria">kvictoria</a> — plug-in victoria metrics to use with `kgo.WithHooks`
├── <a href="./klogrus">klogrus</a> — plug-in sirupsen/logrus to use with `kgo.WithLogger`
├── <a href="./kzap">kzap</a> — plug-in uber-go/zap to use with `kgo.WithLogger`
└── <a href="./kzerolog">kzerolog</a> — plug-in rs/zerolog to use with `kgo.WithLogger`
</pre>
