package kvictoria

type cfg struct {
	namespace string
	subsystem string
}

func newCfg(namespace string, opts ...Opt) cfg {
	cfg := cfg{
		namespace: namespace,
	}

	for _, opt := range opts {
		opt.apply(&cfg)
	}

	return cfg
}

// Opt is an option to configure Metrics.
type Opt interface {
	apply(*cfg)
}

type opt struct{ fn func(*cfg) }

func (o opt) apply(c *cfg) { o.fn(c) }

// Subsystem sets the subsystem for the metrics, overriding the default empty string.
func Subsystem(ss string) Opt {
	return opt{func(c *cfg) { c.subsystem = ss }}
}
