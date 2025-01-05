package kvictoria

import "testing"

func TestMetricsBuildName(t *testing.T) {
	testCases := []struct {
		namespace string
		subsystem string
		name      string
		labels    map[string]string
		exp       string
	}{
		// No labels
		{
			namespace: "foo",
			subsystem: "bar",
			name:      "records_total",
			labels:    nil,
			exp:       "foo_bar_records_total",
		},
		{
			namespace: "foo",
			subsystem: "",
			name:      "records_total",
			labels:    nil,
			exp:       "foo_records_total",
		},
		{
			namespace: "",
			subsystem: "bar",
			name:      "records_total",
			labels:    nil,
			exp:       "bar_records_total",
		},
		{
			namespace: "",
			subsystem: "",
			name:      "records_total",
			labels:    nil,
			exp:       "records_total",
		},
		// With labels
		{
			namespace: "foo",
			subsystem: "bar",
			name:      "records_total",
			labels:    map[string]string{"client_id": "kgo", "node_id": "1"},
			exp:       `foo_bar_records_total{client_id="kgo",node_id="1"}`,
		},
		{
			namespace: "foo",
			subsystem: "",
			name:      "records_total",
			labels:    map[string]string{"client_id": "kgo", "node_id": "1"},
			exp:       `foo_records_total{client_id="kgo",node_id="1"}`,
		},
		{
			namespace: "",
			subsystem: "bar",
			name:      "records_total",
			labels:    map[string]string{"client_id": "kgo", "node_id": "1"},
			exp:       `bar_records_total{client_id="kgo",node_id="1"}`,
		},
		{
			namespace: "",
			subsystem: "",
			name:      "records_total",
			labels:    map[string]string{"client_id": "kgo", "node_id": "1"},
			exp:       `records_total{client_id="kgo",node_id="1"}`,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			m := &Metrics{
				cfg: cfg{
					namespace: tc.namespace,
					subsystem: tc.subsystem,
				},
			}

			name := m.buildName(tc.name, tc.labels)
			if tc.exp != name {
				t.Errorf("expected metric name %v, got %v", tc.exp, name)
			}
		})
	}
}
