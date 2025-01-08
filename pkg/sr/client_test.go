package sr

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSchemaRegistryAPI(t *testing.T) {
	dummySchema := SubjectSchema{
		Subject: "foo",
		Version: 1,
		ID:      1,
		Schema:  Schema{Schema: `{"name":"foo", "type": "record", "fields":[{"name":"str", "type": "string"}]}`},
	}
	dummySchemaWithRef := SubjectSchema{
		Subject: "bar",
		Version: 1,
		ID:      2,
		Schema: Schema{
			Schema:     `{"name":"bar",  "type": "record", "fields":[{"name":"data", "type": "foo"}]}}`,
			References: []SchemaReference{{Name: "foo", Subject: "foo", Version: 1}},
		},
	}
	dummyAuthToken := "foobar"
	ts := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != dummyAuthToken {
				http.Error(w, "missing Authorization header", http.StatusBadRequest)
				return
			}

			path := r.URL.EscapedPath()
			var output any
			switch path {
			case "/compatibility/subjects/foo/versions/1":
				output = CheckCompatibilityResult{Is: true}
			case "/config/foo":
				switch r.Method {
				case http.MethodDelete:
					output = SetCompatibility{Level: CompatFull}
				default:
					output = CompatibilityResult{Subject: dummySchema.Subject, Level: CompatFull}
				}
			case "/mode":
				output = map[string]string{"mode": "READWRITE"}
			case "/subjects":
				output = []string{dummySchema.Subject, dummySchemaWithRef.Subject}
			case "/subjects/foo/versions", "/subjects/bar/versions":
				switch r.Method {
				case http.MethodGet:
					output = []int{1}
				case http.MethodPost:
					if path == "/subjects/foo/versions" {
						output = dummySchema
					} else {
						output = dummySchemaWithRef
					}
				default:
					http.Error(w, fmt.Sprintf("method not supported: %s", r.Method), http.StatusBadRequest)
					return
				}
			case "/subjects/foo":
				switch r.Method {
				case http.MethodDelete:
					output = []int{1}
				default:
					output = dummySchema
				}
			case "/subjects/foo/versions/1":
				output = dummySchema
			case "/subjects/foo/versions/1/schema":
				output = dummySchema.Schema.Schema
			case "/subjects/foo/versions/1/referencedby":
				output = []int{2}
			case "/subjects/bar/versions/1":
				output = dummySchemaWithRef
			case "/schemas":
				output = []SubjectSchema{dummySchema, dummySchemaWithRef}
			case "/schemas/ids/1":
				output = dummySchema
			case "/schemas/ids/1/schema":
				output = dummySchema.Schema.Schema
			case "/schemas/ids/2":
				output = dummySchemaWithRef
			case "/schemas/ids/1/subjects":
				output = []string{dummySchema.Subject}
			case "/schemas/ids/2/subjects":
				output = []string{dummySchemaWithRef.Subject}
			case "/schemas/ids/1/versions":
				output = []map[string]any{{"subject": dummySchema.Subject, "version": dummySchema.Version}}
			case "/schemas/ids/2/versions":
				output = []map[string]any{{"subject": dummySchemaWithRef.Subject, "version": dummySchemaWithRef.Version}}
			case "/schemas/types":
				output = []string{"AVRO", "JSON"}
			default:
				http.Error(w, fmt.Sprintf("path not found: %s", path), http.StatusNotFound)
				return
			}

			b, err := json.Marshal(output)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if len(b) == 0 {
				http.NotFound(w, r)
				return
			}
			_, err = w.Write(b)

			if err != nil {
				http.Error(w, fmt.Sprintf("unable to write response: %s", err), http.StatusInternalServerError)
				return
			}
		}),
	)
	t.Cleanup(ts.Close)

	c, err := NewClient(URLs(ts.URL), PreReq(func(req *http.Request) error {
		req.Header.Set("Authorization", dummyAuthToken)
		return nil
	}))
	if err != nil {
		t.Fatalf("unable to create client: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	tests := []struct {
		name     string
		fn       func() (any, error)
		expected string
	}{
		{
			name:     "get supported types",
			fn:       func() (any, error) { return c.SupportedTypes(ctx) },
			expected: `["AVRO","JSON"]`,
		},
		{
			name:     "get schema by ID",
			fn:       func() (any, error) { return c.SchemaByID(ctx, dummySchema.ID) },
			expected: `{"schema":"{\"name\":\"foo\", \"type\": \"record\", \"fields\":[{\"name\":\"str\", \"type\": \"string\"}]}"}`,
		},
		{
			name:     "get schema text by ID",
			fn:       func() (any, error) { return c.SchemaTextByID(ctx, dummySchema.ID) },
			expected: `"\"{\\\"name\\\":\\\"foo\\\", \\\"type\\\": \\\"record\\\", \\\"fields\\\":[{\\\"name\\\":\\\"str\\\", \\\"type\\\": \\\"string\\\"}]}\""`,
		},
		{
			name:     "get subjects by ID",
			fn:       func() (any, error) { return c.SubjectsByID(ctx, dummySchema.ID) },
			expected: `["foo"]`,
		},
		{
			name:     "get schema versions by ID",
			fn:       func() (any, error) { return c.SchemaVersionsByID(ctx, dummySchema.ID) },
			expected: `[{"subject":"foo","version":1}]`,
		},
		{
			name:     "get subject versions",
			fn:       func() (any, error) { return c.SubjectVersions(ctx, dummySchema.Subject) },
			expected: `[1]`,
		},
		{
			name:     "get schema by version",
			fn:       func() (any, error) { return c.SchemaByVersion(ctx, dummySchema.Subject, dummySchema.Version) },
			expected: `{"subject":"foo","version":1,"id":1,"schema":"{\"name\":\"foo\", \"type\": \"record\", \"fields\":[{\"name\":\"str\", \"type\": \"string\"}]}"}`,
		},
		{
			name:     "get schema text by ID",
			fn:       func() (any, error) { return c.SchemaTextByVersion(ctx, dummySchema.Subject, dummySchema.Version) },
			expected: `"\"{\\\"name\\\":\\\"foo\\\", \\\"type\\\": \\\"record\\\", \\\"fields\\\":[{\\\"name\\\":\\\"str\\\", \\\"type\\\": \\\"string\\\"}]}\""`,
		},
		{
			name:     "get all schemas",
			fn:       func() (any, error) { return c.AllSchemas(ctx) },
			expected: `[{"subject":"foo","version":1,"id":1,"schema":"{\"name\":\"foo\", \"type\": \"record\", \"fields\":[{\"name\":\"str\", \"type\": \"string\"}]}"},{"subject":"bar","version":1,"id":2,"schema":"{\"name\":\"bar\",  \"type\": \"record\", \"fields\":[{\"name\":\"data\", \"type\": \"foo\"}]}}","references":[{"name":"foo","subject":"foo","version":1}]}]`,
		},
		{
			name:     "get all schemas for subject",
			fn:       func() (any, error) { return c.Schemas(ctx, dummySchema.Subject) },
			expected: `[{"subject":"foo","version":1,"id":1,"schema":"{\"name\":\"foo\", \"type\": \"record\", \"fields\":[{\"name\":\"str\", \"type\": \"string\"}]}"}]`,
		},
		{
			name:     "create schema",
			fn:       func() (any, error) { return c.CreateSchema(ctx, dummySchema.Subject, dummySchema.Schema) },
			expected: `{"subject":"foo","version":1,"id":1,"schema":"{\"name\":\"foo\", \"type\": \"record\", \"fields\":[{\"name\":\"str\", \"type\": \"string\"}]}"}`,
		},
		{
			name: "create schema with ID and version",
			fn: func() (any, error) {
				return c.CreateSchemaWithIDAndVersion(ctx, dummySchema.Subject, dummySchema.Schema, dummySchema.ID, dummySchema.Version)
			},
			expected: `{"subject":"foo","version":1,"id":1,"schema":"{\"name\":\"foo\", \"type\": \"record\", \"fields\":[{\"name\":\"str\", \"type\": \"string\"}]}"}`,
		},
		{
			name:     "lookup schema",
			fn:       func() (any, error) { return c.LookupSchema(ctx, dummySchema.Subject, dummySchema.Schema) },
			expected: `{"subject":"foo","version":1,"id":1,"schema":"{\"name\":\"foo\", \"type\": \"record\", \"fields\":[{\"name\":\"str\", \"type\": \"string\"}]}"}`,
		},
		{
			name:     "delete subject",
			fn:       func() (any, error) { return c.DeleteSubject(ctx, dummySchema.Subject, SoftDelete) },
			expected: `[1]`,
		},
		{
			name: "delete schema",
			fn: func() (any, error) {
				return c.DeleteSchema(ctx, dummySchema.Subject, dummySchema.Version, SoftDelete), nil
			},
			expected: `null`,
		},
		{
			name:     "get schema references",
			fn:       func() (any, error) { return c.SchemaReferences(ctx, dummySchema.Subject, dummySchema.Version) },
			expected: `[{"subject":"bar","version":1,"id":2,"schema":"{\"name\":\"bar\",  \"type\": \"record\", \"fields\":[{\"name\":\"data\", \"type\": \"foo\"}]}}","references":[{"name":"foo","subject":"foo","version":1}]}]`,
		},
		{
			name:     "get schema usages by ID",
			fn:       func() (any, error) { return c.SchemaUsagesByID(ctx, dummySchema.ID) },
			expected: `[{"subject":"foo","version":1,"id":1,"schema":"{\"name\":\"foo\", \"type\": \"record\", \"fields\":[{\"name\":\"str\", \"type\": \"string\"}]}"}]`,
		},
		{
			name:     "get subject compatibility",
			fn:       func() (any, error) { return c.Compatibility(ctx, dummySchema.Subject), nil },
			expected: `[{"compatibilityLevel":"FULL","alias":"","normalize":false,"compatibilityGroup":"","defaultMetadata":null,"overrideMetadata":null,"defaultRuleSet":null,"overrideRuleSet":null}]`,
		},
		{
			name: "set subject compatibility",
			fn: func() (any, error) {
				return c.SetCompatibility(ctx, SetCompatibility{Level: CompatForward}, dummySchema.Subject), nil
			},
			expected: `[{"compatibilityLevel":"FORWARD","alias":"","normalize":false,"compatibilityGroup":"","defaultMetadata":null,"overrideMetadata":null,"defaultRuleSet":null,"overrideRuleSet":null}]`,
		},
		{
			name:     "reset subject compatibility",
			fn:       func() (any, error) { return c.ResetCompatibility(ctx, dummySchema.Subject), nil },
			expected: `[{"compatibilityLevel":"FULL","alias":"","normalize":false,"compatibilityGroup":"","defaultMetadata":null,"overrideMetadata":null,"defaultRuleSet":null,"overrideRuleSet":null}]`,
		},
		{
			name: "check schema compatibility",
			fn: func() (any, error) {
				return c.CheckCompatibility(ctx, dummySchema.Subject, dummySchema.Version, dummySchema.Schema)
			},
			expected: `{"is_compatible":true,"messages":null}`,
		},
		{
			name:     "get mode",
			fn:       func() (any, error) { return c.Mode(ctx), nil },
			expected: `[{"Subject":"","Mode":"READWRITE","Err":null}]`,
		},
		{
			name:     "get mode",
			fn:       func() (any, error) { return c.SetMode(ctx, ModeReadOnly, dummySchema.Subject), nil },
			expected: `[{"Subject":"foo","Mode":"IMPORT","Err":{"error_code":0,"message":""}}]`,
		},
		{
			name:     "get mode",
			fn:       func() (any, error) { return c.ResetMode(ctx, dummySchema.Subject), nil },
			expected: `[{"Subject":"foo","Mode":"IMPORT","Err":{"error_code":0,"message":""}}]`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := test.fn()
			if err != nil {
				t.Errorf("failed to call test function: %s", err)
				return
			}
			b, err := json.Marshal(result)
			if err != nil {
				t.Errorf("unable to marshal input: %s", err)
				return
			}
			got := string(b)
			if got != test.expected {
				t.Errorf("expected %q, got %q", test.expected, got)
			}
		})
	}
}
