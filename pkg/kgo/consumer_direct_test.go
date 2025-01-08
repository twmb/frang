package kgo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Allow adding a topic to consume after the client is initialized with nothing
// to consume.
func TestIssue325(t *testing.T) {
	t.Parallel()

	topic, cleanup := tmpTopic(t)
	defer cleanup()

	cl, _ := newTestClient(
		DefaultProduceTopic(topic),
		UnknownTopicRetries(-1),
	)
	defer cl.Close()

	if err := cl.ProduceSync(context.Background(), StringRecord("foo")).FirstErr(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl.AddConsumeTopics(topic)
	recs := cl.PollFetches(ctx).Records()
	if len(recs) != 1 || string(recs[0].Value) != "foo" {
		t.Fatal(recs)
	}
}

// Allow adding a topic to consume after the client is initialized with nothing
// to consume.
func TestConsumeTopicRetrieval(t *testing.T) {
	t.Parallel()
	topicName := "test"
	cl, _ := newTestClient()
	defer cl.Close()
	topics := cl.GetConsumeTopics()
	if len(topics) != 0 {
		t.Fatalf("expected no topics, got %v", topics)
	}
	cl.AddConsumeTopics(topicName)
	topics = cl.GetConsumeTopics()
	if len(topics) != 1 || topics[0] != topicName {
		t.Fatalf("expected to see %v, got %v", topicName, topics)
	}
}

// Allow adding a topic to consume after the client is initialized with nothing
// to consume.
func TestConsumeTopicRetrieval_Many(t *testing.T) {
	t.Parallel()
	topicName := "test"
	cl, _ := newTestClient()
	defer cl.Close()
	topics := cl.GetConsumeTopics()
	if len(topics) != 0 {
		t.Fatalf("expected no topics, got %v", topics)
	}
	for i := 0; i < 100; i++ {
		cl.AddConsumeTopics(fmt.Sprintf("%s_%d", topicName, i))
	}
	topics = cl.GetConsumeTopics()
	sort.Slice(topics, func(i, j int) bool {
		return topics[i] < topics[j]
	})
	if len(topics) != 100 || topics[0] != fmt.Sprintf("%s_%d", topicName, 0) {
		t.Fatalf("expected to see %v, got %v", topicName, topics)
	}
}

// Ensure we only consume one partition if we only ask for one partition.
func TestIssue337(t *testing.T) {
	t.Parallel()

	topic, cleanup := tmpTopicPartitions(t, 2)
	defer cleanup()

	cl, _ := newTestClient(
		DefaultProduceTopic(topic),
		RecordPartitioner(ManualPartitioner()),
		UnknownTopicRetries(-1),
		ConsumePartitions(map[string]map[int32]Offset{
			topic: {0: NewOffset().At(0)},
		}),
	)
	defer cl.Close()

	if err := cl.ProduceSync(context.Background(),
		&Record{Partition: 0, Value: []byte("foo")},
		&Record{Partition: 1, Value: []byte("bar")},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var recs []*Record
out:
	for {
		fs := cl.PollFetches(ctx)
		switch err := fs.Err0(); err {
		default:
			t.Fatalf("unexpected error: %v", err)
		case context.DeadlineExceeded:
			break out
		case nil:
		}
		recs = append(recs, fs.Records()...)
	}
	if len(recs) != 1 {
		t.Fatalf("incorrect number of records, saw: %v", len(recs))
	}
	if string(recs[0].Value) != "foo" {
		t.Fatalf("wrong value, got: %s", recs[0].Value)
	}
}

func TestDirectPartitionPurge(t *testing.T) {
	t.Parallel()

	topic, cleanup := tmpTopicPartitions(t, 2)
	defer cleanup()

	cl, _ := newTestClient(
		DefaultProduceTopic(topic),
		RecordPartitioner(ManualPartitioner()),
		UnknownTopicRetries(-1),
		ConsumePartitions(map[string]map[int32]Offset{
			topic: {0: NewOffset().At(0)},
		}),
	)
	defer cl.Close()

	if err := cl.ProduceSync(context.Background(),
		&Record{Partition: 0, Value: []byte("foo")},
		&Record{Partition: 1, Value: []byte("bar")},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}
	cl.PurgeTopicsFromClient(topic)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	fs := cl.PollFetches(ctx)
	cancel()
	if err := fs.Err0(); err != context.DeadlineExceeded {
		t.Fatal("unexpected success when expecting context.DeadlineExceeded")
	}

	cl.AddConsumeTopics(topic)
	ctx, cancel = context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()

	exp := map[string]bool{
		"foo": true,
		"bar": true,
	}
	for {
		fs := cl.PollFetches(ctx)
		if err := fs.Err0(); err == context.DeadlineExceeded {
			break
		}
		fs.EachRecord(func(r *Record) {
			v := string(r.Value)
			if !exp[v] {
				t.Errorf("saw unexpected value %v", v)
			}
			delete(exp, v)
		})
	}
	if len(exp) > 0 {
		t.Errorf("did not see expected values %v", exp)
	}
}

// Ensure a deleted topic while regex consuming is no longer fetched.
func TestIssue434(t *testing.T) {
	t.Parallel()

	var (
		t1, cleanup1 = tmpTopicPartitions(t, 1)
		t2, cleanup2 = tmpTopicPartitions(t, 1)
	)
	defer cleanup1()
	defer cleanup2()

	cl, _ := newTestClient(
		UnknownTopicRetries(-1),
		ConsumeTopics(fmt.Sprintf("(%s|%s)", t1, t2)),
		ConsumeRegex(),
		FetchMaxWait(100*time.Millisecond),
		KeepRetryableFetchErrors(),
	)
	defer cl.Close()

	if err := cl.ProduceSync(context.Background(),
		&Record{Topic: t1, Value: []byte("t1")},
		&Record{Topic: t2, Value: []byte("t2")},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}
	cleanup2()

	// This test is a slight heuristic check test. We are keeping retryable
	// errors, so if the purge is successful, then we expect no response
	// and we expect the fetch to just contain context.DeadlineExceeded.
	//
	// We can get the topic in the response for a little bit if our fetch
	// is fast enough, so we ignore any errors (UNKNOWN_TOPIC_ID) at the
	// start. We want to ensure the topic is just outright missing from
	// the response because that will mean it is internally purged.
	start := time.Now()
	var missingTopic int
	for missingTopic < 2 {
		if time.Since(start) > 30*time.Second {
			t.Fatal("still seeing topic after 30s")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		fs := cl.PollFetches(ctx)
		cancel()
		var foundTopic bool
		fs.EachTopic(func(ft FetchTopic) {
			if ft.Topic == t2 {
				foundTopic = true
			}
		})
		if !foundTopic {
			missingTopic++
		}
	}
}

func TestAddRemovePartitions(t *testing.T) {
	t.Parallel()

	t1, cleanup := tmpTopicPartitions(t, 2)
	defer cleanup()

	cl, _ := newTestClient(
		UnknownTopicRetries(-1),
		RecordPartitioner(ManualPartitioner()),
		FetchMaxWait(100*time.Millisecond),
	)
	defer cl.Close()

	if err := cl.ProduceSync(context.Background(),
		&Record{Topic: t1, Partition: 0, Value: []byte("v1")},
		&Record{Topic: t1, Partition: 1, Value: []byte("v2")},
		&Record{Topic: t1, Partition: 1, Value: []byte("v3")},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}

	cl.AddConsumePartitions(map[string]map[int32]Offset{
		t1: {0: NewOffset().At(0)},
	})

	recs := cl.PollFetches(context.Background()).Records()
	if len(recs) != 1 || string(recs[0].Value) != "v1" {
		t.Fatalf("expected to see v1, got %v", recs)
	}

	cl.RemoveConsumePartitions(map[string][]int32{
		t1:   {0, 1, 2},
		"t2": {0, 1, 2},
	})

	cl.AddConsumePartitions(map[string]map[int32]Offset{
		t1: {
			0: NewOffset().At(0),
			1: NewOffset().At(1),
		},
	})

	recs = recs[:0]
	for len(recs) < 2 {
		recs = append(recs, cl.PollFetches(context.Background()).Records()...)
	}
	if len(recs) > 2 {
		t.Fatalf("expected to see 2 records, got %v", recs)
	}

	sort.Slice(recs, func(i, j int) bool {
		return recs[i].Partition < recs[j].Partition
	})

	if string(recs[0].Value) != "v1" || string(recs[1].Value) != "v3" {
		t.Fatalf("expected to see v1 and v2, got %v", recs)
	}
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestPauseIssue489(t *testing.T) {
	t.Parallel()

	t1, cleanup := tmpTopicPartitions(t, 3)
	defer cleanup()

	cl, _ := newTestClient(
		UnknownTopicRetries(-1),
		DefaultProduceTopic(t1),
		RecordPartitioner(ManualPartitioner()),
		ConsumeTopics(t1),
		FetchMaxWait(100*time.Millisecond),
	)
	defer cl.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		var exit atomic.Bool
		var which uint8
		for !exit.Load() {
			r := StringRecord("v")
			r.Partition = int32(which % 3)
			which++
			cl.Produce(ctx, r, func(_ *Record, err error) {
				if err == context.Canceled {
					exit.Store(true)
				}
			})
			cl.Flush(ctx)
			time.Sleep(50 * time.Microsecond)
		}
	}()
	defer cancel()

	for _, pollfn := range []struct {
		name string
		fn   func(context.Context) Fetches
	}{
		{"fetches", func(ctx context.Context) Fetches { return cl.PollFetches(ctx) }},
		{"records", func(ctx context.Context) Fetches { return cl.PollRecords(ctx, 1000) }},
	} {
		for i := 0; i < 10; i++ {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			var sawZero, sawOne, sawTwo bool
			for (!sawZero || !sawOne || !sawTwo) && !closed(ctx.Done()) {
				fs := pollfn.fn(ctx)
				fs.EachRecord(func(r *Record) {
					sawZero = sawZero || r.Partition == 0
					sawOne = sawOne || r.Partition == 1
					sawTwo = sawTwo || r.Partition == 2
				})
			}
			cl.PauseFetchPartitions(map[string][]int32{t1: {0}})
			sawZero, sawOne, sawTwo = false, false, false
			for i := 0; i < 10 && !closed(ctx.Done()); i++ {
				fs := pollfn.fn(ctx)
				fs.EachRecord(func(r *Record) {
					sawZero = sawZero || r.Partition == 0
					sawOne = sawOne || r.Partition == 1
					sawTwo = sawTwo || r.Partition == 2
				})
			}
			cancel()
			if sawZero {
				t.Fatalf("%s: saw partition zero even though it was paused", pollfn.name)
			}
			if !sawOne {
				t.Fatalf("%s: did not see partition one even though it was not paused", pollfn.name)
			}
			if !sawTwo {
				t.Fatalf("%s: did not see partition two even though it was not paused", pollfn.name)
			}
			cl.ResumeFetchPartitions(map[string][]int32{t1: {0}})
		}
	}
}

func TestPauseIssueOct2023(t *testing.T) {
	t1, cleanup1 := tmpTopicPartitions(t, 1)
	t2, cleanup2 := tmpTopicPartitions(t, 1)
	t3, cleanup3 := tmpTopicPartitions(t, 1)
	defer cleanup1()
	defer cleanup2()
	defer cleanup3()
	ts := []string{t1, t2, t3}

	cl, _ := newTestClient(
		UnknownTopicRetries(-1),
		ConsumeTopics(ts...),
		MetadataMinAge(50*time.Millisecond),
		FetchMaxWait(100*time.Millisecond),
	)
	defer cl.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		var exit atomic.Bool
		var which int
		for !exit.Load() {
			r := StringRecord("v")
			r.Topic = ts[which%len(ts)]
			which++
			cl.Produce(ctx, r, func(_ *Record, err error) {
				if err == context.Canceled {
					exit.Store(true)
				}
			})
			cl.Flush(ctx)
			time.Sleep(50 * time.Microsecond)
		}
	}()
	defer cancel()

	for _, pollfn := range []struct {
		name string
		fn   func(context.Context) Fetches
	}{
		{"fetches", func(ctx context.Context) Fetches { return cl.PollFetches(ctx) }},
		{"records", func(ctx context.Context) Fetches { return cl.PollRecords(ctx, 1000) }},
	} {
		for i := 0; i < 10; i++ {
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			var sawt1, sawt2, sawt3 bool
			for (!sawt1 || !sawt2 || !sawt3) && !closed(ctx.Done()) {
				fs := pollfn.fn(ctx)
				fs.EachRecord(func(r *Record) {
					sawt1 = sawt1 || r.Topic == t1
					sawt2 = sawt2 || r.Topic == t2
					sawt3 = sawt3 || r.Topic == t3
				})
			}
			cl.PauseFetchTopics(t1)
			sawt1, sawt2, sawt3 = false, false, false
			for i := 0; i < 10 && !closed(ctx.Done()); i++ {
				fs := pollfn.fn(ctx)
				fs.EachRecord(func(r *Record) {
					sawt1 = sawt1 || r.Topic == t1
					sawt2 = sawt2 || r.Topic == t2
					sawt3 = sawt3 || r.Topic == t3
				})
			}
			cancel()
			if sawt1 {
				t.Fatalf("%s: saw topic t1 even though it was paused", pollfn.name)
			}
			if !sawt2 {
				t.Fatalf("%s: did not see topic t2 even though it was not paused", pollfn.name)
			}
			if !sawt3 {
				t.Fatalf("%s: did not see topic t3 even though it was not paused", pollfn.name)
			}
			cl.ResumeFetchTopics(t1)
		}
	}
}

func TestIssue523(t *testing.T) {
	t.Parallel()

	t1, cleanup := tmpTopicPartitions(t, 1)
	defer cleanup()
	g1, gcleanup := tmpGroup(t)
	defer gcleanup()

	cl, _ := newTestClient(
		DefaultProduceTopic(t1),
		ConsumeTopics(".*"+t1+".*"),
		ConsumeRegex(),
		ConsumerGroup(g1),
		MetadataMinAge(100*time.Millisecond),
		FetchMaxWait(time.Second),
		KeepRetryableFetchErrors(),
		UnknownTopicRetries(-1),
	)
	defer cl.Close()

	if err := cl.ProduceSync(context.Background(), StringRecord("foo")).FirstErr(); err != nil {
		t.Fatal(err)
	}

	cl.PollFetches(context.Background())

	cleanup() // delete the topic

	start := time.Now()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		fs := cl.PollFetches(ctx)
		cancel()
		if errors.Is(fs.Err0(), context.DeadlineExceeded) {
			break
		}
		if time.Since(start) > 40*time.Second { // missing topic delete is 15s by default
			t.Fatalf("still repeatedly requesting metadata after 20s")
		}
		if fs.Err0() != nil {
			time.Sleep(time.Second)
		}
	}
}

func TestIssue648(t *testing.T) {
	t.Parallel()
	cl, _ := newTestClient(
		MetadataMinAge(100*time.Millisecond),
		ConsumeTopics("bizbazbuz"),
		FetchMaxWait(time.Second),
		KeepRetryableFetchErrors(),
	)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	fs := cl.PollFetches(ctx)
	cancel()

	var found bool
	fs.EachError(func(_ string, _ int32, err error) {
		if !errors.Is(err, kerr.UnknownTopicOrPartition) {
			t.Errorf("expected ErrUnknownTopicOrPartition, got %v", err)
		} else {
			found = true
		}
	})
	if !found {
		t.Errorf("did not see ErrUnknownTopicOrPartition")
	}
}

func TestIssue810(t *testing.T) {
	t.Parallel()

	t1, cleanup1 := tmpTopicPartitions(t, 1)
	defer cleanup1()

	_, cleanup2 := tmpTopicPartitions(t, 1)
	defer cleanup2()

	// Non-regex consuming: topics are available immediately.
	{
		cl, _ := newTestClient(
			ConsumeTopics(t1),
			UnknownTopicRetries(-1),
		)
		defer cl.Close()

		topics := cl.GetConsumeTopics()
		exp := []string{t1}

		if !reflect.DeepEqual(topics, exp) {
			t.Errorf("non-regex got %v != exp %v", topics, exp)
		}
	}

	// Regex consuming: topics are available only after discovery.
	{
		cl, _ := newTestClient(
			ConsumeTopics(t1),
			ConsumeRegex(),
			UnknownTopicRetries(-1),
			MetadataMaxAge(time.Second),
			MetadataMinAge(100*time.Millisecond),
		)
		defer cl.Close()

		var (
			ticker  = time.NewTicker(100 * time.Millisecond)
			fail    = time.NewTimer(15 * time.Second)
			failed  bool
			lastSaw []string
			exp     = []string{t1}
		)

		defer ticker.Stop()
		defer fail.Stop()

	out:
		for {
			select {
			case <-ticker.C:
				lastSaw = cl.GetConsumeTopics()
				if reflect.DeepEqual(lastSaw, exp) {
					break out
				}
				cl.ForceMetadataRefresh()
			case <-fail.C:
				failed = true
				break out
			}
		}

		if failed {
			t.Errorf("did not see expected topics in time, last saw %v != exp %v", lastSaw, exp)
		}
	}
}

func TestIssue865(t *testing.T) {
	t.Parallel()

	t1, cleanup1 := tmpTopicPartitions(t, 1)
	defer cleanup1()
	t2, cleanup2 := tmpTopicPartitions(t, 1)
	defer cleanup2()

	cl, _ := newTestClient(
		UnknownTopicRetries(-1),
		ConsumeTopics(t1, t2),
		FetchMaxWait(100*time.Millisecond),
	)
	defer cl.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		nrecs      = 10_000
		flushEvery = 1000
		pollAmount = 100
	)

	var wg sync.WaitGroup
	for i := 0; i < nrecs; i++ {
		r1 := StringRecord(strconv.Itoa(i))
		r1.Topic = t1
		wg.Add(1)
		cl.Produce(ctx, r1, func(_ *Record, err error) {
			defer wg.Done()
			if err != nil {
				t.Fatal(err)
			}
		})

		r2 := StringRecord(strconv.Itoa(i))
		r2.Topic = t2
		wg.Add(1)
		cl.Produce(ctx, r2, func(_ *Record, err error) {
			defer wg.Done()
			if err != nil {
				t.Fatal(err)
			}
		})

		if nrecs%flushEvery == 0 {
			cl.Flush(ctx)
		}
	}

	wg.Wait()

	for i := 2 * nrecs; i > 0; {
		ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		fs := cl.PollRecords(ctx, 100)
		cancel()
		cl.ResumeFetchTopics(t2)
		fs.EachRecord(func(r *Record) {
			i--
			if r.Topic == t2 {
				cl.PauseFetchTopics(t2)
			}
		})
	}

	nrecbuf, nbytebuf := cl.BufferedFetchRecords(), cl.BufferedFetchBytes()

	if nrecbuf != 0 {
		t.Errorf("got rec buffered %d != 0", nrecbuf)
	}
	if nbytebuf != 0 {
		t.Errorf("got byte buffered %d != 0", nbytebuf)
	}
}
