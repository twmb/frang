package kgo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var (
	adm             *Client
	testrf          = 3
	testRecordLimit = 500000

	// Kraft sometimes has massive hangs internally when completing
	// transactions. Against zk Kafka as well as Redpanda, we could rely on
	// our internal mitigations to never have KIP-447 problems. Not true
	// against Kraft, see #223.
	requireStableFetch = false

	// Redpanda is a bit more strict with transactions: we must wait for
	// EndTxn to return successfully before beginning a new transaction. We
	// cannot use EndAndBeginTransaction with EndBeginTxnUnsafe.
	allowUnsafe = false

	// We create topics with a different number of partitions to exercise
	// a few extra code paths; we index into npartitions with npartitionsAt,
	// an atomic that we modulo after load.
	npartitions   = []int{7, 11, 31}
	npartitionsAt int64
)

func init() {
	var err error
	adm, err = NewClient(getSeedBrokers())
	if err != nil {
		panic(fmt.Sprintf("unable to create admin client: %v", err))
	}

	if n, _ := strconv.Atoi(os.Getenv("KGO_TEST_RF")); n > 0 {
		testrf = n
	}
	if n, _ := strconv.Atoi(os.Getenv("KGO_TEST_RECORDS")); n > 0 {
		testRecordLimit = n
	}
	if _, exists := os.LookupEnv("KGO_TEST_STABLE_FETCH"); exists {
		requireStableFetch = true
	}
	if _, exists := os.LookupEnv("KGO_TEST_UNSAFE"); exists {
		allowUnsafe = true
	}
}

func getSeedBrokers() Opt {
	seeds := os.Getenv("KGO_SEEDS")
	if seeds == "" {
		seeds = "127.0.0.1:9092"
	}
	return SeedBrokers(strings.Split(seeds, ",")...)
}

var loggerNum atomicI64

var testLogLevel = func() LogLevel {
	level := strings.ToLower(os.Getenv("KGO_LOG_LEVEL"))
	switch level {
	case "debug":
		return LogLevelDebug
	case "none":
		return LogLevelNone
	}
	return LogLevelInfo
}()

func testLogger() Logger {
	num := loggerNum.Add(1)
	pfx := strconv.Itoa(int(num))
	return BasicLogger(os.Stderr, testLogLevel, func() string {
		return time.Now().Format("[15:04:05 ") + pfx + "]"
	})
}

var randsha = func() func() string {
	var mu sync.Mutex
	last := time.Now().UnixNano()
	return func() string {
		mu.Lock()
		defer mu.Unlock()

		now := time.Now().UnixNano()
		if now == last { // should never be the case
			now++
		}

		last = now

		sum := sha256.Sum256([]byte(strconv.Itoa(int(now))))
		return hex.EncodeToString(sum[:])
	}
}()

func tmpTopic(tb testing.TB) (string, func()) {
	partitions := npartitions[int(atomic.AddInt64(&npartitionsAt, 1))%len(npartitions)]
	topic := randsha()
	return tmpNamedTopicPartitions(tb, topic, partitions)
}

func tmpTopicPartitions(tb testing.TB, partitions int) (string, func()) {
	topic := randsha()
	return tmpNamedTopicPartitions(tb, topic, partitions)
}

func tmpNamedTopicPartitions(tb testing.TB, topic string, partitions int) (string, func()) {
	tb.Helper()

	req := kmsg.NewPtrCreateTopicsRequest()
	reqTopic := kmsg.NewCreateTopicsRequestTopic()
	reqTopic.Topic = topic
	reqTopic.NumPartitions = int32(partitions)
	reqTopic.ReplicationFactor = int16(testrf)
	req.Topics = append(req.Topics, reqTopic)

	start := time.Now()
issue:
	resp, err := req.RequestWith(context.Background(), adm)

	// If we run tests in a container _immediately_ after the container
	// starts, we can receive dial errors for a bit if the container is not
	// fully initialized. Handle this by retrying specifically dial errors.
	if ne := (*net.OpError)(nil); errors.As(err, &ne) && ne.Op == "dial" && time.Since(start) < 5*time.Second {
		time.Sleep(10 * time.Millisecond)
		goto issue
	}

	if err == nil {
		err = kerr.ErrorForCode(resp.Topics[0].ErrorCode)
		if errors.Is(err, kerr.TopicAlreadyExists) {
			time.Sleep(10 * time.Millisecond)
			goto issue
		}
	}
	if err != nil {
		tb.Fatalf("unable to create topic %q: %v", topic, err)
	}

	return topic, func() {
		tb.Helper()

		if tb.Failed() {
			tb.Logf("FAILED TESTING -- NOT DELETING TOPIC %s", topic)
			return
		}

		tb.Logf("deleting topic %s", topic)

		req := kmsg.NewPtrDeleteTopicsRequest()
		req.TopicNames = []string{topic}
		reqTopic := kmsg.NewDeleteTopicsRequestTopic()
		reqTopic.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, reqTopic)

		resp, err := req.RequestWith(context.Background(), adm)
		if err == nil {
			err = kerr.ErrorForCode(resp.Topics[0].ErrorCode)
		}
		if err != nil {
			tb.Fatalf("unable to delete topic %q: %v", topic, err)
		}
	}
}

func tmpGroup(tb testing.TB) (string, func()) {
	tb.Helper()

	group := randsha()

	return group, func() {
		tb.Helper()

		if tb.Failed() {
			tb.Logf("FAILED TESTING -- NOT DELETING GROUP %s", group)
			return
		}
		tb.Logf("deleting group %s", group)

		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{group}
		resp, err := req.RequestWith(context.Background(), adm)
		if err == nil {
			err = kerr.ErrorForCode(resp.Groups[0].ErrorCode)
		}
		if err != nil {
			tb.Logf("unable to delete group %q: %v", group, err)
		}
	}
}

// testConsumer performs ETL inside or outside transactions;
// the ETL functions are defined in group_test.go and txn_test.go
type testConsumer struct {
	errCh chan<- error

	consumeFrom string
	produceTo   string

	group    string
	balancer GroupBalancer

	expBody []byte // what every record body should be

	consumed atomicU64 // shared atomically

	wg sync.WaitGroup
	mu sync.Mutex

	logsOnce sync.Once

	// part2key tracks keys we have consumed from which partition
	part2key map[int32][]int

	// partOffsets tracks partition offsets we have consumed to ensure no
	// duplicates
	partOffsets map[partOffset]struct{}
}

type partOffset struct {
	part   int32
	offset int64
}

func newTestConsumer(
	errCh chan error,
	consumeFrom string,
	produceTo string,
	group string,
	balancer GroupBalancer,
	expBody []byte,
) *testConsumer {
	return &testConsumer{
		errCh: errCh,

		consumeFrom: consumeFrom,
		produceTo:   produceTo,

		group:    group,
		balancer: balancer,

		expBody: expBody,

		part2key:    make(map[int32][]int),
		partOffsets: make(map[partOffset]struct{}),
	}
}

func (c *testConsumer) wait() {
	c.wg.Wait()
}

func (c *testConsumer) goRun(transactional bool, etlsBeforeQuit int) {
	if transactional {
		c.goTransact(etlsBeforeQuit)
	} else {
		c.goGroupETL(etlsBeforeQuit)
	}
}

func testChainETL(
	t *testing.T,
	topic1 string,
	body []byte,
	errs chan error,
	transactional bool,
	balancer GroupBalancer,
) {
	var (
		/////////////
		// LEVEL 1 //
		/////////////

		group1, groupCleanup1 = tmpGroup(t)
		topic2, topic2Cleanup = tmpTopic(t)

		consumers1 = newTestConsumer(
			errs,
			topic1,
			topic2,
			group1,
			balancer,
			body,
		)

		/////////////
		// LEVEL 2 //
		/////////////

		group2, groupCleanup2 = tmpGroup(t)
		topic3, topic3Cleanup = tmpTopic(t)

		consumers2 = newTestConsumer(
			errs,
			topic2,
			topic3,
			group2,
			balancer,
			body,
		)

		/////////////
		// LEVEL 3 //
		/////////////

		group3, groupCleanup3 = tmpGroup(t)
		topic4, topic4Cleanup = tmpTopic(t)

		consumers3 = newTestConsumer(
			errs,
			topic3,
			topic4,
			group3,
			balancer,
			body,
		)
	)

	defer func() {
		groupCleanup1()
		groupCleanup2()
		groupCleanup3()
		topic2Cleanup()
		topic3Cleanup()
		topic4Cleanup()
	}()

	////////////////////
	// CONSUMER START //
	////////////////////

	for i := 0; i < 3; i++ { // three consumers start with standard poll&commit behavior
		consumers1.goRun(transactional, -1)
		consumers2.goRun(transactional, -1)
		consumers3.goRun(transactional, -1)
	}

	consumers1.goRun(transactional, 0) // bail immediately
	consumers1.goRun(transactional, 2) // bail after two txns
	consumers2.goRun(transactional, 2) // same

	time.Sleep(5 * time.Second)
	for i := 0; i < 3; i++ { // trigger rebalance after 5s with more consumers
		consumers1.goRun(transactional, -1)
		consumers2.goRun(transactional, -1)
		consumers3.goRun(transactional, -1)
	}

	consumers2.goRun(transactional, 0) // bail immediately
	consumers1.goRun(transactional, 1) // bail after one txn

	doneConsume := make(chan struct{})
	go func() {
		consumers1.wait()
		consumers2.wait()
		consumers3.wait()
		close(doneConsume)
	}()

	//////////////////////
	// FINAL VALIDATION //
	//////////////////////

out:
	for {
		select {
		case err := <-errs:
			t.Fatal(err)
		case <-doneConsume:
			break out
		}
	}

	for level, part2key := range []map[int32][]int{
		consumers1.part2key,
		consumers2.part2key,
		consumers3.part2key,
	} {
		allKeys := make([]int, 0, testRecordLimit) // did we receive everything we sent?
		for _, keys := range part2key {
			allKeys = append(allKeys, keys...)
		}

		if len(allKeys) != testRecordLimit {
			t.Errorf("consumers %d: got %d keys != exp %d", level, len(allKeys), testRecordLimit)
		}

		sort.Ints(allKeys)
		for i := 0; i < testRecordLimit; i++ {
			if allKeys[i] != i {
				t.Fatalf("consumers %d: got key %d != exp %d, first 100: %v", level, allKeys[i], i, allKeys[:100])
			}
		}
	}
}
