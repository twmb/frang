package kgo

import (
	"context"
	"errors"
	"reflect"
	"time"
	"unsafe"
)

// RecordHeader contains extra information that can be sent with Records.
type RecordHeader struct {
	Key   string
	Value []byte
}

// RecordAttrs contains additional meta information about a record, such as its
// compression or timestamp type.
type RecordAttrs struct {
	// 6 bits are used right now for record batches, and we use the high
	// bit to signify no timestamp due to v0 message set.
	//
	// bits 1 thru 3:
	//   000 no compression
	//   001 gzip
	//   010 snappy
	//   011 lz4
	//   100 zstd
	// bit 4: timestamp type
	// bit 5: is transactional
	// bit 6: is control
	// bit 8: no timestamp type
	attrs uint8
}

// TimestampType specifies how Timestamp was determined.
//
// The default, 0, means that the timestamp was determined in a client
// when the record was produced.
//
// An alternative is 1, which is when the Timestamp is set in Kafka.
//
// Records pre 0.10.0 did not have timestamps and have value -1.
func (a RecordAttrs) TimestampType() int8 {
	if a.attrs&0b1000_0000 != 0 {
		return -1
	}
	return int8(a.attrs & 0b0000_1000)
}

// CompressionType signifies with which algorithm this record was compressed.
//
// 0 is no compression, 1 is gzip, 2 is snappy, 3 is lz4, and 4 is zstd.
func (a RecordAttrs) CompressionType() uint8 {
	return a.attrs & 0b0000_0111
}

// IsTransactional returns whether a record is a part of a transaction.
func (a RecordAttrs) IsTransactional() bool {
	return a.attrs&0b0001_0000 != 0
}

// IsControl returns whether a record is a "control" record (ABORT or COMMIT).
// These are generally not visible unless explicitly opted into.
func (a RecordAttrs) IsControl() bool {
	return a.attrs&0b0010_0000 != 0
}

// Record is a record to write to Kafka.
type Record struct {
	// ctx is an optional field that can be used to pass contextual information
	// to a record.
	//
	// This is useful when you want to propagate span data from tracing libraries.
	ctx context.Context

	// Key is an optional field that can be used for partition assignment.
	//
	// This is generally used with a hash partitioner to cause all records
	// with the same key to go to the same partition.
	Key []byte
	// Value is blob of data to write to Kafka.
	Value []byte

	// Headers are optional key/value pairs that are passed along with
	// records.
	//
	// These are purely for producers and consumers; Kafka does not look at
	// this field and only writes it to disk.
	Headers []RecordHeader

	// NOTE: if logAppendTime, timestamp is MaxTimestamp, not first + delta
	// zendesk/ruby-kafka#706

	// Timestamp is the timestamp that will be used for this record.
	//
	// Record batches are always written with "CreateTime", meaning that
	// timestamps are generated by clients rather than brokers.
	//
	// When producing, if this field is not yet set, it is set to time.Now.
	Timestamp time.Time

	// Topic is the topic that a record is written to.
	//
	// This must be set for producing.
	Topic string

	// Partition is the partition that a record is written to.
	//
	// For producing, this is left unset. This will be set by the client as
	// appropriate. Alternatively, you can use the ManualPartitioner, which
	// makes it such that this field is always the field chosen when
	// partitioning (i.e., you partition manually ahead of time).
	Partition int32

	// Attrs specifies what attributes were on this record.
	Attrs RecordAttrs

	// ProducerEpoch is the producer epoch of this message if it was
	// produced with a producer ID. An epoch and ID of 0 means it was not.
	//
	// For producing, this is left unset. This will be set by the client
	// as appropriate.
	ProducerEpoch int16

	// ProducerEpoch is the producer ID of this message if it was produced
	// with a producer ID. An epoch and ID of 0 means it was not.
	//
	// For producing, this is left unset. This will be set by the client
	// as appropriate.
	ProducerID int64

	// LeaderEpoch is the leader epoch of the broker at the time this
	// record was written, or -1 if on message sets.
	//
	// For committing records, it is not recommended to modify the
	// LeaderEpoch. Clients use the LeaderEpoch for data loss detection.
	LeaderEpoch int32

	// Offset is the offset that a record is written as.
	//
	// For producing, this is left unset. This will be set by the client as
	// appropriate. If you are producing with no acks, this will just be
	// the offset used in the produce request and does not mirror the
	// offset actually stored within Kafka.
	Offset int64
}

// WithContext enriches the Record with a Context.
func (r *Record) WithContext(ctx context.Context) *Record {
	if ctx == nil {
		panic("nil context")
	}
	r2 := new(Record)
	*r2 = *r
	r2.ctx = ctx
	return r2
}

// SetContext enriches the Record with a Context.
func (r *Record) SetContext(ctx context.Context) {
	if ctx == nil {
		panic("nil context")
	}
	r.ctx = ctx
}

// Context returns the Records context.
func (r *Record) Context() context.Context {
	if r.ctx != nil {
		return r.ctx
	}
	return context.Background()
}

// When buffering records, we calculate the length and tsDelta ahead of time
// (also because number width affects encoding length). We repurpose the Offset
// field to save space.
func (r *Record) setLengthAndTimestampDelta(length, tsDelta int32) {
	r.Offset = int64(uint64(uint32(length))<<32 | uint64(uint32(tsDelta)))
}

func (r *Record) lengthAndTimestampDelta() (length, tsDelta int32) {
	return int32(uint32(uint64(r.Offset) >> 32)), int32(uint32(uint64(r.Offset)))
}

// AppendFormat appends a record to b given the layout or returns an error if
// the layout is invalid. This is a one-off shortcut for using
// NewRecordFormatter. See that function's documentation for the layout
// specification.
func (r *Record) AppendFormat(b []byte, layout string) ([]byte, error) {
	f, err := NewRecordFormatter(layout)
	if err != nil {
		return b, err
	}
	return f.AppendRecord(b, r), nil
}

// StringRecord returns a Record with the Value field set to the input value
// string. For producing, this function is useful in tandem with the
// client-level DefailtProduceTopic option.
//
// This function uses the 'unsafe' package to avoid copying value into a slice.
//
// NOTE: It is NOT SAFE to modify the record's value. This function should only
// be used if you only ever read record fields. This function can safely be used
// for producing; the client never modifies a record's key nor value fields.
func StringRecord(value string) *Record {
	var slice []byte
	slicehdr := (*reflect.SliceHeader)(unsafe.Pointer(&slice))
	slicehdr.Data = ((*reflect.StringHeader)(unsafe.Pointer(&value))).Data
	slicehdr.Len = len(value)
	slicehdr.Cap = len(value)

	return &Record{Value: slice}
}

// KeyStringRecord returns a Record with the Key and Value fields set to the
// input key and value strings. For producing, this function is useful in
// tandem with the client-level DefailtProduceTopic option.
//
// This function uses the 'unsafe' package to avoid copying value into a slice.
//
// NOTE: It is NOT SAFE to modify the record's value. This function should only
// be used if you only ever read record fields. This function can safely be used
// for producing; the client never modifies a record's key nor value fields.
func KeyStringRecord(key, value string) *Record {
	r := StringRecord(value)

	keyhdr := (*reflect.SliceHeader)(unsafe.Pointer(&r.Key))
	keyhdr.Data = ((*reflect.StringHeader)(unsafe.Pointer(&key))).Data
	keyhdr.Len = len(key)
	keyhdr.Cap = len(key)

	return r
}

// SliceRecord returns a Record with the Value field set to the input value
// slice. For producing, this function is useful in tandem with the
// client-level DefailtProduceTopic option.
func SliceRecord(value []byte) *Record {
	return &Record{Value: value}
}

// KeySliceRecord returns a Record with the Key and Value fields set to the
// input key and value slices. For producing, this function is useful in
// tandem with the client-level DefailtProduceTopic option.
func KeySliceRecord(key, value []byte) *Record {
	return &Record{Key: key, Value: value}
}

// FetchPartition is a response for a partition in a fetched topic from a
// broker.
type FetchPartition struct {
	// Partition is the partition this is for.
	Partition int32
	// Err is an error for this partition in the fetch.
	//
	// Note that if this is a fatal error, such as data loss or non
	// retriable errors, this partition will never be fetched again.
	Err error
	// HighWatermark is the current high watermark for this partition, that
	// is, the current offset that is on all in sync replicas.
	HighWatermark int64
	// LastStableOffset is the offset at which all prior offsets have been
	// "decided". Non transactional records are always decided immediately,
	// but transactional records are only decided once they are committed
	// or aborted.
	//
	// The LastStableOffset will always be at or under the HighWatermark.
	LastStableOffset int64
	// LogStartOffset is the low watermark of this partition, otherwise
	// known as the earliest offset in the partition.
	LogStartOffset int64
	// Records contains feched records for this partition.
	Records []*Record
}

// EachRecord calls fn for each record in the partition.
func (p *FetchPartition) EachRecord(fn func(*Record)) {
	for _, r := range p.Records {
		fn(r)
	}
}

// FetchTopic is a response for a fetched topic from a broker.
type FetchTopic struct {
	// Topic is the topic this is for.
	Topic string
	// Partitions contains individual partitions in the topic that were
	// fetched.
	Partitions []FetchPartition
}

// EachPartition calls fn for each partition in Fetches.
func (t *FetchTopic) EachPartition(fn func(FetchPartition)) {
	for i := range t.Partitions {
		fn(t.Partitions[i])
	}
}

// EachRecord calls fn for each record in the topic, in any partition order.
func (t *FetchTopic) EachRecord(fn func(*Record)) {
	for i := range t.Partitions {
		for _, r := range t.Partitions[i].Records {
			fn(r)
		}
	}
}

// Records returns all records in all partitions in this topic.
//
// This is a convenience function that does a single slice allocation. If you
// can process records individually, it is far more efficient to use the Each
// functions.
func (t *FetchTopic) Records() []*Record {
	var n int
	t.EachPartition(func(p FetchPartition) {
		n += len(p.Records)
	})
	rs := make([]*Record, 0, n)
	t.EachPartition(func(p FetchPartition) {
		rs = append(rs, p.Records...)
	})
	return rs
}

// Fetch is an individual response from a broker.
type Fetch struct {
	// Topics are all topics being responded to from a fetch to a broker.
	Topics []FetchTopic
}

// Fetches is a group of fetches from brokers.
type Fetches []Fetch

// FetchError is an error in a fetch along with the topic and partition that
// the error was on.
type FetchError struct {
	Topic     string
	Partition int32
	Err       error
}

// Errors returns all errors in a fetch with the topic and partition that
// errored.
//
// There are four classes of errors possible:
//
//  1. a normal kerr.Error; these are usually the non-retriable kerr.Errors,
//     but theoretically a non-retriable error can be fixed at runtime (auth
//     error? fix auth). It is worth restarting the client for these errors if
//     you do not intend to fix this problem at runtime.
//
//  2. an injected *ErrDataLoss; these are informational, the client
//     automatically resets consuming to where it should and resumes. This
//     error is worth logging and investigating, but not worth restarting the
//     client for.
//
//  3. an untyped batch parse failure; these are usually unrecoverable by
//     restarts, and it may be best to just let the client continue. However,
//     restarting is an option, but you may need to manually repair your
//     partition.
//
//  4. an injected ErrClientClosed; this is a fatal informational error that
//     is returned from every Poll call if the client has been closed.
//     A corresponding helper function IsClientClosed can be used to detect
//     this error.
func (fs Fetches) Errors() []FetchError {
	var errs []FetchError
	fs.EachError(func(t string, p int32, err error) {
		errs = append(errs, FetchError{t, p, err})
	})
	return errs
}

// When we fetch, it is possible for Kafka to reply with topics / partitions
// that have no records and no errors. This will definitely happen outside of
// fetch sessions, but may also happen at other times (for some reason).
// When that happens we want to ignore the fetch.
func (f Fetch) hasErrorsOrRecords() bool {
	for i := range f.Topics {
		t := &f.Topics[i]
		for j := range t.Partitions {
			p := &t.Partitions[j]
			if p.Err != nil || len(p.Records) > 0 {
				return true
			}
		}
	}
	return false
}

// IsClientClosed returns whether the fetches include an error indicating that
// the client is closed.
//
// This function is useful to break out of a poll loop; you likely want to call
// this function before calling Errors. If you may cancel the context to poll,
// you may want to use Err0 and manually check errors.Is(ErrClientClosed) or
// errors.Is(context.Canceled).
func (fs Fetches) IsClientClosed() bool {
	// An injected ErrClientClosed is a single fetch with one topic and
	// one partition. We can use this to make IsClientClosed do less work.
	return len(fs) == 1 && len(fs[0].Topics) == 1 && len(fs[0].Topics[0].Partitions) == 1 && errors.Is(fs[0].Topics[0].Partitions[0].Err, ErrClientClosed)
}

// Err0 returns the error at the 0th index fetch, topic, and partition. This
// can be used to quickly check if polling returned early because the client
// was closed or the context was canceled and is faster than performing a
// linear scan over all partitions with Err. When the client is closed or the
// context is canceled, fetches will contain only one partition whose Err field
// indicates the close / cancel. Note that this returns whatever the first
// error is, nil or non-nil, and does not check for a specific error value.
func (fs Fetches) Err0() error {
	if len(fs) > 0 && len(fs[0].Topics) > 0 && len(fs[0].Topics[0].Partitions) > 0 {
		return fs[0].Topics[0].Partitions[0].Err
	}
	return nil
}

// Err returns the first error in all fetches, if any. This can be used to
// quickly check if the client is closed or your poll context was canceled, or
// to check if there's some other error that requires deeper investigation with
// EachError. This function performs a linear scan over all fetched partitions.
// It is recommended to always check all errors. If you would like to more
// quickly check ahead of time if a poll was canceled because of closing the
// client or canceling the context, you can use Err0.
func (fs Fetches) Err() error {
	for _, f := range fs {
		for i := range f.Topics {
			ft := &f.Topics[i]
			for j := range ft.Partitions {
				fp := &ft.Partitions[j]
				if fp.Err != nil {
					return fp.Err
				}
			}
		}
	}
	return nil
}

// EachError calls fn for every partition that had a fetch error with the
// topic, partition, and error.
//
// This function has the same semantics as the Errors function; refer to the
// documentation on that function for what types of errors are possible.
func (fs Fetches) EachError(fn func(string, int32, error)) {
	for _, f := range fs {
		for i := range f.Topics {
			ft := &f.Topics[i]
			for j := range ft.Partitions {
				fp := &ft.Partitions[j]
				if fp.Err != nil {
					fn(ft.Topic, fp.Partition, fp.Err)
				}
			}
		}
	}
}

// RecordIter returns an iterator over all records in a fetch.
//
// Note that errors should be inspected as well.
func (fs Fetches) RecordIter() *FetchesRecordIter {
	iter := &FetchesRecordIter{fetches: fs}
	iter.prepareNext()
	return iter
}

// FetchesRecordIter iterates over records in a fetch.
type FetchesRecordIter struct {
	fetches []Fetch
	ti      int // index to current topic in fetches[0]
	pi      int // index to current partition in current topic
	ri      int // index to current record in current partition
}

// Done returns whether there are any more records to iterate over.
func (i *FetchesRecordIter) Done() bool {
	return len(i.fetches) == 0
}

// Next returns the next record from a fetch.
func (i *FetchesRecordIter) Next() *Record {
	next := i.fetches[0].Topics[i.ti].Partitions[i.pi].Records[i.ri]
	i.ri++
	i.prepareNext()
	return next
}

func (i *FetchesRecordIter) prepareNext() {
beforeFetch0:
	if len(i.fetches) == 0 {
		return
	}

	fetch0 := &i.fetches[0]
beforeTopic:
	if i.ti >= len(fetch0.Topics) {
		i.fetches = i.fetches[1:]
		i.ti = 0
		goto beforeFetch0
	}

	topic := &fetch0.Topics[i.ti]
beforePartition:
	if i.pi >= len(topic.Partitions) {
		i.ti++
		i.pi = 0
		goto beforeTopic
	}

	partition := &topic.Partitions[i.pi]
	if i.ri >= len(partition.Records) {
		i.pi++
		i.ri = 0
		goto beforePartition
	}
}

// EachPartition calls fn for each partition in Fetches.
//
// Partitions are not visited in any specific order, and a topic may be visited
// multiple times if it is spread across fetches.
func (fs Fetches) EachPartition(fn func(FetchTopicPartition)) {
	for _, fetch := range fs {
		for _, topic := range fetch.Topics {
			for i := range topic.Partitions {
				fn(FetchTopicPartition{
					Topic:          topic.Topic,
					FetchPartition: topic.Partitions[i],
				})
			}
		}
	}
}

// EachTopic calls fn for each topic in Fetches.
//
// This is a convenience function that groups all partitions for the same topic
// from many fetches into one FetchTopic. A map is internally allocated to
// group partitions per topic before calling fn.
func (fs Fetches) EachTopic(fn func(FetchTopic)) {
	switch len(fs) {
	case 0:
		return
	case 1:
		for _, topic := range fs[0].Topics {
			fn(topic)
		}
		return
	}

	topics := make(map[string][]FetchPartition)
	for _, fetch := range fs {
		for _, topic := range fetch.Topics {
			topics[topic.Topic] = append(topics[topic.Topic], topic.Partitions...)
		}
	}

	for topic, partitions := range topics {
		fn(FetchTopic{
			topic,
			partitions,
		})
	}
}

// EachRecord calls fn for each record in Fetches.
//
// This is very similar to using a record iter, and is solely a convenience
// function depending on which style you prefer.
func (fs Fetches) EachRecord(fn func(*Record)) {
	for iter := fs.RecordIter(); !iter.Done(); {
		fn(iter.Next())
	}
}

// Records returns all records in all fetches.
//
// This is a convenience function that does a single slice allocation. If you
// can process records individually, it is far more efficient to use the Each
// functions or the RecordIter.
func (fs Fetches) Records() []*Record {
	rs := make([]*Record, 0, fs.NumRecords())
	fs.EachPartition(func(p FetchTopicPartition) {
		rs = append(rs, p.Records...)
	})
	return rs
}

// NumRecords returns the total number of records across all fetched partitions.
func (fs Fetches) NumRecords() (n int) {
	fs.EachPartition(func(p FetchTopicPartition) {
		n += len(p.Records)
	})
	return n
}

// Empty checks whether the fetch result empty. This method is faster than NumRecords() == 0.
func (fs Fetches) Empty() bool {
	for i := range fs {
		for j := range fs[i].Topics {
			for k := range fs[i].Topics[j].Partitions {
				if len(fs[i].Topics[j].Partitions[k].Records) > 0 {
					return false
				}
			}
		}
	}

	return true
}

// FetchTopicPartition is similar to FetchTopic, but for an individual
// partition.
type FetchTopicPartition struct {
	// Topic is the topic this is for.
	Topic string
	// FetchPartition is an individual partition within this topic.
	FetchPartition
}

// EachRecord calls fn for each record in the topic's partition.
func (r *FetchTopicPartition) EachRecord(fn func(*Record)) {
	for _, r := range r.Records {
		fn(r)
	}
}
