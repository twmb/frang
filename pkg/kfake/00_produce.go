package kfake

import (
	"hash/crc32"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TODO
// * Leaders
// * Support txns
// * Multiple batches in one produce
// * Compact

func init() { regKey(0, 3, 9) }

func (c *Cluster) handleProduce(b *broker, kreq kmsg.Request) (kmsg.Response, error) {
	var (
		req   = kreq.(*kmsg.ProduceRequest)
		resp  = req.ResponseKind().(*kmsg.ProduceResponse)
		tdone = make(map[string][]kmsg.ProduceResponseTopicPartition)
	)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	donep := func(t string, p kmsg.ProduceRequestTopicPartition, errCode int16) *kmsg.ProduceResponseTopicPartition {
		sp := kmsg.NewProduceResponseTopicPartition()
		sp.Partition = p.Partition
		sp.ErrorCode = errCode
		ps := tdone[t]
		ps = append(ps, sp)
		tdone[t] = ps
		return &ps[len(ps)-1]
	}
	donet := func(t kmsg.ProduceRequestTopic, errCode int16) {
		for _, p := range t.Partitions {
			donep(t.Topic, p, errCode)
		}
	}
	donets := func(errCode int16) {
		for _, t := range req.Topics {
			donet(t, errCode)
		}
	}
	toresp := func() kmsg.Response {
		for topic, partitions := range tdone {
			st := kmsg.NewProduceResponseTopic()
			st.Topic = topic
			st.Partitions = partitions
			resp.Topics = append(resp.Topics, st)
		}
		return resp
	}

	switch req.Acks {
	case -1, 0, 1:
	default:
		donets(kerr.InvalidRequiredAcks.Code)
		return toresp(), nil
	}

	now := time.Now().UnixMilli()
	for _, rt := range req.Topics {
		for _, rp := range rt.Partitions {
			pd, ok := c.data.tps.getp(rt.Topic, rp.Partition)
			if !ok {
				donep(rt.Topic, rp, kerr.UnknownTopicOrPartition.Code)
				continue
			}
			if pd.leader != b {
				donep(rt.Topic, rp, kerr.NotLeaderForPartition.Code)
				continue
			}

			var b kmsg.RecordBatch
			if err := b.ReadFrom(rp.Records); err != nil {
				donep(rt.Topic, rp, kerr.CorruptMessage.Code)
				continue
			}
			if b.FirstOffset != 0 {
				donep(rt.Topic, rp, kerr.CorruptMessage.Code)
				continue
			}
			if int(b.Length) != len(rp.Records)-12 {
				donep(rt.Topic, rp, kerr.CorruptMessage.Code)
				continue
			}
			if b.PartitionLeaderEpoch != -1 {
				donep(rt.Topic, rp, kerr.CorruptMessage.Code)
				continue
			}
			if b.Magic != 2 {
				donep(rt.Topic, rp, kerr.CorruptMessage.Code)
				continue
			}
			if b.CRC != int32(crc32.Checksum(rp.Records[21:], crc32c)) { // crc starts at byte 21
				donep(rt.Topic, rp, kerr.CorruptMessage.Code)
				continue
			}
			attrs := uint16(b.Attributes)
			if attrs&0x0007 > 4 {
				donep(rt.Topic, rp, kerr.CorruptMessage.Code)
				continue
			}
			logAppendTime := int64(-1)
			if attrs&0x0008 > 0 {
				b.FirstTimestamp = now
				b.MaxTimestamp = now
				logAppendTime = now
			}
			if b.LastOffsetDelta != b.NumRecords-1 {
				donep(rt.Topic, rp, kerr.CorruptMessage.Code)
				continue
			}

			txnal := attrs&0xfff0 != 0
			pid, seq := c.pids.get(b.ProducerID, b.ProducerEpoch, rt.Topic, rp.Partition)
			if txnal && seq == nil {
				donep(rt.Topic, rp, kerr.InvalidTxnState.Code)
				continue
			}
			be := b.ProducerEpoch
			switch {
			case seq == nil && be != -1:
				donep(rt.Topic, rp, kerr.InvalidTxnState.Code)
				continue
			case seq != nil && be < pid.epoch:
				donep(rt.Topic, rp, kerr.FencedLeaderEpoch.Code)
				continue
			case seq != nil && be > pid.epoch:
				donep(rt.Topic, rp, kerr.UnknownLeaderEpoch.Code)
				continue
			}
			ok, dup := seq.pushAndValidate(b.FirstSequence, b.NumRecords)
			if !ok {
				donep(rt.Topic, rp, kerr.OutOfOrderSequenceNumber.Code)
				continue
			}
			if dup {
				donep(rt.Topic, rp, 0)
				continue
			}
			baseOffset := pd.highWatermark
			lso := pd.logStartOffset
			batchptr := pd.pushBatch(len(rp.Records), b, txnal)
			if txnal {
				pid.batches = append(pid.batches, batchptr)
			}
			sp := donep(rt.Topic, rp, 0)
			sp.BaseOffset = baseOffset
			sp.LogAppendTime = logAppendTime
			sp.LogStartOffset = lso
		}
	}

	if req.Acks == 0 {
		return nil, nil
	}
	return toresp(), nil
}

var crc32c = crc32.MakeTable(crc32.Castagnoli)
