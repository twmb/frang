package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() { regKey(24, 0, 3) }

func (c *Cluster) handleAddPartitionsToTxn(b *broker, kreq kmsg.Request) (kmsg.Response, error) {
	req := kreq.(*kmsg.AddPartitionsToTxnRequest)
	resp := req.ResponseKind().(*kmsg.AddPartitionsToTxnResponse)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	tidx := make(map[string]int)
	donet := func(t string, errCode int16) *kmsg.AddPartitionsToTxnResponseTopic {
		if i, ok := tidx[t]; ok {
			return &resp.Topics[i]
		}
		tidx[t] = len(resp.Topics)
		st := kmsg.NewAddPartitionsToTxnResponseTopic()
		st.Topic = t
		resp.Topics = append(resp.Topics, st)
		return &resp.Topics[len(resp.Topics)-1]
	}
	donep := func(t string, p int32, errCode int16) *kmsg.AddPartitionsToTxnResponseTopicPartition {
		sp := kmsg.NewAddPartitionsToTxnResponseTopicPartition()
		sp.Partition = p
		sp.ErrorCode = errCode
		st := donet(t, 0)
		st.Partitions = append(st.Partitions, sp)
		return &st.Partitions[len(st.Partitions)-1]
	}

	doneall := func(errCode int16) {
		for _, rt := range req.Topics {
			for _, rp := range rt.Partitions {
				donep(rt.Topic, rp, errCode)
			}
		}
	}

	var noAttempt bool
out:
	for _, rt := range req.Topics {
		ps, ok := c.data.tps.gett(rt.Topic)
		if !ok {
			noAttempt = true
			break out
		}
		for _, rp := range rt.Partitions {
			_, ok := ps[rp]
			if !ok {
				noAttempt = true
				break out
			}
		}
	}

	if noAttempt {
		for _, rt := range req.Topics {
			ps, ok := c.data.tps.gett(rt.Topic)
			for _, rp := range rt.Partitions {
				if !ok {
					donep(rt.Topic, rp, kerr.UnknownTopicOrPartition.Code)
					continue
				}
				_, ok := ps[rp]
				if !ok {
					donep(rt.Topic, rp, kerr.UnknownTopicOrPartition.Code)
					continue
				}
				donep(rt.Topic, rp, kerr.OperationNotAttempted.Code)
			}
		}
		return resp, nil
	}

	coordinator := c.coordinator(req.TransactionalID)
	if b != coordinator {
		doneall(kerr.NotCoordinator.Code)
		return resp, nil
	}

	pid := c.pids.getpid(req.ProducerID)
	if pid == nil {
		doneall(kerr.InvalidProducerIDMapping.Code)
		return resp, nil
	}
	if pid.epoch != req.ProducerEpoch {
		doneall(kerr.InvalidProducerEpoch.Code)
		return resp, nil
	}

	for _, rt := range req.Topics {
		for _, rp := range rt.Partitions {
			pid.addPart(rt.Topic, rp)
		}
	}

	return resp, nil
}
