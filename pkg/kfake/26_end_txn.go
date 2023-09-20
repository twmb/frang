package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() { regKey(26, 0, 3) }

func (c *Cluster) handleEndTxn(b *broker, kreq kmsg.Request) (kmsg.Response, error) {
	req := kreq.(*kmsg.EndTxnRequest)
	resp := req.ResponseKind().(*kmsg.EndTxnResponse)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	coordinator := c.coordinator(req.TransactionalID)
	if b != coordinator {
		resp.ErrorCode = kerr.NotCoordinator.Code
		return resp, nil
	}

	pid := c.pids.getpid(req.ProducerID)
	if pid == nil {
		resp.ErrorCode = kerr.InvalidProducerIDMapping.Code
		return resp, nil
	}
	if pid.epoch != req.ProducerEpoch {
		resp.ErrorCode = kerr.InvalidProducerEpoch.Code
		return resp, nil
	}
	if !pid.inTx {
		resp.ErrorCode = kerr.InvalidTxnState.Code
		return resp, nil
	}
	pid.finishTx(req.Commit)
	return resp, nil
}
