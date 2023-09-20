package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TODO
// * v3+

func init() { regKey(22, 0, 2) }

func (c *Cluster) handleInitProducerID(b *broker, kreq kmsg.Request) (kmsg.Response, error) {
	var (
		req  = kreq.(*kmsg.InitProducerIDRequest)
		resp = req.ResponseKind().(*kmsg.InitProducerIDResponse)
	)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if req.TransactionalID != nil {
		txid := *req.TransactionalID
		if txid == "" {
			resp.ErrorCode = kerr.InvalidRequest.Code
			return resp, nil
		}
		coordinator := c.coordinator(txid)
		if b != coordinator {
			resp.ErrorCode = kerr.NotCoordinator.Code
			return resp, nil
		}
		if req.TransactionTimeoutMillis < 0 { // TODO transaction.max.timeout.ms
			resp.ErrorCode = kerr.InvalidTransactionTimeout.Code
			return resp, nil
		}
	}

	id, epoch := c.pids.create(req.TransactionalID, req.TransactionTimeoutMillis)
	resp.ProducerID = id
	resp.ProducerEpoch = epoch
	return resp, nil
}
