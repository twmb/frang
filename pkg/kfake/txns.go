package kfake

import (
	"hash/crc32"
	"hash/fnv"
	"math"
	"math/rand"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

/*
* AddPartitionsToTxn
* AddOffsetsToTxn
* EndTxn
* TxnOffsetCommit
 */

// TODO
//
// * Add heap of last use, add index to pidseq, and remove pidseq as they
// exhaust max # of pids configured.
//
// * Wrap epochs

type (
	pids struct {
		ids map[int64]*pidseqs

		txs      map[*pidseqs]struct{}
		txNotify chan struct{}
		c        *Cluster
	}

	pidseqs struct {
		pids *pids

		id    int64
		epoch int16
		seqs  tps[pidseq]

		name          string
		txnal         bool
		timeoutMillis int32
		parts         tps[struct{}] // partitions in the transaction, if transactional
		txStart       time.Time
		inTx          bool
		batches       []*partBatch
		partDatas     map[*partData]struct{}
	}

	pidseq struct {
		seq [5]int32
		at  uint8
	}
)

func (pids *pids) pushTx(pid *pidseqs) {
	if pids.txs == nil {
		pids.txs = make(map[*pidseqs]struct{})
		pids.txNotify = make(chan struct{}, 1)
	}
	pids.txs[pid] = struct{}{}
	pid.inTx = true

	if len(pids.txs) > 1 {
		select {
		case pids.txNotify <- struct{}{}:
		default:
		}
		return
	}

	go func() {
		var exit bool
		for !exit {
			var minPid *pidseqs
			var minExpire time.Time
			pids.c.admin(func() {
				for pid := range pids.txs {
					timeout := time.Duration(pid.timeoutMillis) * time.Millisecond
					expire := pid.txStart.Add(timeout)
					if minPid == nil || expire.Before(minExpire) {
						minPid = pid
						minExpire = expire
					}
				}
				exit = len(pids.txs) == 0
			})
			if exit {
				return
			}

			timeout := time.NewTimer(time.Until(minExpire))
			select {
			case <-pids.c.die:
				timeout.Stop()
				return
			case <-pids.txNotify:
				timeout.Stop()
				continue
			case <-timeout.C:
			}

			pids.c.admin(func() {
				if _, ok := pids.txs[minPid]; !ok {
					return
				}
				minPid.finishTx(false)
				minPid.epoch++
				delete(pids.txs, minPid)
				exit = len(pids.txs) == 0
			})
		}
	}()
}

func (pids *pids) getpid(id int64) *pidseqs {
	if pids.ids == nil {
		return nil
	}
	seqs := pids.ids[id]
	return seqs
}

// Returns the pidseq and current epoch for this toppar. If this is
// transactional and the toppar has not been added to the txn, returns nil.
func (pids *pids) get(id int64, epoch int16, t string, p int32) (*pidseqs, *pidseq) {
	if pids.ids == nil {
		return nil, nil
	}
	seqs := pids.ids[id]
	if seqs == nil {
		return nil, nil
	}
	if seqs.txnal && !seqs.parts.checkp(t, p) {
		return nil, nil
	}
	return seqs, seqs.seqs.mkpDefault(t, p)
}

func (pids *pids) create(txid *string, timeoutMillis int32) (int64, int16) {
	if pids.ids == nil {
		pids.ids = make(map[int64]*pidseqs)
	}
	var id int64
	var name string
	if txid != nil {
		hasher := fnv.New64()
		hasher.Write([]byte(*txid))
		id = int64(hasher.Sum64()) & math.MaxInt64
		name = *txid
	} else {
		for {
			id = int64(rand.Uint64()) & math.MaxInt64
			if _, exists := pids.ids[id]; !exists {
				break
			}
		}
	}
	pid, exists := pids.ids[id]
	if exists {
		pid.epoch++
		return id, pid.epoch
	}
	pid = &pidseqs{
		pids:          pids,
		id:            id,
		name:          name,
		txnal:         txid != nil,
		timeoutMillis: timeoutMillis,
	}
	pids.ids[id] = pid
	return id, 0
}

func (pid *pidseqs) finishTx(commit bool) {
	defer func() {
		pid.parts = nil
		pid.txStart = time.Time{}
		pid.inTx = false
		if !commit {
			for _, batch := range pid.batches {
				batch.inTx = false
				batch.aborted = true
			}
		}
		pid.batches = nil
		delete(pid.pids.txs, pid)
		if len(pid.pids.txs) == 0 {
			select {
			case pid.pids.txNotify <- struct{}{}:
			default:
			}
		}
	}()

	attrs := int16(0x0010)
	if commit {
		attrs |= 0x0020
	}
	now := time.Now().UnixMilli()
	rec := kmsg.Record{
		Key:   []byte{0, 0, 0, 0},
		Value: []byte{0, 0, 0, 0, 0, 0},
	}
	if commit {
		key[3] = 1
	}
	rec.Length = int32(len(rec.AppendTo(nil)) - 1) // -1 because varintlen of this short record is 1 byte
	b := kmsg.RecordBatch{
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           attrs,
		LastOffsetDelta:      0,
		FirstTimestamp:       now,
		MaxTimestamp:         now,
		ProducerID:           pid.id,
		ProducerEpoch:        pid.epoch,
		FirstSequence:        -1,
		NumRecords:           1,
		Records:              rec.AppendTo(nil),
	}
	benc := b.AppendTo(nil)
	b.Length = int32(len(benc) - 12)
	b.CRC = int32(crc32.Checksum(benc[21:], crc32c))

	pid.parts.each(func(t string, p int32, _ *struct{}) {
		pd, ok := pid.pids.c.data.tps.getp(t, p)
		if !ok {
			return // topic deleted while in txn?
		}
		pd.pushBatch(len(benc), b, true)
	})
}

func (pid *pidseqs) addPart(t string, p int32) {
	pid.parts.mkpDefault(t, p)
	if len(pid.parts) > 1 {
		return
	}
	pid.txStart = time.Now()
	pid.pids.pushTx(pid)
}

func (s *pidseq) pushAndValidate(firstSeq, numRecs int32) (ok, dup bool) {
	// If there is no pid, we do not do duplicate detection.
	if s == nil {
		return true, false
	}
	var (
		seq    = firstSeq
		seq64  = int64(seq)
		next64 = (seq64 + int64(numRecs)) % math.MaxInt32
		next   = int32(next64)
	)
	for i := 0; i < 5; i++ {
		if s.seq[i] == seq && s.seq[(i+1)%5] == next {
			return true, true
		}
	}
	if s.seq[s.at] != seq {
		return false, false
	}
	s.at = (s.at + 1) % 5
	s.seq[s.at] = next
	return true, false
}
