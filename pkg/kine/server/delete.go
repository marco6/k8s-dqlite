package server

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func isDelete(txn *etcdserverpb.TxnRequest) (int64, []byte, bool) {
	if len(txn.Compare) == 0 &&
		len(txn.Failure) == 0 &&
		len(txn.Success) == 2 &&
		txn.Success[0].GetRequestRange() != nil &&
		txn.Success[1].GetRequestDeleteRange() != nil {
		rng := txn.Success[1].GetRequestDeleteRange()
		return 0, rng.Key, true
	}
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Failure) == 1 &&
		txn.Failure[0].GetRequestRange() != nil &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestDeleteRange() != nil {
		return txn.Compare[0].GetModRevision(), txn.Success[0].GetRequestDeleteRange().Key, true
	}
	return 0, nil, false
}

func (l *LimitedServer) delete(ctx context.Context, key []byte, revision int64) (*etcdserverpb.TxnResponse, error) {
	rev, kv, ok, err := l.backend.Delete(ctx, key, revision)
	if err != nil {
		return nil, err
	}

	if !ok {
		return &etcdserverpb.TxnResponse{
			Header: txnHeader(rev),
			Responses: []*etcdserverpb.ResponseOp{
				{
					Response: &etcdserverpb.ResponseOp_ResponseRange{
						ResponseRange: &etcdserverpb.RangeResponse{
							Header: txnHeader(rev),
							Kvs:    toKVs(kv),
						},
					},
				},
			},
			Succeeded: false,
		}, nil
	}

	return &etcdserverpb.TxnResponse{
		Header: txnHeader(rev),
		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
					ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{
						Header:  txnHeader(rev),
						PrevKvs: toKVs(kv),
					},
				},
			},
		},
		Succeeded: true,
	}, nil
}
