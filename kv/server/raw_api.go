package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	resp := &kvrpcpb.RawGetResponse{}
	val, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	resp.Value = val
	if val == nil {
		resp.NotFound = true
	}
	return resp, nil

}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	batch := []storage.Modify{
		{Data: put},
	}
	if err := server.storage.Write(req.Context, batch); err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawPutResponse{}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	batch := []storage.Modify{
		{Data: del},
	}

	if err := server.storage.Write(req.Context, batch); err != nil {
		return nil, err
	}
	resp := &kvrpcpb.RawDeleteResponse{}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := &kvrpcpb.RawScanResponse{}
	n := req.Limit
	if n == 0 {
		return resp, nil
	}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	it := reader.IterCF(req.Cf)
	defer it.Close()

	var res []*kvrpcpb.KvPair

	for it.Seek(req.StartKey); it.Valid(); it.Next() {
		item := it.Item()
		v, e := item.ValueCopy(nil)
		if e != nil {
			return nil, e
		}
		k := item.KeyCopy(nil)
		pair := &kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		}
		res = append(res, pair)
		n--
		if n == 0 {
			break
		}
	}
	resp.Kvs = res
	return resp, nil
}
