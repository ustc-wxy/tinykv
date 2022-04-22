package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	s := &StandAloneStorage{
		db: engine_util.CreateDB(conf.DBPath, false),
	}
	return s
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	e := s.db.Close()
	if e != nil {
		log.Fatal(e)
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{
		s.db,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			if e := txn.Set(engine_util.KeyWithCF(m.Cf(), m.Key()), m.Value()); e != nil {
				return e
			}
		case storage.Delete:
			txn.Delete(m.Key())
			if e := txn.Delete(engine_util.KeyWithCF(m.Cf(), m.Key())); e != nil {
				return e
			}
		}
	}
	if e := txn.Commit(); e != nil {
		return e
	}
	return nil
}

type StandAloneStorageReader struct {
	db *badger.DB
}

func (sReader *StandAloneStorageReader) Close() {
	txn := sReader.db.NewTransaction(false)
	txn.Discard()
}
func (sReader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	v, e := engine_util.GetCF(sReader.db, cf, key)
	if e == badger.ErrKeyNotFound {
		return nil, nil
	}
	return v, e
}
func (sReader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := sReader.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}
