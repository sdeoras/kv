package kv

import "github.com/boltdb/bolt"

// NewBoltKv provides a new instance of KV with bolt db as backend.
func NewBoltKv(dbFile, nameSpace string) (KV, CloseFunc, error) {
	kv := new(boltKv)
	kv.nameSpace = nameSpace
	var err error
	kv.db, err = bolt.Open(dbFile, 0666, nil)
	if err != nil {
		return nil, nil, err
	}
	f := func() error { return kv.db.Close() }

	if err := kv.db.Update(func(t *bolt.Tx) error {
		_, err := t.CreateBucketIfNotExists([]byte(nameSpace))
		return err
	}); err != nil {
		return nil, nil, err
	}
	return kv, f, nil
}
