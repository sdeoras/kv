package kv

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/boltdb/bolt"
)

// CloseFunc is a closure that can be deferred called to close the database.
type CloseFunc func() error

// boltKv implements KV interface using boltdb as backend kv store.
type boltKv struct {
	// mu is used to lock update operations on database.
	mu sync.Mutex
	// nameSpace is the top level bucket name.
	nameSpace string
	// db is the database object for which database file is opened.
	db *bolt.DB
}

// newBoltKv provides a new instance of KV with bolt db as backend.
func newBoltKv(dbFile, nameSpace string) (*boltKv, CloseFunc, error) {
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

// Set sets a value at a key.
func (kv *boltKv) Set(key string, val []byte) error {
	if len(key) == 0 || val == nil {
		return fmt.Errorf("key can not be empty and val can not be nil")
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	return kv.db.Update(func(t *bolt.Tx) error {
		bucketList := strings.Split(filepath.Join(kv.nameSpace, key), "/")
		b := t.Bucket([]byte(bucketList[0]))
		bucketList = bucketList[1:]
		if len(bucketList) > 0 {
			var err error
			for i := 0; i < len(bucketList)-1; i++ {
				b, err = b.CreateBucketIfNotExists([]byte(bucketList[i]))
				if err != nil {
					return err
				}
			}
			return b.Put([]byte(bucketList[len(bucketList)-1]), val)
		} else {
			return b.Put([]byte(key), val)
		}
	})
}

// Get gets a value from a key.
func (kv *boltKv) Get(key string) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key can not be empty")
	}

	var val []byte
	err := kv.db.View(func(t *bolt.Tx) error {
		bucketList := strings.Split(filepath.Join(kv.nameSpace, key), "/")
		b := t.Bucket([]byte(bucketList[0]))
		bucketList = bucketList[1:]
		if len(bucketList) > 0 {
			for i := 0; i < len(bucketList)-1; i++ {
				val := b.Get([]byte(bucketList[i]))
				if val != nil {
					return fmt.Errorf(bucketList[i],
						"does not point at a bucket, it points to a value")
				}
				b = b.Bucket([]byte(bucketList[i]))
				if b == nil {
					return fmt.Errorf("bucket does not exist:%s",
						filepath.Join(bucketList[:i+1]...))
				}
			}
			val = b.Get([]byte(bucketList[len(bucketList)-1]))
		} else {
			val = b.Get([]byte(key))
		}
		return nil
	})

	// we have ensured during Set that value for any key cannot be nil.
	// A nil value means either key points to a bucket or key does not exist.
	// Both such conditions should result in error for the use case of this pkg.
	if val == nil {
		err = fmt.Errorf("invalid key")
	}

	return val, err
}

// Delete delets a key.
func (kv *boltKv) Delete(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("key can not be empty")
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	err := kv.db.Update(func(t *bolt.Tx) error {
		bucketList := strings.Split(filepath.Join(kv.nameSpace, key), "/")
		b := t.Bucket([]byte(bucketList[0]))
		bucketList = bucketList[1:]
		if len(bucketList) > 0 {
			for i := 0; i < len(bucketList)-1; i++ {
				val := b.Get([]byte(bucketList[i]))
				if val != nil {
					return fmt.Errorf(bucketList[i],
						"does not point at a bucket, it points to a value")
				}
				b = b.Bucket([]byte(bucketList[i]))
				if b == nil {
					return fmt.Errorf("bucket does not exist:%s",
						filepath.Join(bucketList[:i+1]...))
				}
			}
			key = bucketList[len(bucketList)-1]
		}

		val := b.Get([]byte(key))
		if val == nil {
			return b.DeleteBucket([]byte(key))
		} else {
			return b.Delete([]byte(key))
		}
	})

	if err != nil {
		return fmt.Errorf("key or bucket could not be deleted:%v", err)
	}

	return nil
}

func (kv *boltKv) Enumerate(key string) ([]string, error) {
	var list []string
	err := kv.db.View(func(t *bolt.Tx) error {

		// Get the bucket on which we would iterate for keys
		bucketList := strings.Split(filepath.Join(kv.nameSpace, key), "/")
		b := t.Bucket([]byte(bucketList[0]))
		bucketList = bucketList[1:]
		if len(bucketList) > 0 {
			for i := 0; i < len(bucketList); i++ {
				val := b.Get([]byte(bucketList[i]))
				if val != nil {
					return fmt.Errorf(bucketList[i],
						"does not point at a bucket, it points to a value")
				}
				b = b.Bucket([]byte(bucketList[i]))
				if b == nil {
					return fmt.Errorf("bucket does not exist:%s",
						filepath.Join(bucketList[:i+1]...))
				}
			}
		}

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v != nil {
				list = append(list, filepath.Join(key, string(k)))
			} else {
				if subList, err := kv.Enumerate(filepath.Join(key, string(k))); err != nil {
					return err
				} else {
					list = append(list, subList...)
				}
			}
		}
		return nil
	})

	return list, err
}
