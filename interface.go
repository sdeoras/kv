package kv

import "context"

// CloseFunc is a closure that can be deferred called to close the database.
type CloseFunc func() error

// KV defines a minimalist interface to a key value store.
type KV interface {
	// Set sets a value against a key.
	// A key can be in the format of a path: a/b/c/key
	Set(key string, val []byte) error
	// Get gets a value from a key.
	Get(key string) ([]byte, error)
	// Delete delets a key deleting everything in the tree
	// if key points to a bucket name.
	Delete(key string) error
	// Enumerate lists keys
	Enumerate(key string) ([]string, error)
}

// NewBoltKv provides a new instance of KV with bolt db as backend.
func NewBoltKv(dbFile, nameSpace string) (KV, CloseFunc, error) {
	return newBoltKv(dbFile, nameSpace)
}

// NewMemKv provides a new instance of KV with mem db as backend.
func NewMemKv() KV {
	return newMemKv()
}

// NewDataStoreKv provides a new instance of KV with Google cloud data-store as backend.
func NewDataStoreKv(ctx context.Context, projectID, nameSpace string) (KV, CloseFunc, error) {
	return newDataStoreKv(ctx, projectID, nameSpace)
}
