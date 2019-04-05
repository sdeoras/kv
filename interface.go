package kv

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
