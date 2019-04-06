package kv

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

type memdb struct {
	mu        sync.Mutex
	nameSpace string
	links     map[string]*node
}

type node struct {
	value []byte
	links map[string]*node
}

// newMemKv provides a new instance of KV
func newMemKv() *memdb {
	m := new(memdb)
	m.nameSpace = "default"
	m.links = make(map[string]*node)
	n := new(node)
	n.links = make(map[string]*node)
	m.links[m.nameSpace] = n
	return m
}

func (m *memdb) Get(key string) ([]byte, error) {
	n, ok := m.links[m.nameSpace]
	if !ok {
		return nil, fmt.Errorf("namespace not found")
	}

	for _, key := range splitKey(key, m.nameSpace) {
		if n.links == nil {
			return nil, fmt.Errorf("invalid key, key not found")
		}

		n, ok = n.links[key]
		if !ok {
			return nil, fmt.Errorf("invalid key, key not found")
		}
	}

	if len(n.links) != 0 || n.value == nil {
		return nil, fmt.Errorf("invalid key, key does not refer to leaf node:%d, %d",
			len(n.links), len(n.value))
	}

	b := make([]byte, len(n.value))
	for i := range n.value {
		b[i] = n.value[i]
	}

	return b, nil
}

func (m *memdb) Set(key string, val []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(key) == 0 {
		return fmt.Errorf("cannot set empty key")
	}

	if val == nil {
		return fmt.Errorf("cannot set nil value, use zero value please")
	}

	n, ok := m.links[m.nameSpace]
	if !ok {
		return fmt.Errorf("namespace not found")
	}

	for _, key := range splitKey(key, m.nameSpace) {
		if n.links == nil {
			n.links = make(map[string]*node)
		}

		if m, ok := n.links[key]; !ok {
			m = new(node)
			m.links = make(map[string]*node)
			n.links[key] = m
			n = m
		} else {
			n = m
		}
	}

	if len(n.links) != 0 {
		return fmt.Errorf("invalid key, key already exists and points to a bucket, not a value")
	}

	b := make([]byte, len(val))
	for i := range val {
		b[i] = val[i]
	}
	n.value = b

	return nil
}

func (m *memdb) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	n, ok := m.links[m.nameSpace]
	if !ok {
		return fmt.Errorf("namespace not found")
	}

	parent := n
	keyToDelete := key

	for _, key := range splitKey(key, m.nameSpace) {
		if n.links == nil {
			return fmt.Errorf("invalid key, key not found")
		}

		parent = n
		keyToDelete = key

		n, ok = n.links[key]
		if !ok {
			return fmt.Errorf("invalid key, key not found")
		}
	}

	delete(parent.links, keyToDelete)

	return nil
}

func (m *memdb) Enumerate(key string) ([]string, error) {
	n, ok := m.links[m.nameSpace]
	if !ok {
		return nil, fmt.Errorf("namespace not found")
	}

	for _, key := range splitKey(key, m.nameSpace) {
		if n.links == nil {
			return nil, fmt.Errorf("invalid key, key not found")
		}

		n, ok = n.links[key]
		if !ok {
			return nil, fmt.Errorf("invalid key, key not found")
		}
	}

	keys := make([]string, 0, len(n.links))
	for k, v := range n.links {
		if len(v.links) > 0 {
			subKeys, err := m.Enumerate(filepath.Join(key, k))
			if err != nil {
				return nil, err
			}
			keys = append(keys, subKeys...)
		} else {
			if v.value != nil {
				keys = append(keys, filepath.Join(key, k))
			}
		}
	}

	return keys, nil
}

func splitKey(key, nameSpace string) []string {
	key = filepath.Join(nameSpace, key)
	keys := strings.Split(key, "/")
	keys = keys[1:]
	return keys
}
