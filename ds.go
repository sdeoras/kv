package kv

import (
	"context"
	"fmt"
	"path/filepath"

	"cloud.google.com/go/datastore"
)

type dsKv struct {
	ctx       context.Context
	nameSpace string
	client    *datastore.Client
}

type Buffer struct {
	Valid bool
	Value []byte
}

func newDataStoreKv(ctx context.Context, projectID, nameSpace string) (*dsKv, func() error, error) {
	client, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		return nil, nil, err
	}

	f := func() error {
		return client.Close()
	}

	return &dsKv{
		ctx:       ctx,
		nameSpace: nameSpace,
		client:    client,
	}, f, nil
}

func (d *dsKv) Get(key string) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}

	keys := splitKey(key, d.nameSpace)

	var parent *datastore.Key
	var child *datastore.Key
	for i := range keys {
		child = datastore.NameKey(d.nameSpace, filepath.Join(keys[:i+1]...), parent)
		if err := d.client.Get(d.ctx, child, new(Buffer)); err != nil {
			return nil, err
		}
		parent = child
	}

	b := new(Buffer)
	if err := d.client.Get(d.ctx, child, b); err != nil {
		return nil, err
	}

	if !b.Valid {
		return nil, fmt.Errorf("invalid key. key points to a bucket and not a value")
	}

	return b.Value, nil
}

func (d *dsKv) Set(key string, val []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("cannot set empty key")
	}

	if val == nil {
		return fmt.Errorf("val cannot be nil, use zero value instead")
	}

	var parent *datastore.Key
	var child *datastore.Key

	keys := splitKey(key, d.nameSpace)
	for i := range keys {
		i := i
		child = datastore.NameKey(d.nameSpace, filepath.Join(keys[:i+1]...), parent)
		err := d.client.Get(d.ctx, child, new(Buffer))
		switch err {
		// if the key does not exist, then put one
		case datastore.ErrNoSuchEntity, datastore.ErrInvalidKey:
			child, err = d.client.Put(d.ctx, child, new(Buffer))
			if err != nil {
				return err
			}
		case nil:
		default:
			return err
		}
		parent = child
	}

	b := new(Buffer)
	b.Value = val
	b.Valid = true
	_, err := d.client.Put(d.ctx, child, b)
	return err
}

func (d *dsKv) Delete(key string) error {
	keys, err := d.enumerate(key)
	if err != nil {
		return err
	}

	for _, key := range keys {
		if err := d.client.Delete(context.Background(), key); err != nil {
			return err
		}
	}

	return nil
}

func (d *dsKv) enumerate(key string) ([]*datastore.Key, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}

	keys := splitKey(key, d.nameSpace)

	var parent *datastore.Key
	var child *datastore.Key
	for i := range keys {
		child = datastore.NameKey(d.nameSpace, filepath.Join(keys[:i+1]...), parent)
		if err := d.client.Get(d.ctx, child, new(Buffer)); err != nil {
			return nil, err
		}
		parent = child
	}

	q := datastore.NewQuery("test").Ancestor(child).KeysOnly()

	return d.client.GetAll(context.Background(), q, nil)
}

func (d *dsKv) Enumerate(key string) ([]string, error) {
	keys, err := d.enumerate(key)
	if err != nil {
		return nil, err
	}

	var outKeys []string
	for _, key := range keys {
		b := new(Buffer)
		if err := d.client.Get(context.Background(), key, b); err != nil {
			return nil, err
		} else {
			if b.Valid {
				outKeys = append(outKeys, key.Name)
			}
		}
	}

	return outKeys, nil
}
