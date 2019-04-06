package kv

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestMemKv_GetSet(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if retVal, err := kv.Get(key); err != nil {
		t.Fatal(err)
	} else if string(retVal) != val {
		t.Fatal("not val")
	}
}

func TestMemKv_GetSetWrongKey(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if val, err := kv.Get("wrongKey"); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestMemKv_GetSetWrongBucket(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if val, err := kv.Get("/a/b/d/this"); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestMemKv_SetEmptyKey(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set("", []byte(val)); err == nil {
		t.Fatal("expected err here")
	}
}

func TestMemKv_GetEmptyKey(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if val, err := kv.Get(""); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestMemKv_GetBucket(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if val, err := kv.Get("/a/b/c"); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestMemKv_SetNilValue(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, nil); err == nil {
		t.Fatal("expected err when trying to set a nil value")
	}
}

func TestMemKv_SetZeroValue(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, []byte{}); err != nil {
		t.Fatal(err)
	}
}

func TestMemKv_GetZeroValue(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, []byte{}); err != nil {
		t.Fatal(err)
	}

	if val, err := kv.Get(key); err != nil {
		t.Fatal(err)
	} else {
		if val == nil {
			t.Fatal("expected val to be zero length but not nil, got nil")
		} else {
			if len(val) != 0 {
				t.Fatal("expected val to be zero length, got:", len(val))
			}
		}
	}
}

func TestMemKv_DeleteKey(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// now delete the tree
	if err := kv.Delete(key); err != nil {
		t.Fatal(err)
	}

	// now ensure you can't get that thing
	if val, err := kv.Get(key); err == nil && val != nil {
		t.Fatal("expected returned value to be nil, got slice length:", len(val))
	}
}

func TestMemKv_DeleteTree(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	// set something else in the same bucket
	if err := kv.Set(filepath.Join(bktName, "someOtherKey"), []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}

	// now delete the tree
	if err := kv.Delete("/a/b"); err != nil {
		t.Fatal(err)
	}

	// now ensure you can't get that thing
	if val, err := kv.Get(key); err == nil && val != nil {
		t.Fatal("expected returned value to be nil, got slice length:", len(val))
	}

	// now ensure you can't get that other thing as well
	if val, err := kv.Get(filepath.Join(bktName, "someOtherKey")); err == nil && val != nil {
		t.Fatal("expected returned value to be nil, got slice length:", len(val))
	}
}

func TestMemKv_DeleteDeletedKey(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	// set something else in the same bucket
	if err := kv.Set(filepath.Join(bktName, "someOtherKey"), []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}

	// now delete the key
	if err := kv.Delete(key); err != nil {
		t.Fatal(err)
	}

	// now delete the key
	if err := kv.Delete(key); err == nil {
		t.Fatal("expected error when deleting key twice")
	}
}

func TestMemKv_Enumerate(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	// set something else in the same bucket
	if err := kv.Set(filepath.Join(bktName, "someOtherKey"), []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}

	keys, err := kv.Enumerate("/a/b/")
	if err != nil {
		t.Fatal(err)
	}

	for _, key := range keys {
		switch _, v := filepath.Split(key); v {
		case "myKey", "someOtherKey":
		default:
			t.Fatal("did not expect this key to be present in the list:", key)
		}
	}
}

func TestMemKv_DeleteEnumerate(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	// set something else in the same bucket
	if err := kv.Set(filepath.Join(bktName, "someOtherKey"), []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}

	if err := kv.Delete(key); err != nil {
		t.Fatal(err)
	}

	keys, err := kv.Enumerate("/a/b/")
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != 1 {
		t.Fatal("expected only one key, found:", len(keys))
	}

	for _, key := range keys {
		switch _, v := filepath.Split(key); v {
		case "someOtherKey":
		default:
			t.Fatal("did not expect this key to be present in the list:", key)
		}
	}
}

func TestMemKv_DeleteAllEnumerate(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv := NewMemKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	bktName, _ := filepath.Split(key)
	// set something else in the same bucket
	if err := kv.Set(filepath.Join(bktName, "someOtherKey"), []byte("someOtherValue")); err != nil {
		t.Fatal(err)
	}

	if err := kv.Delete(key); err != nil {
		t.Fatal(err)
	}

	if err := kv.Delete(filepath.Join(bktName, "someOtherKey")); err != nil {
		t.Fatal(err)
	}

	keys, err := kv.Enumerate("/a/b/")
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) > 0 {
		fmt.Println(keys)
		t.Fatal("did not expect any key to be listed, found:", len(keys))
	}
}
