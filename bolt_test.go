package kv

import (
	"os"
	"path/filepath"
	"testing"
)

var (
	key        = "/a/b/c/myKey"
	val        = "val"
	dbFileName = "/tmp/bolt.db"
	nameSpace  = "test"
)

func TestBoltKv_GetSet(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

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

func TestBoltKv_GetSetWrongKey(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if val, err := kv.Get("wrongKey"); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestBoltKv_GetSetWrongBucket(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if val, err := kv.Get("/a/b/d/this"); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestBoltKv_SetEmptyKey(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

	// set something
	if err := kv.Set("", []byte(val)); err == nil {
		t.Fatal("expected err here")
	}
}

func TestBoltKv_GetEmptyKey(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if val, err := kv.Get(""); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestBoltKv_GetBucket(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

	// set something
	if err := kv.Set(key, []byte(val)); err != nil {
		t.Fatal(err)
	}

	// get that thing
	if val, err := kv.Get("/a/b/c"); err == nil {
		t.Fatal("expected error here, got value length:", len(val))
	}
}

func TestBoltKv_SetNilValue(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

	// set something
	if err := kv.Set(key, nil); err == nil {
		t.Fatal("expected err when trying to set a nil value")
	}
}

func TestBoltKv_SetZeroValue(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

	// set something
	if err := kv.Set(key, []byte{}); err != nil {
		t.Fatal(err)
	}
}

func TestBoltKv_GetZeroValue(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

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

func TestBoltKv_DeleteKey(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

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

func TestBoltKv_DeleteTree(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

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

func TestBoltKv_DeleteDeletedKey(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

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

func TestBoltKv_Enumerate(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

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

func TestBoltKv_DeleteEnumerate(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

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

func TestBoltKv_DeleteAllEnumerate(t *testing.T) {
	defer func() { _ = os.Remove(dbFileName) }()

	kv, closeKv, err := NewBoltKv(dbFileName, nameSpace)
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

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
		t.Fatal("did not expect any key to be listed, found:", len(keys))
	}
}
