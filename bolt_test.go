package kv

import (
	"fmt"
	"testing"
)

func Test_Db(t *testing.T) {
	kv, closeKv, err := NewBoltKv("/tmp/bolt2.db", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer closeKv()

	if err := kv.Set("a/b/c/this", []byte("val")); err != nil {
		t.Fatal(err)
	}

	if val, err := kv.Get("a/b/c/this"); err != nil {
		t.Fatal(err)
	} else if string(val) != "val" {
		t.Fatal("not val")
	}

	if err := kv.Delete("a/b/"); err != nil {
		t.Fatal(err)
	}

	fmt.Println("=============")
	if val, err := kv.Get("a/b/c/this"); err == nil && val != nil {
		t.Fatal("expected returned value to be nil, got length", len(val))
	}
}
