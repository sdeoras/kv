# kv
A minimalist key-value store for simple use cases.

## installation
```bash
go get github.com/sdeoras/kv
```

## usage
This package defines a minimalist key-value interface and provides following implementations:
* boltdb
* in-memory database
* Google cloud data-store

## keys
Keys can be simple strings or be written in a filepath format, e.g. `a/b/c/myKey`. Keys are parsed
and appropriate nested tree structure is constructed for whichever backend is being used.

Furthermore, a tree can be deleted simply by entering partial key (e.g. `a/b`), that is a common
prefix to other keys in that tree.

Leading `/` is ignored, i.e., `/a/b/c/myKey` is the same as `a/b/c/myKey`.

### using boltdb database as backend
To create an instance of `kv` using `boltdb` as
backend you can use the `NewBoltKv` function as follows. A namespace is
simply a partition inside the database file.
```go
import "github.com/sdeoras/kv"

func main() {
	kvdb, closeKv, err := kv.NewBoltKv(dbFileName, nameSpace)
	// handle err
	defer closeKv()
	
	if err := kvdb.Set(key, val); err != nil {
		// handle err
	}
	
	if val, err := kvdb.Get(key); err != nil {
		// handle err
	}
}
``` 

### using in-memory database backend
To create an instance of `KV` using in-memory backend you can use
`NewMemKv` function as follows:
```go
import "github.com/sdeoras/kv"

func main() {
	kvdb, err := kv.NewMemKv()
	// handle err
}
``` 

### using google cloud data-store backend
To create an instance of `KV` using google cloud data-store as backend you can use
`NewDataStoreKv` function as follows:
```go
import "github.com/sdeoras/kv"

func main() {
	kvdb, closeKv, err := kv.NewDataStoreKv(context.Background(), projectID, nameSpace)
	// handle err
	defer closeKv()
}
``` 

## nested keys
`key` can be represented in the filepath format. For instance
`/a/b/c/myKey1` and `/a/b/c/myKey2` are part of the same bucket
`/a/b/c` and both be deleted by deleting the key `/a/b/c`

## enumerate keys
If following keys are present in the db:
* /a/b/c/key1
* /a/b/c/key2

then enumerating using `/`, or `/a` or `/a/b` or `/a/b/c` would
result in the following list as output:
* /a/b/c/key1
* /a/b/c/key2