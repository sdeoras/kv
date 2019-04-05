# kv
A minimalist key-value store for simple use cases.

## installation
```bash
go get github.com/sdeoras/kv
```

## usage
This package defines an interface and provides an implementation
using `boltdb`. To create an instance of `kv` using `boltdb` as
backend you can use the `New` method as follows:
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