package queue

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestBadgerDequeue(t *testing.T) {
	assert := assert.New(t)
	dir, _ := ioutil.TempDir("", "badger")
	bo := &BadgerOptions{
		Directory: dir,
	}

	b, err := NewBadgerQueue(bo, nil, zap.NewNop())
	assert.NoError(err)

	b.dequeuer() // Nothing there

	b.Enqueue(&jaeger.Batch{})
	b.dequeuer() // Should take item
	b.dequeuer() // All taken
	b.Enqueue(&jaeger.Batch{})
	b.Enqueue(&jaeger.Batch{})

	b.dequeuer() // Should take item
	b.dequeuer() // Should take item
	b.dequeuer() // Nothing there

	assert.True(false)
}

func TestOddTransaction(t *testing.T) {
	assert := assert.New(t)
	dir, _ := ioutil.TempDir("", "badger")

	opts := badger.DefaultOptions
	opts.TableLoadingMode = options.MemoryMap
	opts.Dir = dir
	opts.ValueDir = dir

	key := []byte("key1")

	db, err := badger.Open(opts)
	assert.NoError(err)

	txn := db.NewTransaction(true)
	txn.Set(key, nil)
	err = txn.Commit(nil)
	assert.NoError(err)
	txn.Discard()

	txn2 := db.NewTransaction(true)
	item, err := txn2.Get(key)
	assert.NoError(err)
	assert.Equal("key1", string(item.Key()))
	assert.NoError(err)
	err = txn2.Delete(key)
	assert.NoError(err)
	err = txn2.Commit(nil)
	txn.Discard()
	assert.NoError(err)
	txn2.Discard()
}

func TestDequeueIsolation(t *testing.T) {
	assert := assert.New(t)
	dir, _ := ioutil.TempDir("", "badger")
	bo := &BadgerOptions{
		Directory: dir,
	}

	b, err := NewBadgerQueue(bo, nil, zap.NewNop())
	assert.NoError(err)

	b.Enqueue(&jaeger.Batch{}) // One item stored, now we need to read it only once.. somehow

	key := []byte{0xFF}

	txn := b.store.NewTransaction(true)
	// b.dequeue(txn)
	txn.Set(key, []byte{0xFF})

	txn2 := b.store.NewTransaction(true)
	txn2.Set(key, nil)
	// b.dequeue(txn2)

	err = txn.Commit(nil)
	assert.NoError(err)
	err = txn2.Commit(nil)
	assert.Error(err)

	txn.Discard()
	txn2.Discard()

	assert.True(false)
}

func TestPostgresExamples(t *testing.T) {
	dir, _ := ioutil.TempDir("", "badger")

	opts := badger.DefaultOptions
	opts.TableLoadingMode = options.MemoryMap
	opts.Dir = dir
	opts.ValueDir = dir

	db, err := badger.Open(opts)
	if err != nil {
		t.FailNow()
	}

	txn1 := db.NewTransaction(true)
	_, err = txn1.Get([]byte("color"))
	if err == badger.ErrKeyNotFound {
		txn1.Set([]byte("color"), []byte("black"))
	}
	txn2 := db.NewTransaction(true)
	_, err = txn2.Get([]byte("color"))
	if err == badger.ErrKeyNotFound {
		txn2.Set([]byte("color"), []byte("white"))
	}
	err = txn2.Commit(nil)
	if err != nil {
		t.FailNow()
	}

	err = txn1.Commit(nil)
	if err != nil {
		fmt.Printf("txn1 commit failed\n")
		t.FailNow()
	}

	txn3 := db.NewTransaction(true)
	item, err := txn3.Get([]byte("color"))
	if err != nil {
		t.FailNow()
	}

	val, err := item.Value()
	if err != nil {
		t.FailNow()
	}

	fmt.Printf("%s\n", string(val))
	if string(val) != "white" {
		t.FailNow()
	}
}
