package queue

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	b "github.com/go-swagger/go-swagger/fixtures/bugs/910"
	"github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	"go.uber.org/zap"
)

const (
	txIDSeqKey byte = 0x01
	queueKey   byte = 0x10
	lockSuffix byte = 0xFF
)

// BadgerOptions includes all the parameters for the underlying badger instance
type BadgerOptions struct {
	Directory   string
	SyncWrites  bool
	Concurrency int
}

type consumerMessage struct {
	batch *jaeger.Batch
	txID  uint64
}

// BadgerQueue is persistent backend for the queued reporter
type BadgerQueue struct {
	queueCount   int32
	store        *badger.DB
	logger       *zap.Logger
	txIDGen      *badger.Sequence
	processor    func(*jaeger.Batch) error
	receiverChan chan consumerMessage
	closeChan    chan struct{}
	ackChan      chan uint64
	notifyChan   chan bool
}

// NewBadgerQueue returns
func NewBadgerQueue(bopts *BadgerOptions, processor func(*jaeger.Batch) error, logger *zap.Logger) (*BadgerQueue, error) {
	opts := badger.DefaultOptions
	opts.TableLoadingMode = options.MemoryMap
	opts.Dir = bopts.Directory
	opts.ValueDir = bopts.Directory
	opts.SyncWrites = bopts.SyncWrites

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	seq, err := db.GetSequence([]byte{txIDSeqKey}, 1000)
	if err != nil {
		return nil, err
	}

	// Remove all the lock keys here and calculate current size - the process has restarted
	initialCount, err := b.initializeQueue()
	if err != nil {
		return nil, err
	}

	b := &BadgerQueue{
		queueCount:   initialCount,
		logger:       logger,
		store:        db,
		txIDGen:      seq,
		ackChan:      make(chan uint64, bopts.Concurrency),
		receiverChan: make(chan consumerMessage, bopts.Concurrency),
		closeChan:    make(chan struct{}),
		notifyChan:   make(chan bool),
		processor:    processor,
	}

	if b.processor != nil {
		// Start ack processing
		go b.acker()
		// Start dequeue processing
		go b.dequeuer()
		// Start processing items
		for i := 0; i < bopts.Concurrency; i++ {
			go b.batchProcessor()
		}
	} else {
		b.logger.Error("No processor defined for the queue reporter, items will only be stored in the queue")
	}

	// TODO Start maintenance and other.. as soon as these are in the common place
	/*
		f.registerBadgerExpvarMetrics(metricsFactory)

		go f.maintenance()
		go f.metricsCopier()
	*/
	// TODO Add metrics (queued items, dequeued items, queue size, average queue waiting time etc..)
	return b, nil
}

func (b *BadgerQueue) batchProcessor() {
	for {
		select {
		case cm := <-b.receiverChan:
			// Any error is not retryable and has nothing to do with queue implementation..
			err := b.processor(cm.batch)
			if err != nil {
				b.logger.Error("Could not process batch", zap.Error(err))
			}
			b.ackChan <- cm.txID
		case <-b.closeChan:
			return
		}
	}
}

// Enqueue stores the batch to the Badger's storage
func (b *BadgerQueue) Enqueue(batch *jaeger.Batch) error {
	buf := new(bytes.Buffer)
	txID, err := b.nextTransactionID()
	if err != nil {
		return err
	}
	binary.Write(buf, binary.BigEndian, queueKey)
	binary.Write(buf, binary.BigEndian, txID)
	key := buf.Bytes()
	binary.Write(buf, binary.BigEndian, lockSuffix)
	lockKey := buf.Bytes()

	bypassQueue := false

	atomic.LoadInt32(&b.queueCount) < cap(b.receiverChan) {
		bypassQueue = true
	}

	// TODO Needs Thrift serializer and deserializer..

	// var val []byte
	// val, err = proto.Marshal(batch)

	// if err != nil {
	// 	return err
	// }

	err = b.store.Update(func(txn *badger.Txn) error {
		if bypassQueue {
			err := txn.Set(lockKey, nil)
			if err != nil {
				return err
			}
		}
		// TODO Add payload
		return txn.Set(key, nil)
	})
	atomic.AddInt32(&b.queueCount, int32(1))
	if err == nil && bypassQueue {
		// Push to channel since there's space - no point reading it from the badger
		b.receiverChan <- consumerMessage{
			txID:  txID,
			batch: batch,
		}
	} else if err == nil && !bypassQueue {
		// Notify the dequeuer that new stuff has been added (if it is sleeping)
		select {
		case b.notifyChan <- true:
		default:
		}
	}
	return err
}

func (b *BadgerQueue) nextTransactionID() (uint64, error) {
	return b.txIDGen.Next()
}

// getLockTime calculates the proper timeout for processed item
func (b *BadgerQueue) getLockTime() time.Duration {
	return time.Hour
}

// dequeuer runs in the background and pushes data to the channel whenever there's available space.
// This is done to push racing of concurrent processors to the chan implementation instead of badger.
// However, this method should stay concurrent safe (if there's need to add more loaders). To provide
// that safety, we read the lock key before writing it - this causes a transaction error with
// snapshot isolation.
func (b *BadgerQueue) dequeuer() {
	for {
		select {
		case <-b.notifyChan:
			// Process all the items in the queue
			err := b.dequeueAvailableItems()
			if err != nil {
				b.logger.Error("Could not dequeue next item from badger", zap.Error(err))
			}
		default:
			// Keep sleeping
		}
	}
}

func parseItem(item *badger.Item) (consumerMessage, error) {
	txID := binary.BigEndian.Uint64(item.Key()[1:])
	value := []byte{}
	value, err := item.ValueCopy(value)
	if err != nil {
		return consumerMessage{}, err
	}

	// TODO Deserialize the *batch.Jaeger

	cm := consumerMessage{
		txID: txID,
	}

	return cm, nil
}

// dequeueAvailableItems fetches items from the queue until there's no more to be fetched
// and places them to the receiverChan for processing
func (b *BadgerQueue) dequeueAvailableItems() error {
	for {
		fetchedItems := make([]consumerMessage, 0, cap(b.receiverChan))

		err := b.store.Update(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()

			prefix := []byte{queueKey}
			lockKey := make([]byte, 10)

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				lockKey = lockKey[:0]
				copy(lockKey, item.Key())
				lockKey[10] = lockSuffix

				it.Next()
				if !it.Valid() {
					// We're at the last item in the queue - but this one wasn't taken
					cm, err := parseItem(item)
					if err != nil {
						return err
					}
					fetchedItems = append(fetchedItems, cm)

					txn.Set(lockKey, nil)
					return nil
				}

				// There is next key, lets check it..
				if bytes.Equal(item.Key(), lockKey) {
					// This item is taken already - proceed to next one
					continue
				} else {
					cm, err := parseItem(item)
					if err != nil {
						return err
					}
					fetchedItems = append(fetchedItems, cm)

					txn.Set(lockKey, nil)
					return nil
				}
			}
			return nil
		})

		if err != nil {
			return err
		}

		for _, cm := range fetchedItems {
			b.receiverChan <- cm
		}

		// We cleared the queue during this transaction, back to sleep
		if len(fetchedItems) < cap(fetchedItems) {
			return nil
		}
	}
}

// acker receives acknowledgements transactionIds from the ackChan and delete them from badger
func (b *BadgerQueue) acker() {
	for {
		select {
		case id := <-b.ackChan:
			buf := new(bytes.Buffer)
			binary.Write(buf, binary.BigEndian, queueKey)
			binary.Write(buf, binary.BigEndian, id)
			key := buf.Bytes()
			binary.Write(buf, binary.BigEndian, byte(0xFF))
			lockKey := buf.Bytes()
			err := b.store.Update(func(txn *badger.Txn) error {
				txn.Delete(key)
				txn.Delete(lockKey)
				return nil
			})
			if err != nil {
				b.logger.Error("Could not remove item from the queue", zap.Error(err))
			}
			atomic.AddInt32(&b.queueCount, int32(-1))
		case <-b.closeChan:
			return
		}
	}
}

// Close stops processing items from the queue and closes the underlying storage
func (b *BadgerQueue) Close() {
	close(b.closeChan)
	err := b.store.Close()
	if err != nil {
		b.logger.Error("Could not close badger instance properly", zap.Error(err))
	}
}

func (b *BadgerQueue) initializeQueue() (uint32, error) {
	// Remove all keys, set the correct queue size
	return 0, nil
}

// TODO Add to the previous ones.. max retries option (with requeue or ditch) - sort of like DLQ so that one object doesn't close the whole processing
// if one item is broken

// TODO Could these be in some common repository for all badger parts, this one as well as the main storage
/*
// Maintenance starts a background maintenance job for the badger K/V store, such as ValueLogGC
func (f *Factory) maintenance() {
	maintenanceTicker := time.NewTicker(f.Options.primary.MaintenanceInterval)
	defer maintenanceTicker.Stop()
	for {
		select {
		case <-f.maintenanceDone:
			return
		case t := <-maintenanceTicker.C:
			var err error

			// After there's nothing to clean, the err is raised
			for err == nil {
				err = f.store.RunValueLogGC(0.5) // 0.5 is selected to rewrite a file if half of it can be discarded
			}
			if err == badger.ErrNoRewrite {
				f.metrics.LastValueLogCleaned.Update(t.UnixNano())
			} else {
				f.logger.Error("Failed to run ValueLogGC", zap.Error(err))
			}

			f.metrics.LastMaintenanceRun.Update(t.UnixNano())
			f.diskStatisticsUpdate()
		}
	}
}

func (f *Factory) metricsCopier() {
	metricsTicker := time.NewTicker(f.Options.primary.MetricsUpdateInterval)
	defer metricsTicker.Stop()
	for {
		select {
		case <-f.maintenanceDone:
			return
		case <-metricsTicker.C:
			expvar.Do(func(kv expvar.KeyValue) {
				if strings.HasPrefix(kv.Key, "badger") {
					if intVal, ok := kv.Value.(*expvar.Int); ok {
						if g, found := f.metrics.badgerMetrics[kv.Key]; found {
							g.Update(intVal.Value())
						}
					} else if mapVal, ok := kv.Value.(*expvar.Map); ok {
						mapVal.Do(func(innerKv expvar.KeyValue) {
							// The metrics we're interested in have only a single inner key (dynamic name)
							// and we're only interested in its value
							if intVal, ok := innerKv.Value.(*expvar.Int); ok {
								if g, found := f.metrics.badgerMetrics[kv.Key]; found {
									g.Update(intVal.Value())
								}
							}
						})
					}
				}
			})
		}
	}
}

func (f *Factory) registerBadgerExpvarMetrics(metricsFactory metrics.Factory) {
	f.metrics.badgerMetrics = make(map[string]metrics.Gauge)

	expvar.Do(func(kv expvar.KeyValue) {
		if strings.HasPrefix(kv.Key, "badger") {
			if _, ok := kv.Value.(*expvar.Int); ok {
				g := metricsFactory.Gauge(metrics.Options{Name: kv.Key})
				f.metrics.badgerMetrics[kv.Key] = g
			} else if mapVal, ok := kv.Value.(*expvar.Map); ok {
				mapVal.Do(func(innerKv expvar.KeyValue) {
					// The metrics we're interested in have only a single inner key (dynamic name)
					// and we're only interested in its value
					if _, ok = innerKv.Value.(*expvar.Int); ok {
						g := metricsFactory.Gauge(metrics.Options{Name: kv.Key})
						f.metrics.badgerMetrics[kv.Key] = g
					}
				})
			}
		}
	})
}
*/
