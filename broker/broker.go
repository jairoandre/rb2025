package broker

import (
	"context"
	"log"
	"payments-rinha/model"
	"payments-rinha/postgres"
	"payments-rinha/processor"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type BrokerPool struct {
	Client    *redis.Client
	Db        *postgres.DB
	Processor *processor.Processor
	Suspend   bool
	MaxTries  int
	Timeout   time.Duration
	GroupName string
}

func NewBrokerPool(addr string, pass string, db *postgres.DB, pr *processor.Processor, groupName string) *BrokerPool {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     pass, // or set if needed
		PoolSize:     30,
		MinIdleConns: 10,
	})
	return &BrokerPool{
		Client:    rdb,
		Db:        db,
		Processor: pr,
		Suspend:   false,
		MaxTries:  3,
		Timeout:   500 * time.Millisecond,
		GroupName: groupName,
	}
}

func (r *BrokerPool) CreateGroup(stream string) {
	ctx := context.Background()
	err := r.Client.XGroupCreateMkStream(ctx, stream, r.GroupName, "$").Err()
	if err != nil {
		if err.Error() == "BUSYGROUP Consumer Group name already exists" {
			log.Printf("Consumer group %s already exists on stream %s", r.GroupName, stream)
		} else {
			log.Fatalf("Failed to create consumer group: %v", err)
		}
	} else {
		log.Printf("Consumer group %s created on stream %s", r.GroupName, stream)
	}
}

func (r *BrokerPool) SendEvent(event map[string]any, streamName string) error {
	var lastErr error
	for attempt := 1; attempt <= r.MaxTries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), r.Timeout)
		defer cancel()
		err := r.Client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			Values: event,
		}).Err()
		if err == nil {
			return nil
		}
		lastErr = err
		log.Printf("Broker enqueue failed (attempt %d/%d): %v", attempt, r.MaxTries, err)
	}
	log.Printf("Broker enqueue permanently failed: %v", lastErr)
	return lastErr
}

func (r *BrokerPool) CheckSuspend() {
	defaultFailing, _ := r.Processor.DefaultHealth()
	if defaultFailing {
		fallbackFailing, _ := r.Processor.FallbackHealth()
		if fallbackFailing {
			log.Printf("Sleeping for 5 seconds...")
			time.Sleep(5 * time.Second)
			log.Printf("Waking up...")
			return
		}
	}
	log.Print("Unpause listener")
	r.Suspend = false
}

func (r *BrokerPool) ReadFromStreamJob(ctx context.Context, streanName string, lastId string, jobs chan<- redis.XMessage) (string, error) {
	select {
	case <-ctx.Done():
		return lastId, nil
	default:
		// nothing
	}
	res, err := r.Client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{streanName, lastId},
		Count:   1,
		Block:   0,
	}).Result()
	if err != nil {
		return lastId, err
	}

	messages := res[0].Messages

	for _, msg := range messages {
		jobs <- msg
		lastId = msg.ID
	}
	return lastId, nil

}

func worker(ctx context.Context, jobs <-chan redis.XMessage, wg *sync.WaitGroup, msgHandler func(redis.XMessage)) {
	defer wg.Done()
	for event := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
			msgHandler(event)
		}
	}
}

func (r *BrokerPool) StartListener(ctx context.Context, wg *sync.WaitGroup) {
	const numWorkers = 100
	jobs := make(chan redis.XMessage, 1000)
	for range numWorkers {
		wg.Add(1)
		go worker(ctx, jobs, wg, r.ProcessEvent)
	}
	go func() {
		lastId := "$"
		for {
			select {
			case <-ctx.Done():
				close(jobs)
				return
			default:
				// nothing
			}
			if r.Suspend {
				r.CheckSuspend()
				continue
			}
			newLastId, err := r.ReadFromStreamJob(ctx, "payments", lastId, jobs)
			if err != nil {
				continue
			}
			lastId = newLastId
		}
	}()
	finishedPaymentsJobs := make(chan redis.XMessage, 1000)
	for range numWorkers {
		wg.Add(1)
		go worker(ctx, finishedPaymentsJobs, wg, r.ProcessFinishedEvent)
	}
	go func() {
		lastId := "$"
		for {
			select {
			case <-ctx.Done():
				close(finishedPaymentsJobs)
				return
			default:
				// nothing
			}
			if r.Suspend {
				continue
			}
			newLastId, err := r.ReadFromStreamJob(ctx, "payments-finished", lastId, finishedPaymentsJobs)
			if err != nil {
				continue
			}
			lastId = newLastId
		}

	}()
}

func (r *BrokerPool) ProcessEvent(msg redis.XMessage) {
	event := model.PaymentEvent{
		CorrelationID: msg.Values["correlationId"].(string),
		Amount:        parseFloat(msg.Values["amount"]),
		RequestedAt:   msg.Values["requestedAt"].(string),
	}
	proc, err := r.Processor.SendPayment(event)
	if err != nil {
		r.Suspend = true
		// Resend to the queue
		r.SendEvent(event.ToMap(), "payments")
		return
	}
	paymentFinished := model.PaymentFinishedEvent{
		CorrelationID: event.CorrelationID,
		Amount:        event.Amount,
		RequestedAt:   event.RequestedAt,
		Processor:     proc,
	}
	r.SendEvent(paymentFinished.ToMap(), "payments-finished")
}

func (r *BrokerPool) ProcessFinishedEvent(msg redis.XMessage) {
	event := model.PaymentFinishedEvent{
		CorrelationID: msg.Values["correlationId"].(string),
		Amount:        parseFloat(msg.Values["amount"]),
		RequestedAt:   msg.Values["requestedAt"].(string),
		Processor:     parseInt(msg.Values["processor"]),
	}
	r.saveInDatabse(event)
}

func (r *BrokerPool) saveInDatabse(event model.PaymentFinishedEvent) {
	requestedAt, err := time.Parse(time.RFC3339, event.RequestedAt)
	if err != nil {
		log.Printf("Error converting date")
		return
	}
	err = r.Db.InsertPayment(event.CorrelationID, event.Amount, event.Processor, requestedAt)
	if err != nil {
		log.Printf("Error saving in the database: %v", err)
	}
}

func parseFloat(v any) float64 {
	switch val := v.(type) {
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	case float64:
		return val
	default:
		return 0
	}
}

func parseInt(v any) int {
	switch val := v.(type) {
	case string:
		f, _ := strconv.Atoi(val)
		return f
	case int:
		return val
	default:
		return 0
	}
}
