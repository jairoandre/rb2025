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
	Client       *redis.Client
	Db           *postgres.DB
	Processor    *processor.Processor
	Suspend      bool
	MaxTries     int
	Timeout      time.Duration
	ConsumerName string
}

const (
	paymentsStream         = "payments"
	paymentsFinishedStream = "payments-finished"
)

func NewBrokerPool(addr string, pass string, db *postgres.DB, pr *processor.Processor, consumerName string) *BrokerPool {
	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     pass, // or set if needed
		PoolSize:     30,
		MinIdleConns: 10,
	})
	broker := &BrokerPool{
		Client:       rdb,
		Db:           db,
		Processor:    pr,
		Suspend:      false,
		MaxTries:     3,
		Timeout:      500 * time.Millisecond,
		ConsumerName: consumerName,
	}
	broker.CreateGroup(paymentsStream, paymentsStream)
	broker.CreateGroup(paymentsFinishedStream, paymentsFinishedStream)
	return broker
}

func (r *BrokerPool) CreateGroup(stream string, groupName string) {
	ctx := context.Background()
	err := r.Client.XGroupCreateMkStream(ctx, stream, groupName, "$").Err()
	if err != nil {
		if err.Error() == "BUSYGROUP Consumer Group name already exists" {
			log.Printf("Consumer group %s already exists on stream %s", groupName, stream)
		} else {
			log.Fatalf("Failed to create consumer group: %v", err)
		}
	} else {
		log.Printf("Consumer group %s created on stream %s", groupName, stream)
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

func (r *BrokerPool) ReadFromStreamGroupJob(ctx context.Context, streamName string, groupName string, jobs chan<- redis.XMessage) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		// nothing
	}
	res, err := r.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: r.ConsumerName,
		Streams:  []string{streamName, ">"},
		Count:    10,
		Block:    5 * time.Second,
	}).Result()
	if err != nil {
		return err
	}
	for _, streamRes := range res {
		for _, msg := range streamRes.Messages {
			n, err := r.Client.XAck(ctx, streamName, groupName, msg.ID).Result()
			if err != nil {
				log.Printf("Error to ack message: %s, %v", msg.ID, err)
			} else if n == 0 {
				log.Printf("Message %s not acknowledge (not in PEL)", msg.ID)
			} else {
				jobs <- msg
			}
		}
	}
	return nil

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
			err := r.ReadFromStreamGroupJob(ctx, paymentsStream, paymentsStream, jobs)
			if err != nil {
				continue
			}
		}
	}()
	finishedPaymentsJobs := make(chan redis.XMessage, 1000)
	for range numWorkers {
		wg.Add(1)
		go worker(ctx, finishedPaymentsJobs, wg, r.ProcessFinishedEvent)
	}
	go func() {
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
			err := r.ReadFromStreamGroupJob(ctx, paymentsFinishedStream, paymentsFinishedStream, finishedPaymentsJobs)
			if err != nil {
				continue
			}
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
