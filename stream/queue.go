package stream

import (
    "fmt"
    "sync"
    "github.com/confluentinc/confluent-kafka-go/kafka"
)

type ConcurrentQueue struct {
    queue        []interface{}
    mu           sync.Mutex
    notEmpty     *sync.Cond
    shutdown     chan struct{}
    dequeued     chan interface{}
    waitGroup    sync.WaitGroup
    kafkaProducer *kafka.Producer
    kafkaTopics   []string
}

func Start(kafkaBroker string, kafkaTopics []string) (*ConcurrentQueue) {
    q := &ConcurrentQueue{
        queue:     make([]interface{}, 0),
        shutdown:  make(chan struct{}),
        dequeued:  make(chan interface{}, 1),
        kafkaTopics: kafkaTopics,
    }
    q.notEmpty = sync.NewCond(&q.mu)

    producer, err := CreateProducer(kafkaBroker)
	if(err != nil) {
		return nil
	}

    q.kafkaProducer = producer

    return q
}

func (q *ConcurrentQueue) Enqueue(item interface{}) {
    q.mu.Lock()
    defer q.mu.Unlock()

    q.queue = append(q.queue, item)
    q.notEmpty.Signal()
}

func (q *ConcurrentQueue) Dequeue() interface{} {
    q.mu.Lock()
    defer q.mu.Unlock()

    for len(q.queue) == 0 {
        select {
        case <-q.shutdown:
            return nil
        default:
            q.notEmpty.Wait()
        }
    }

    item := q.queue[0]
    q.queue = q.queue[1:]

    return item
}

func (q *ConcurrentQueue) StartDequeue() {
    defer q.waitGroup.Done()

    for {
        item := q.Dequeue()
        if item == nil {
            return
        }

        msg := &kafka.Message{
            TopicPartition: kafka.TopicPartition{
                Topic:     &q.kafkaTopics[0],
                Partition: 0,
            },
            Value: []byte(fmt.Sprintf("%v", item)),
        }

        err := ProduceMessage(q.kafkaProducer, "logs", msg)
        if err != nil {
            fmt.Printf("Failed to send message to Kafka: %v\n", err)
        }

        q.dequeued <- item
    }
}
