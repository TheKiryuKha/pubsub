package pubsub

// @todo: nice error handling and loggin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const ExchangeName string = "main"

type Pubsub struct {
	conn *rmq.AmqpConnection
	bus  *rmq.Publisher

	cancelConsumers context.CancelFunc
	wg              sync.WaitGroup
}

type Event struct {
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload"`
}

type Handler interface {
	Queue() string
	Events() []string
	Handle(context.Context, Event) error
}

func New(ctx context.Context, address string) (*Pubsub, error) {
	env := rmq.NewEnvironment(address, nil)

	conn, err := env.NewConnection(ctx)
	if err != nil {
		return nil, err
	}

	_, err = conn.Management().DeclareExchange(
		ctx,
		&rmq.DirectExchangeSpecification{Name: ExchangeName},
	)
	if err != nil {
		return nil, err
	}

	publisher, err := conn.NewPublisher(ctx, nil, nil)
	if err != nil {
		return nil, err
	}

	return &Pubsub{conn: conn, bus: publisher}, nil
}

/*
IMPORTANT!
This function must be called only once in the programm
*/
func (p *Pubsub) RegisterHandlers(ctx context.Context, handlers ...Handler) error {
	consumerCtx, cancel := context.WithCancel(ctx)
	p.cancelConsumers = cancel

	success := false
	defer func() {
		if !success {
			cancel()
			p.wg.Wait()
		}
	}()

	for _, handler := range handlers {
		// create queue
		qInfo, err := p.conn.Management().DeclareQueue(
			ctx,
			&rmq.QuorumQueueSpecification{Name: handler.Queue()},
		)
		if err != nil {
			return err
		}

		// bind routing keys to queue
		for _, event := range handler.Events() {
			_, err = p.conn.Management().Bind(ctx, &rmq.ExchangeToQueueBindingSpecification{
				SourceExchange:   ExchangeName,
				DestinationQueue: qInfo.Name(),
				BindingKey:       event,
			})
			if err != nil {
				return err
			}
		}

		// run consumer
		consumer, err := p.conn.NewConsumer(ctx, qInfo.Name(), nil)
		if err != nil {
			return err
		}

		p.wg.Add(1)
		go func(consumer *rmq.Consumer, handler Handler, ctx context.Context) {
			defer p.wg.Done()
			defer func() {
				cancelCtx, cancel := context.WithTimeout(
					context.Background(),
					5*time.Second,
				)
				defer cancel()

				consumer.Close(cancelCtx)
			}()

			for {
				delivery, err := consumer.Receive(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					log.Printf("%v. Queue: %s. Cancel Reciever", err, handler.Queue())
					return
				}

				msg := delivery.Message().Data[0]
				var event Event

				err = json.Unmarshal(msg, &event)
				if err != nil {
					// @todo: nice retries
					delivery.Requeue(ctx)
					continue
				}

				err = handler.Handle(ctx, event)
				if err != nil {
					delivery.Requeue(ctx)
					continue
				}

				err = delivery.Accept(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					log.Printf("%v. Queue: %s. Failed to accept delivery. Close reciever", err, handler.Queue())
					return
				}
			}
		}(consumer, handler, consumerCtx)
	}

	success = true

	return nil
}

func (p *Pubsub) Dispatch(ctx context.Context, event Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg, err := rmq.NewMessageWithAddress(
		payload,
		&rmq.ExchangeAddress{Exchange: ExchangeName, Key: event.Type},
	)
	if err != nil {
		return err
	}

	res, err := p.bus.Publish(ctx, msg)
	if err != nil {
		return err
	}

	switch res.Outcome.(type) {
	case *rmq.StateAccepted:
	default:
		return fmt.Errorf("Unexpected publish outcome: %v", res.Outcome)
	}

	return nil
}

func (p *Pubsub) Close(ctx context.Context) error {
	var errs []error

	if p.cancelConsumers != nil {
		p.cancelConsumers()
		p.wg.Wait()
	}

	if err := p.bus.Close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("Failed to close publisher, %v", err))
	}

	if err := p.conn.Close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("Failed to close connection, %v", err))
	}

	return errors.Join(errs...)
}
