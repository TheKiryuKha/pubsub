package pubsub

// @todo: nice error handling and loggin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const ExchangeName string = "main"

type Pubsub struct {
	ctx  context.Context
	conn *rmq.AmqpConnection
	bus  *rmq.Publisher
}

type Event struct {
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload"`
}

type Handler interface {
	Queue() string
	Events() []string
	Handle(Event) error
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

	return &Pubsub{ctx: ctx, conn: conn, bus: publisher}, nil
}

/*
This thing should:

- create an echange
- create neccessary queues
- register recievers as handlers(idk, LOL)
*/
func (p *Pubsub) RegisterHandlers(handlers ...Handler) error {
	for _, handler := range handlers {
		// create queue
		qInfo, err := p.conn.Management().DeclareQueue(
			p.ctx,
			&rmq.QuorumQueueSpecification{Name: handler.Queue()},
		)
		if err != nil {
			return err
		}

		// bind routing keys to queue
		for _, event := range handler.Events() {
			_, err = p.conn.Management().Bind(p.ctx, &rmq.ExchangeToQueueBindingSpecification{
				SourceExchange:   ExchangeName,
				DestinationQueue: qInfo.Name(),
				BindingKey:       event,
			})
			if err != nil {
				return err
			}
		}

		// run consumer
		consumer, err := p.conn.NewConsumer(p.ctx, qInfo.Name(), nil)
		if err != nil {
			return err
		}

		go func(consumer *rmq.Consumer, handler Handler) {
			defer consumer.Close(p.ctx)

			for {
				delivery, err := consumer.Receive(p.ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					log.Printf("%v", err)
					continue
				}

				msg := delivery.Message().Data[0]
				var event Event

				err = json.Unmarshal(msg, &event)
				if err != nil {
					// @todo: nice retries
					delivery.Requeue(p.ctx)
					continue
				}

				err = handler.Handle(event)
				if err != nil {
					delivery.Requeue(p.ctx)
					continue
				}

				delivery.Accept(p.ctx)
			}
		}(consumer, handler)
	}
	return nil
}

func (p *Pubsub) Dispatch(event Event) error {
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

	res, err := p.bus.Publish(p.ctx, msg)
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

func (p *Pubsub) Close() {
	_ = p.bus.Close(context.Background())
	_ = p.conn.Close(context.Background())
}
