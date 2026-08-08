package pubsub

import (
	"context"
	"fmt"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const ExchangeName string = "main"

type Pubsub struct {
	ctx  context.Context
	conn *rmq.AmqpConnection
	bus  *rmq.Publisher
}

type Event struct {
	Type    string
	Message string
}

type Handler interface {
	Queue() string
	Events() []string
	Handle(Event)
}

func New(ctx context.Context, address string) (*Pubsub, error) {
	env := rmq.NewEnvironment(address, nil)

	conn, err := env.NewConnection(ctx)
	if err != nil {
		return &Pubsub{}, err
	}

	_, err = conn.Management().DeclareExchange(
		ctx,
		&rmq.DirectExchangeSpecification{Name: ExchangeName},
	)
	if err != nil {
		return &Pubsub{}, err
	}

	publisher, err := conn.NewPublisher(ctx, nil, nil)
	if err != nil {
		return &Pubsub{}, err
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
		qInfo, err := p.conn.Management().DeclareQueue(
			p.ctx,
			&rmq.QuorumQueueSpecification{Name: handler.Queue()},
		)
		if err != nil {
			return err
		}

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
	}
	return nil
}

func (p *Pubsub) Dispatch(event Event) error {
	msg, err := rmq.NewMessageWithAddress(
		[]byte(event.Message),
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
	_ = p.conn.Close(context.Background())
	_ = p.bus.Close(context.Background())
}
