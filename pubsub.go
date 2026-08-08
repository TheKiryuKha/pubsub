package pubsub

import (
	"context"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const ExchangeName string = "main"

/*
@todo: implement Close function

defer pub.Close()

- conn.Close()
- publisher.Close()
*/
type Pubsub struct {
	conn *rmq.AmqpConnection
	ctx  context.Context
}

type Message struct {
	Message string
}

type Handler interface {
	Queue() string
	Events() []string
	Handle(Message)
}

func New(ctx context.Context, address string) (*Pubsub, error) {
	env := rmq.NewEnvironment(address, nil)

	conn, err := env.NewConnection(ctx)
	if err != nil {
		return &Pubsub{}, err
	}

	return &Pubsub{conn: conn, ctx: ctx}, nil
}

/*
This thing should:

- create an echange
- create neccessary queues
- register recievers as handlers(idk, LOL)
*/
func (p *Pubsub) RegisterHandlers(handlers ...Handler) error {
	_, err := p.conn.Management().DeclareExchange(
		p.ctx,
		&rmq.DirectExchangeSpecification{Name: ExchangeName},
	)
	if err != nil {
		return err
	}

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
