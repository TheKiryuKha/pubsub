package pubsub

import (
	"context"

	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

type Pubsub struct {
	conn *rmq.AmqpConnection
}

func New(ctx context.Context, address string) (*Pubsub, error) {
	env := rmq.NewEnvironment(address, nil)

	conn, err := env.NewConnection(ctx)
	if err != nil {
		return &Pubsub{}, err
	}

	return &Pubsub{conn}, nil
}
