package pkg

import (
	"errors"
	"fmt"

	"github.com/streadway/amqp"
)

type MessageBody struct {
	Data []byte
	Type string
}

type Message struct {
	Queue         string
	ReplyTo       string
	ContentType   string
	CorrelationID string
	Priority      uint8
	Body          MessageBody
}

type Connection struct {
	name     string
	conn     *amqp.Connection
	channel  *amqp.Channel
	exchange string
	queue    []string
	err      chan error
}

var (
	connectionPool = make(map[string]*Connection)
)

func NewConnection(name, exchange string, queue []string) *Connection {
	if c, ok := connectionPool[name]; ok {
		return c
	}
	c := &Connection{
		exchange: exchange,
		queue:    queue,
		err:      make(chan error),
	}

	connectionPool[name] = c

	return c
}

func GetConnection(name string) *Connection {
	return connectionPool[name]
}

func (c *Connection) Connect() error {
	var err error
	c.conn, err = amqp.Dial("amqp://root:endi@localhost:5672/")
	if err != nil {
		return fmt.Errorf("error in creating rabbitmq connection with %s", err.Error())
	}

	go func() {
		<-c.conn.NotifyClose(make(chan *amqp.Error))
		c.err <- errors.New("connection Closed")
	}()

	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("Channel: %s", err)
	}

	if err := c.channel.ExchangeDeclare(
		c.exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("Error in exchange Declare: %s", err)
	}
	return nil
}

func (c *Connection) BindQueue() error {
	for _, q := range c.queue {
		if _, err := c.channel.QueueDeclare(
			q,
			true,
			false,
			false,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("error in declaring the queue %s", err)
		}
		if err := c.channel.QueueBind(q, "my_routing_key", c.exchange, false, nil); err != nil {
			return fmt.Errorf("Queue bind error: %s", err)
		}
	}
	return nil
}

func (c *Connection) Reconnect() error {
	if err := c.Connect(); err != nil {
		return err
	}

	if err := c.BindQueue(); err != nil {
		return err
	}
	return nil
}

func (c *Connection) Consume() (map[string]<-chan amqp.Delivery, error) {
	m := make(map[string]<-chan amqp.Delivery)
	for _, q := range c.queue {
		deliveries, err := c.channel.Consume(
			q,
			"",
			false,
			false,
			false,
			false,
			nil,
		)

		if err != nil {
			return nil, err
		}

		m[q] = deliveries
	}
	return m, nil
}

func (c *Connection) HandleConsumedDeliveries(
	q string,
	delivery <-chan amqp.Delivery,
	fn func(Connection, string, <-chan amqp.Delivery),
) {
	for {
		go fn(*c, q, delivery)
		if err := <-c.err; err != nil {
			c.Reconnect()
			deliveries, err := c.Consume()
			if err != nil {
				panic(err)
			}
			delivery = deliveries[q]
		}
	}
}

func (c *Connection) Publish(m Message) error {
	select {
	case err := <-c.err:
		if err != nil {
			c.Reconnect()
		}
	default:
	}

	p := amqp.Publishing{
		Headers:       amqp.Table{"type": m.Body.Type},
		ContentType:   m.ContentType,
		CorrelationId: m.CorrelationID,
		Body:          m.Body.Data,
		ReplyTo:       m.ReplyTo,
	}
	if err := c.channel.Publish(c.exchange, m.Queue, false, false, p); err != nil {
		return fmt.Errorf("Error in publishing: %s", err)
	}
	return nil
}
