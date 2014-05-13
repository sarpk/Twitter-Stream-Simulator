package workerpackage

import (
	"github.com/streadway/amqp"
	"log"
	"fmt"
)

type worker func([]byte) bool

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}


func ListenForever(msgs <-chan amqp.Delivery, myWorker worker) {
	for {
		for d := range msgs {
			if myWorker(d.Body) { //expect worker to return true if work is completed successfully
				d.Ack(false) //acknowledge message to be deleted from the queue
			}
		}
	}
}

// Initialises the worker and returns msg delivery 
func InitWorker(queueName string) <-chan amqp.Delivery {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	//defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	//defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs_topic", // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")
	q, err := ch.QueueDeclare(
		queueName,    // name
		false, // durable
		false, // delete when usused
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")
	
	ch.Qos(1, 0, false) //Take one message at a time
	
	routing_key := "twitter"
	exchange := "logs_topic"
	log.Printf("Binding queue %s to exchange %s with routing key %s", q.Name, exchange, routing_key)
	err = ch.QueueBind(
		q.Name,        // queue name
		routing_key,   // routing key
		exchange, // exchange
		false,
			nil)
	failOnError(err, "Failed to bind a queue")
	
	msgs, err := ch.Consume(
		q.Name, // name
		"",      // consumerTag,
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	
	return msgs
}
