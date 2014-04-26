package main

import (
	"github.com/streadway/amqp"
	"log"
	//"os"
	"fmt"
	"time"
	"math/rand"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func randInt(min int , max int) int {
    return min + rand.Intn(max-min)
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

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
		"",    // name
		false, // durable
		false, // delete when usused
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")
	
	routing_key := "twitter"
	log.Printf("Binding queue %s to exchange %s with routing key %s", q.Name, "logs_topic", routing_key)
	err = ch.QueueBind(
		q.Name,        // queue name
		routing_key,   // routing key
		"logs_topic", // exchange
		false,
			nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	rand.Seed( time.Now().UTC().UnixNano())
	
	for {
		for d := range msgs {
			log.Printf(" Processing %s", d.Body)
			randSec := time.Duration(randInt(1000,9000))
			time.Sleep( randSec * time.Millisecond)//simulate working
		}
	}
}
