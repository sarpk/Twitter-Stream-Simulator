package main
 
import (
    "log"
    "time"
	"flag"
	"fmt"
    "github.com/darkhelmet/twitterstream"
	"github.com/streadway/amqp"
)
 
 var (
    consumerKey    = flag.String("consumer-key", "", "consumer key")
    consumerSecret = flag.String("consumer-secret", "", "consumer secret")
    accessToken    = flag.String("access-token", "", "access token")
    accessSecret   = flag.String("access-secret", "", "access token secret")
	streamTrack    = flag.String("track", "justin, obama, royal, charles, prince, navy, legal, law, life, eating, dining, at", "Twitter API Stream Tracker Keyword")
)
 
func decode(conn *twitterstream.Connection, channel *amqp.Channel) {
    for {
        if tweet, err := conn.Next(); err == nil {
            //log.Printf("%s said: %s", tweet.User.ScreenName, tweet.Text)
			sendMsg(channel, tweet.Text)
        } else {
            log.Printf("Failed decoding tweet: %s", err)
            return
        }
    }
}
 
func twitterHandle(channel *amqp.Channel) {
	client := twitterstream.NewClient(*consumerKey, *consumerSecret, *accessToken, *accessSecret)
    for {
        conn, err := client.Track(*streamTrack)
		log.Printf("Tracking %s ...", *streamTrack)
        if err != nil {
            log.Println("Tracking failed, sleeping for 1 minute")
            time.Sleep(1 * time.Minute)
            continue
        }
        decode(conn, channel)
    }
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func sendMsg(channel *amqp.Channel, message string) {
	err := channel.Publish(
		"logs_topic",         // exchange
		"twitter",            // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp.Publishing{
			ContentType:     "text/plain",
			Body:            []byte(message),
		})

	failOnError(err, "Failed to publish a message")
	//log.Printf(" [x] Sent %s", message)

}

func rabbitMQInit() *amqp.Channel {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	//defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	//defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs_topic", // name
		"topic",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // noWait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare an exchange")
	return ch
}
 
func main() {
	channel := rabbitMQInit()
	//sendMsg(channel, "check")
	twitterHandle(channel)
}