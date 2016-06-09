package main

import (
	"github.com/garyburd/redigo/redis"
	"github.com/jasonlvhit/gocron"
	"github.com/streadway/amqp"
	"encoding/json"
	"os"
	"os/signal"
	"sync"
)

type Monitor struct {
	ID      string `json:"id"`
	Monitor struct {
		Check struct {
			Arguments string `json:"arguments"`
			Interval  int    `json:"interval"`
			Type      string `json:"type"`
		} `json:"check"`
		Notifier struct {
			Arguments string `json:"arguments"`
			Type      string `json:"type"`
		} `json:"notifier"`
	} `json:"monitor"`
}


var pool *redis.Pool
var conn *amqp.Connection
var channel *amqp.Channel

func InitPublisher() {
	if conn == nil {
		conn, _ = amqp.Dial("amqp://guest:guest@"+ os.Args[1] + ":5672/")
	}
	if channel == nil {
		channel,_  = conn.Channel()
	}
}

func main() {

	pool = redis.NewPool(func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", os.Args[2] + ":6379")
		if err != nil {
			panic(err)
		}
		return c, err
	},
		10)
	defer pool.Close()

	sch1 := gocron.NewScheduler()
	sch2 := gocron.NewScheduler()

  sch1.Every(30).Seconds().Do(getAndPublishInterval, 30)
	sch2.Every(60).Seconds().Do(getAndPublishInterval, 60)

	sch1.Start()
	sch2.Start()

	WaitForCtrlC()

}

func getAndPublishInterval(desiredInterval int) {
	client := pool.Get()
	response, _ := redis.Strings(client.Do("KEYS", "monitor-*"))
	for _, value := range response {
		storedMonitor, _ := redis.String(client.Do("GET", value))
		var monitor Monitor
		json.Unmarshal([]byte(storedMonitor), &monitor)
		if monitor.Monitor.Check.Interval == desiredInterval {
			publish("checks",monitor.Monitor.Check.Type,false,false,storedMonitor)
		}
	}
}

func WaitForCtrlC() {
    var end_waiter sync.WaitGroup
    end_waiter.Add(1)
    var signal_channel chan os.Signal
    signal_channel = make(chan os.Signal, 1)
    signal.Notify(signal_channel, os.Interrupt)
    go func() {
        <-signal_channel
        end_waiter.Done()
    }()
    end_waiter.Wait()
}

func publish(exchange string,key string,  mandatory bool,  immediate bool, msg string) {
	InitPublisher()
	channel.Publish(exchange, key, mandatory, immediate, amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
		})
}
