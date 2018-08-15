package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"pack.ag/amqp"
)

func main() {
	// Create client
	client, err := amqp.Dial("amqps://azbetlogsingest.servicebus.windows.net",
		amqp.ConnSASLPlain("RootManageSharedAccessKey", <RootManageSharedAccessKey-key>),
	)
	if err != nil {
		log.Fatal("Dialing AMQP server:", err)
	}
	defer client.Close()

	// Open a session
	session, err := client.NewSession()
	if err != nil {
		log.Fatal("Creating AMQP session:", err)
	}

	// Create a receiver
	receiver, err := session.NewReceiver(
		amqp.LinkSourceAddress("/azbetlogs5/ConsumerGroups/go-test/Partitions/1"),
		amqp.LinkCredit(10),
		amqp.LinkSelectorFilter(fmt.Sprintf("amqp.annotation.x-opt-enqueued-time > %d", time.Now().UTC().UnixNano()/1000000)),
	)
	if err != nil {
		log.Fatal("Creating receiver link:", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//bytesPerBucket := make(map[int]int64)

	for {
		// Receive next message
		msg, err := receiver.Receive(ctx)
		if err != nil {
			log.Fatal("Reading message from AMQP:", err)
		}

		// Accept message
		msg.Accept()

		if t, ok := msg.ApplicationProperties["Type"]; !ok {
			continue
		} else {
			if s, ok := t.(string); !ok || s != "Beats" {
				continue
			}
		}

		fmt.Println(string(msg.Data[0]))

		/*
			var timestamp time.Time
			if t, ok := msg.ApplicationProperties["Timestamp"]; !ok {
				continue
			} else {
				if s, ok := t.(string); !ok {
					continue
				} else {
					if timestamp, err = time.Parse("2006-01-02T15:04:05.000-07:00", s); err != nil {
						fmt.Println(err.Error())
						continue
					}
				}
			}

				if bucket, ok := bytesPerBucket[timestamp.Minute()]; !ok {
					bytesPerBucket[timestamp.Minute()] = int64(len(msg.Data[0]))
				} else {
					bytesPerBucket[timestamp.Minute()] = bucket + int64(len(msg.Data[0]))
				}
				fmt.Printf("%v\n", bytesPerBucket)
		*/
	}
}
