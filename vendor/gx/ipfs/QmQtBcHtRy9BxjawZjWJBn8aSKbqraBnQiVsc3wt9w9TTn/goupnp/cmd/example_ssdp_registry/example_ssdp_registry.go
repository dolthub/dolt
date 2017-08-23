package main

import (
	"log"

	"gx/ipfs/QmQtBcHtRy9BxjawZjWJBn8aSKbqraBnQiVsc3wt9w9TTn/goupnp/ssdp"
)

func main() {
	c := make(chan ssdp.Update)
	srv, reg := ssdp.NewServerAndRegistry()
	reg.AddListener(c)
	go listener(c)
	if err := srv.ListenAndServe(); err != nil {
		log.Print("ListenAndServe failed: ", err)
	}
}

func listener(c <-chan ssdp.Update) {
	for u := range c {
		if u.Entry != nil {
			log.Printf("Event: %v USN: %s Entry: %#v", u.EventType, u.USN, *u.Entry)
		} else {
			log.Printf("Event: %v USN: %s Entry: <nil>", u.EventType, u.USN)
		}
	}
}
