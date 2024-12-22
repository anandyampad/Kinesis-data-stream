package main

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/vmware/vmware-go-kcl-v2/consumer"
)

func main() {

	args := os.Args[1:]

	// Check if arguments were provided
	if len(args) == 0 {
		log.Info("No command line arguments provided.")
		return
	}
	consumer.InitConsumer(args)

	//log.SetLevel(log.DebugLevel)

	// Keep the application running to process data continuously
	select {} // Blocks the main goroutine indefinitely
}
