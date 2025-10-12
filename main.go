package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"bsc-memes-indexer/lib"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("[info] received signal, shutting down...")
		cancel()
	}()

	producer, err := lib.NewProducer()
	if err != nil {
		log.Fatalf("[fatal] failed to create producer: %v", err)
	}
	defer producer.Close()

	trades, errChan := producer.Start(ctx)

	log.Println("[info] starting producer...")

	for {
		select {
		case <-ctx.Done():
			log.Println("[info] shutting down producer loop.")
			return
		case err, ok := <-errChan:
			if !ok {
				log.Println("[info] error channel closed.")
				return
			}
			if err != nil {
				log.Printf("[error] subscription failed: %v", err)
			}
		case trade, ok := <-trades:
			if !ok {
				log.Println("[info] trades channel closed.")
				return
			}
			jsonOutput, err := json.Marshal(trade)
			if err != nil {
				log.Printf("[warn] failed to marshal trade to JSON: %v", err)
				continue
			}
			// Using log.Println to ensure consistent output formatting
			log.Println(string(jsonOutput))
		}
	}
}
