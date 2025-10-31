package main

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	// Konfigurisanje konekcije
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // lista etcd servera
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Postavljanje ključa
	_, err = cli.Put(ctx, "mykey", "myvalue")
	if err != nil {
		log.Fatalf("Failed to put key: %v", err)
	}

	// Čitanje ključa
	resp, err := cli.Get(ctx, "mykey")
	if err != nil {
		log.Fatalf("Failed to get key: %v", err)
	}

	for _, kv := range resp.Kvs {
		fmt.Printf("Key: %s, Value: %s\n", kv.Key, kv.Value)
	}
}
