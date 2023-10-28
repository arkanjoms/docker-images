package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type Topic struct {
	Name          string         `json:"name,omitempty"`
	Subscriptions []Subscription `json:"subscriptions,omitempty"`
}

type Subscription struct {
	Name string `json:"name,omitempty"`
}

func processTopic(ctx context.Context, client *pubsub.Client, t Topic) {
	topic := createTopic(ctx, client, t)
	for _, s := range t.Subscriptions {
		processSubscriptions(ctx, client, s, topic)
	}
}

func createTopic(ctx context.Context, client *pubsub.Client, t Topic) *pubsub.Topic {
	topic, err := client.CreateTopic(ctx, t.Name)
	if err != nil {
		if strings.Contains(err.Error(), "AlreadyExists") {
			fmt.Printf("Topic %s already exists\n", t.Name)
			return client.Topic(t.Name)
		}
		panic(fmt.Sprintf("CreateTopic: %v\n", err))
	}
	fmt.Printf("Topic created: %v\n", topic)
	return topic
}

func processSubscriptions(ctx context.Context, client *pubsub.Client, s Subscription, topic *pubsub.Topic) {
	sub, err := client.CreateSubscription(ctx, s.Name, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 20 * time.Second,
	})
	if err != nil {
		if strings.Contains(err.Error(), "AlreadyExists") {
			fmt.Printf("Subscription %s already exists\n", s.Name)
			return
		}
		panic(fmt.Sprintf("could not create subscription: %v\n", err))
	}
	fmt.Printf("Created subscription: %v\n", sub)
}

func loadFile() []Topic {
	content, err := os.ReadFile("config.json")
	if err != nil {
		panic(fmt.Sprintf("could not load file: %v\n", err))
	}
	var topics []Topic
	if err = json.Unmarshal(content, &topics); err != nil {
		panic(fmt.Sprintf("could not unmarshal data into struct: %v\n", err))
	}
	return topics
}

func main() {
	//_ = os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:7001")
	ctx := context.Background()
	projectID := os.Getenv("PUBSUB_PROJECT_ID")
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		panic(fmt.Sprintf("could not create pubsub client: %v\n", err))
	}
	defer func() {
		fmt.Printf("closing pubsub client: %v\n", client.Close())
	}()

	if projectID == "" {
		panic("env variable PUBSUB_PROJECT_ID is not set\n")
	}

	topics := loadFile()
	for _, t := range topics {
		processTopic(ctx, client, t)
	}
}
