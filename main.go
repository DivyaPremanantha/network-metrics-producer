package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

// NetworkMetrics is one metrics event for a network service (fictional demo data).
type NetworkMetrics struct {
	ServiceId      string `json:"serviceId"`
	Timestamp      string `json:"timestamp"`
	BytesIn        int64  `json:"bytesIn"`
	BytesOut       int64  `json:"bytesOut"`
	DroppedPackets int64  `json:"droppedPackets"`
	FcsErrors      int64  `json:"fcsErrors"`
}

func main() {
	serviceCert, err := os.ReadFile("/service.cert")
	if err != nil {
		log.Fatalf("Failed to load SERVICE_CERT: %s", err)
	}
	serviceKey, err := os.ReadFile("/service.key")
	if err != nil {
		log.Fatalf("Failed to load SERVICE_KEY: %s", err)
	}
	caCert, err := os.ReadFile("/ca.pem")
	if err != nil {
		log.Fatalf("Failed to load CA_CERT: %s", err)
	}

	keypair, err := tls.X509KeyPair(serviceCert, serviceKey)
	if err != nil {
		log.Fatalf("Failed to load access key and/or access certificate: %s", err)
	}
	caCertPool := x509.NewCertPool()
	if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
		log.Fatalf("Failed to parse CA certificate")
	}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS: &tls.Config{
			Certificates: []tls.Certificate{keypair},
			RootCAs:      caCertPool,
		},
	}

	serviceURI := os.Getenv("SERVICE_URI")
	if serviceURI == "" {
		log.Fatalf("Environment variable 'SERVICE_URI' not set")
	}
	topicName := os.Getenv("TOPIC_NAME")
	if topicName == "" {
		log.Fatalf("Environment variable 'TOPIC_NAME' not set")
	}

	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{serviceURI},
		Topic:   topicName,
		Dialer:  dialer,
	})
	defer producer.Close()

	// POST /publish-metrics publishes one network metrics event to the Kafka topic.
	http.HandleFunc("/publish-metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}

		var data NetworkMetrics
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, "Invalid request payload", http.StatusBadRequest)
			return
		}
		if data.ServiceId == "" {
			data.ServiceId = "SVC-10045"
		}
		if data.Timestamp == "" {
			data.Timestamp = time.Now().UTC().Format(time.RFC3339)
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			http.Error(w, "Failed to marshal JSON data", http.StatusInternalServerError)
			return
		}
		if err := producer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(data.ServiceId),
			Value: jsonData,
		}); err != nil {
			log.Printf("Failed to send message: %s", err)
			http.Error(w, "Failed to send message", http.StatusInternalServerError)
			return
		}

		log.Printf("Message sent: %s", jsonData)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "success",
			"message": "Network metrics event published",
			"values":  string(jsonData),
		})
	})

	log.Println("Server starting on port 9090...")
	log.Fatal(http.ListenAndServe(":9090", nil))
}
