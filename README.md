# Network Metrics Producer

Demo producer for the "Self-Service Access to Network Events" scenario.
Publishes fictional network performance metrics events for SVC-10045 to a Kafka topic.

## Endpoint
POST /publish-metrics (port 9090)

```json
{"serviceId":"SVC-10045","bytesIn":48213377,"bytesOut":39117204,"droppedPackets":12,"fcsErrors":3}
```
`serviceId` defaults to SVC-10045 and `timestamp` to the current UTC time if omitted.

## Configuration (WSO2 Developer Platform)
File mounts: /service.cert, /service.key, /ca.pem (from the Kafka service Overview tab)
Environment variables: SERVICE_URI, TOPIC_NAME (network-performance-metrics)

All data is fictional demo data.
