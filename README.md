# Network Metrics Producer

Based on the WSO2 kafka-sample-temperature-producer. Publishes fictional network performance metrics to a Kafka topic.

POST http://localhost:9090/publish-metrics

```json
{"serviceId":"SVC-10045","bytesIn":48213377,"bytesOut":39117204,"droppedPackets":12,"fcsErrors":3}
```
`timestamp` defaults to the current UTC time if omitted.

Configs: file mounts /service.cert, /service.key, /ca.pem; env vars SERVICE_URI, TOPIC_NAME.
