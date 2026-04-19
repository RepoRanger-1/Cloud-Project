const kafka = require('kafka-node');
const express = require('express');
const promClient = require('prom-client');

const KAFKA = process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092';
const EVENT_INTERVAL_MS = Number(process.env.EVENT_INTERVAL_MS || 1000);

// --- 1. METRICS SETUP ---
const app = express();
const register = new promClient.Registry();
promClient.collectDefaultMetrics({ register });

const eventsSentCounter = new promClient.Counter({
    name: 'ecommerce_events_sent_total',
    help: 'Total events successfully sent to Kafka'
});
const eventsFailedCounter = new promClient.Counter({
    name: 'ecommerce_events_failed_total',
    help: 'Total events that failed to send'
});

register.registerMetric(eventsSentCounter);
register.registerMetric(eventsFailedCounter);

app.get('/metrics', async (req, res) => {
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
});
app.listen(8080, '0.0.0.0', () => console.log('Producer metrics on :8080'));

const client = new kafka.KafkaClient({ kafkaHost: KAFKA });
const producer = new kafka.Producer(client);

producer.on('ready', () => {
    console.log('Producer is ready (Kafka:', KAFKA + ')');
    const admin = new kafka.Admin(client);
    admin.createTopics([{ topic: 'ecommerce-events', partitions: 1, replicationFactor: 1 }], (err, res) => {});

    const eventTypes = ["view_product", "add_to_cart", "purchase"];

    setInterval(() => {
        const event = {
            user_id: "U" + Math.floor(Math.random() * 100),
            event_type: eventTypes[Math.floor(Math.random() * eventTypes.length)],
            product_id: "P" + Math.floor(Math.random() * 50),
            timestamp: new Date().toISOString(),
            price: Math.floor(Math.random() * 1000)
        };

        producer.send([{ topic: 'ecommerce-events', messages: JSON.stringify(event) }], (err, data) => {
            if (err) {
                console.error('Error:', err);
                eventsFailedCounter.inc();
            } else {
                console.log('Sent:', event.event_type);
                eventsSentCounter.inc();
            }
        });
    }, EVENT_INTERVAL_MS*0.5);
});

producer.on('error', (err) => {
    console.error('Producer error:', err);
    eventsFailedCounter.inc();
});
