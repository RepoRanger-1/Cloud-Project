const kafka = require('kafka-node');
const express = require('express');
const promClient = require('prom-client');

// --- 1. METRICS SETUP ---
const app = express();
const register = new promClient.Registry();
promClient.collectDefaultMetrics({ register }); // Free CPU/Memory tracking

// Define exactly what we are tracking
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

// Host the metrics page on port 8080
app.get('/metrics', async (req, res) => {
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
});
app.listen(8080, '0.0.0.0', () => console.log('Producer Metrics running on port 8080'));


const client = new kafka.KafkaClient({ kafkaHost: 'kafka:9092' });
const producer = new kafka.Producer(client);

producer.on('ready', () => {
    console.log('Producer is ready');
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
    }, 1000);
});

producer.on('error', (err) => {
    console.error('Producer error:', err);
    eventsFailedCounter.inc();
});