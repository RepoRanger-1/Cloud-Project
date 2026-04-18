const kafka = require('kafka-node');
const mongoose = require('mongoose');
const express = require('express');
const promClient = require('prom-client');

// --- 1. METRICS SETUP ---
const app = express();
const register = new promClient.Registry();
promClient.collectDefaultMetrics({ register });

const eventsProcessedCounter = new promClient.Counter({
    name: 'ecommerce_events_processed_total',
    help: 'Total events successfully read from Kafka'
});

const processingLatency = new promClient.Histogram({
    name: 'ecommerce_processing_delay_ms',
    help: 'Time difference between event creation and consumption (ms)',
    buckets: [10, 50, 100, 250, 500, 1000, 5000] // Categorizes delay speeds
});

register.registerMetric(eventsProcessedCounter);
register.registerMetric(processingLatency);

app.get('/metrics', async (req, res) => {
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
});
// Use 8081 so it doesn't clash with the producer
app.listen(8081, '0.0.0.0', () => console.log('Producer Metrics running on port 8081'));


// --- 2. YOUR EXISTING CONSUMER CODE ---
mongoose.connect('mongodb://mongodb:27017/ecommerce'); // Note: changed 127.0.0.1 to 'mongodb' for K8s compatibility

const eventSchema = new mongoose.Schema({
    user_id: String, event_type: String, product_id: String, timestamp: String, price: Number
});
const Event = mongoose.model('Event', eventSchema);

const client = new kafka.KafkaClient({ kafkaHost: 'kafka:9092' });
const consumer = new kafka.Consumer(client, [{ topic: 'ecommerce-events', partition: 0 }], { autoCommit: true });

consumer.on('message', async (message) => {
    const data = JSON.parse(message.value);

    // 🔥 TRACK PROCESSING DELAY (Current Time - Event Time)
    const eventTime = new Date(data.timestamp).getTime();
    const delayMs = Date.now() - eventTime;
    processingLatency.observe(delayMs); 
    
    // 🔥 TRACK PROCESSING COUNT
    eventsProcessedCounter.inc();

    console.log(`Processed ${data.event_type} | Delay: ${delayMs}ms`);

    const newEvent = new Event(data);
    await newEvent.save();
});

consumer.on('error', (err) => console.error('Consumer error:', err));