const kafka = require('kafka-node');
const mongoose = require('mongoose');
const express = require('express');
const promClient = require('prom-client');

const KAFKA = process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092';
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017/ecommerce';
const KAFKA_CONSUMER_GROUP = process.env.KAFKA_CONSUMER_GROUP || 'ecommerce-consumer-group';

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
    buckets: [10, 50, 100, 250, 500, 1000, 5000]
});

register.registerMetric(eventsProcessedCounter);
register.registerMetric(processingLatency);

app.get('/metrics', async (req, res) => {
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
});
app.listen(8081, '0.0.0.0', () => console.log('Consumer metrics on :8081 (Kafka:', KAFKA + ', group:', KAFKA_CONSUMER_GROUP + ')'));

mongoose.connect(MONGODB_URI);

const eventSchema = new mongoose.Schema({
    user_id: String, event_type: String, product_id: String, timestamp: String, price: Number
});
const Event = mongoose.model('Event', eventSchema);

const consumer = new kafka.ConsumerGroup({
    kafkaHost: KAFKA,
    groupId: KAFKA_CONSUMER_GROUP,
    autoCommit: true,
    fromOffset: 'latest',
    protocol: ['roundrobin'],
    encoding: 'utf8'
}, ['ecommerce-events']);

consumer.on('message', async (message) => {
    const data = JSON.parse(message.value);

    const eventTime = new Date(data.timestamp).getTime();
    const delayMs = Date.now() - eventTime;
    processingLatency.observe(delayMs);

    eventsProcessedCounter.inc();

    console.log(`Processed ${data.event_type} | Delay: ${delayMs}ms`);

    const newEvent = new Event(data);
    await newEvent.save();
});

consumer.on('error', (err) => console.error('Consumer error:', err));
