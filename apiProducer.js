const express = require('express');
const kafka = require('kafka-node');

const KAFKA = process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092';
const PORT = Number(process.env.PORT) || 4000;

const app = express();
app.use(express.json());

const client = new kafka.KafkaClient({ kafkaHost: KAFKA });
const producer = new kafka.Producer(client);

producer.on('ready', () => {
    console.log('API Producer ready (Kafka:', KAFKA + ')');
});

app.post('/event', (req, res) => {
    const event = req.body;

    producer.send([{
        topic: 'ecommerce-events',
        messages: JSON.stringify(event)
    }], (err, data) => {
        if (err) return res.status(500).send(err);
        res.send('Event sent to Kafka');
    });
});

app.listen(PORT, '0.0.0.0', () => {
    console.log('API running on port', PORT);
});
