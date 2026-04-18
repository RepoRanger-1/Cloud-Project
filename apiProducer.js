const express = require('express');
const kafka = require('kafka-node');

const app = express();
app.use(express.json());

const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const producer = new kafka.Producer(client);

producer.on('ready', () => {
    console.log("API Producer ready");
});

// 🔥 API endpoint
app.post('/event', (req, res) => {
    const event = req.body;

    producer.send([{
        topic: 'ecommerce-events',
        messages: JSON.stringify(event)
    }], (err, data) => {
        if (err) return res.status(500).send(err);
        res.send("Event sent to Kafka");
    });
});

app.listen(4000, () => {
    console.log("API running on port 4000");
});