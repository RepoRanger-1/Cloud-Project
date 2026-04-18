const fs = require('fs');
const kafka = require('kafka-node');

const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const producer = new kafka.Producer(client);

producer.on('ready', () => {
    console.log("Batch Producer ready");

    const data = JSON.parse(fs.readFileSync('events.json'));

    data.forEach(event => {
        producer.send([{
            topic: 'ecommerce-events',
            messages: JSON.stringify(event)
        }], (err) => {
            if (err) console.error(err);
        });
    });

    console.log("Batch upload completed");
});

producer.on('error', (err) => {
    console.error(err);
});