const mongoose = require('mongoose');

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017/ecommerce';

mongoose.connect(MONGODB_URI);

const eventSchema = new mongoose.Schema({
    user_id: String,
    event_type: String,
    product_id: String,
    timestamp: String,
    price: Number
});

const Event = mongoose.model('Event', eventSchema);

setInterval(async () => {
    console.log('\nRunning batch processing (Mongo stats from consumer collection `events`)...');

    const totalEvents = await Event.countDocuments();

    const purchases = await Event.countDocuments({
        event_type: "purchase"
    });

    const revenueData = await Event.aggregate([
        { $match: { event_type: "purchase" } },
        { $group: { _id: null, total: { $sum: "$price" } } }
    ]);

    const revenue = revenueData[0]?.total || 0;

    console.log('Batch stats:');
    console.log('Total Events:', totalEvents);
    console.log('Purchases:', purchases);
    console.log('Revenue:', revenue);

}, 5000);
