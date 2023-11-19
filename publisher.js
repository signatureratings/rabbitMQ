const express = require('express');
const app = express();
const PORT = 3000;

const {connect, closeConnection, publishMessage} = require('./rabbitMQ.js');


app.post('/placeOrder', express.json(), async (req, res) => {
    try{
    const {productID, productName, quantity, price, paymentDetails, shippingDetails} = req.body;
    if(!productID || !productName || !quantity || !price || !paymentDetails || !shippingDetails){
        return res.status(400).json({message: 'Invalid request'});
    }
    // process the order
    await publishMessage(channel, 'payment_exchange', 'payment', JSON.stringify({productID, productName, quantity, price, paymentDetails}));
    await publishMessage(channel, 'shipping_exchange', 'shipping', JSON.stringify({productID, productName, quantity, price, shippingDetails}));
    res.status(200).json({message: 'Order placed successfully'});
    }
catch(err){
console.error(err.message);
res.status(500).json({message: 'Internal server error'});
}
});

var rabbitMQConnection, channel;
const server = app.listen(PORT, async () => {
    let response = await connect();
    rabbitMQConnection = response[0];
    channel = response[1];
    console.log(`Server listening on port ${PORT}`);
})

// Handle cleanup when the server is being closed
process.on('SIGINT', async () => {
    try {
        // Close your RabbitMQ connection and perform other cleanup tasks
        // Assuming you have a closeConnection function in your rabbitmq.js file
        await closeConnection(rabbitMQConnection);
        // Close the Express server
        server.close(() => {
            console.log('Express server closed.');
            process.exit(0);
        });
    } catch (ex) {
        console.error('Error during cleanup:', ex.message);
        process.exit(1);
    }
});