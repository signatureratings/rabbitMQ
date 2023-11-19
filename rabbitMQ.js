const amqp = require('amqplib');

async function connect() {
    try {
        const connection = await startConnection();
        const channel = await createChannel(connection);
        const exchangeName2 = 'shipping_exchange';
        const exchangeName1 = 'payment_exchange';
        const exchangeType = 'direct';
        const routingKey1 = 'payment';
        const routingKey2 = 'shipping';
        const queueName1 = 'payment_queue';
        const queueName2 = 'shipping_queue';
        await createExchange(channel, exchangeName1, exchangeType);
        await createExchange(channel, exchangeName2, exchangeType);
        await createQueue(channel, queueName1, exchangeName1, routingKey1);
        await createQueue(channel, queueName2, exchangeName2, routingKey2);
        return [connection, channel];
    }
    catch (ex){
        console.error(ex.message);
    }
}

async function startConnection(){
    try{
        const amqpServer = "amqp://localhost:5672"
        const connection = await amqp.connect(amqpServer);
        console.log('Connection established successfully.');
        return connection;
    }
    catch(ex){
        console.error(ex.message);
    }
}

async function createChannel(connection){
    try{
        const channel = await connection.createChannel();
        console.log('Channel created successfully.');
        return channel;
    }
    catch(ex){
        console.error(ex.message);
    }
}

async function createExchange(channel, exchangeName, exchangeType){
    try{
        await channel.assertExchange(exchangeName, exchangeType, {durable: true});
        console.log(`Exchange '${exchangeName}' created with type '${exchangeType}'.`);
    }
    catch(ex){
        console.error(ex.message);
    }
}

async function createQueue(channel, queueName, exchangeName, routingKey){
    try{
        await channel.assertQueue(queueName, {durable: true});
        if(exchangeName && routingKey){
            await channel.bindQueue(queueName, exchangeName, routingKey);
        }
        console.log(`Queue '${queueName}' created${exchangeName ? ` and bound to exchange '${exchangeName}' with routing key '${routingKey}'` : ''}.`);
        return queueName;
    }
    catch(ex){
        console.error(ex.message);
    }
}

async function closeConnection(connection) {
    try {
        await connection.close();
        console.log('RabbitMQ connection closed.');
    } catch (ex) {
        console.error('Error closing RabbitMQ connection:', ex.message);
    }
}

async function publishMessage(channel, exchangeName, routingKey, message){
    try{
        await channel.publish(exchangeName, routingKey, Buffer.from(message));
    }
    catch(ex){
        console.error(ex.message);
    }
}

async function consumeMessage(channel, queueName){
    try{
        await channel.consume(queueName, message => {
            if(message !== null){
            console.log(`Message received: ${message.content.toString()}`);
            const {productID, productName, quantity, price, paymentDetails} = JSON.parse(message.content.toString());
            insertPaymentDetails(23, price*quantity, productName, quantity);
            channel.ack(message);
            }
        });
    }
    catch(ex){
        console.error(ex.message);
    }

}

module.exports = {
    connect,
    closeConnection,
    publishMessage,
    consumeMessage
}