const express = require("express");
const sqlite3 = require("sqlite3").verbose();

const {connect, closeConnection} = require('./rabbitMQ.js');

const app = express();
const PORT = 3001;
const db = new sqlite3.Database('./database.sqlite');
db.serialize(() => {
    db.run('CREATE TABLE IF NOT EXISTS payment(id INTEGER PRIMARY KEY, amount INTEGER, productName TEXT, quantity INTEGER, status TEXT)');
})

// write a fucntion to insert the payment details into the database
function insertPaymentDetails(id, price, productName, quantity){
    try{
        
        db.run('INSERT INTO payment(id, amount, productName, quantity, status) VALUES(?, ?, ?, ?, ?)', [id, price, productName, quantity, 'completed']);
    }
    catch(ex){
        console.error('Error occured here: ',ex.message);
        if (err.code === 'SQLITE_CONSTRAINT') {
            // Handle constraint violation (e.g., unique constraint)
            console.error('Error: Unique constraint violation.');
        }
    }
}

async function consumeMessage(channel, queueName){
    try{
        await channel.consume(queueName, (message) => {
            if(message !== null){
            console.log(`Message received: ${message.content.toString()}`);
            const {productID, productName, quantity, price, paymentDetails} = JSON.parse(message.content.toString());
            insertPaymentDetails(parseInt(productID), parseInt(price)*parseInt(quantity), productName, parseInt(quantity));
            channel.ack(message);
            }
        });
    }
    catch(ex){
        console.error('Error at consume message: ',ex.message);
    }

}

let rabbitMQConnection, channel;
(async ()=>{
    let response = await connect();
    rabbitMQConnection = response[0];
    channel = response[1];
    consumeMessage(channel, 'payment_queue');
})();

app.get('/payment', async (req, res) => {
    // get the payment details from the database
    console.log('api is called from consumer nodejs')
    const {id} = req.query;
    if(!id){
        return res.status(400).json({message: 'Invalid request'});
    }
    db.get('SELECT * FROM payment WHERE id = ?', [id], (err, row) => {
        if(err){
            return res.status(500).json({message: 'Internal server error'});
        }
        if(!row){
            return res.status(404).json({message: 'Payment not found'});
        }
        res.status(200).json(row);
    })
})


const server = app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
})


// Handle cleanup when the server is being closed
process.on('SIGINT', async () => {
    try {
        // Close your RabbitMQ connection and perform other cleanup tasks
        // Assuming you have a closeConnection function in your rabbitmq.js file
        await closeConnection(rabbitMQConnection);
        db.close();
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

