from flask import Flask, jsonify
import sqlite3
import json
import pika
import threading
import concurrent.futures


app = Flask(__name__)



def consumeMessage():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672))
        channel = connection.channel()
        channel.queue_declare(queue='shipping_queue', durable=True)

        conn = sqlite3.connect('database.sqlite')
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS shipping (id INTEGER PRIMARY KEY, amount INTEGER, productName TEXT, quantity INTEGER, status TEXT)')
        conn.commit()

        def callback(ch, method, properties, body):
            try:
                print(" [x] Received %r" % body)
                shipping = json.loads(body)

                # Insert data into SQLite database
                cursor.execute('INSERT INTO shipping (id, amount, productName, quantity, status) VALUES (?, ?, ?, ?, ?)',
                               (shipping['productID'], int(shipping['price']) * int(shipping['quantity']),
                                shipping['productName'], int(shipping['quantity']), 'confirmed'))
                conn.commit()

                #ch.basic_ack(delivery_tag=method.delivery_tag)
            except json.JSONDecodeError as json_error:
                print(f"JSON Decode Error: {json_error}")
            except sqlite3.Error as sqlite_error:
                print(f"SQLite Error: {sqlite_error}")
            except Exception as e:
                print(f"Unexpected Error: {e}")

        channel.basic_consume(queue='shipping_queue', on_message_callback=callback, auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError as connection_error:
        print(f"AMQP Connection Error: {connection_error}")
    except pika.exceptions.AMQPChannelError as channel_error:
        print(f"AMQP Channel Error: {channel_error}")
    except Exception as e:
        print(f"Unexpected Error: {e}")

def start_consuming():
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     executor.submit(consumeMessage)
    thread = threading.Thread(target=consumeMessage)
    thread.start()

@app.route('/shipping/<int:id>', methods=['GET'])
def getShippingDetails(id):
    try:
        print('hi')
        conn = sqlite3.connect('database.sqlite')
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM shipping WHERE id=?', (id,))
        shipping = cursor.fetchone()
        if shipping:
            return jsonify(shipping)
        else:
            return jsonify({'error': 'Shipping not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # starting the consumer in different thread or process
    # consumer_thread = threading.Thread(target=consumeMessage)
    # consumer_thread.start()
    start_consuming()

    #starting the flask app
    app.run(debug=True, port=3002)