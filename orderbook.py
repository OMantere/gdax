import json
import time
import gdax
import os, sys
from websocket import create_connection, WebSocketConnectionClosedException
from pymongo import MongoClient
from threading import Thread

url = os.getenv('MONGO_URL')
mongo_client = MongoClient(url)
products = ['BTC-USD']

class WSClient(object):
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.url = 'wss://ws-feed.gdax.com'
        self.products = products
        self.channels = ['full']
        self.msg_count = 0
        self.mongo_collection = mongo_collection=mongo_client.crypto_test.messages

    def _connect(self):
        sub = {'type': 'subscribe', 'product_ids': self.products, 'channels': self.channels}
        self.ws = create_connection(self.url)
        self.ws.send(json.dumps(sub))
 
    def _listen(self):
        print('Started message collection')
        while not self.stop:
            try:
                if int(time.time() % 30) == 0:
                    self.ws.ping('keepalive')
                data = self.ws.recv()
                msg = json.loads(data)
            except ValueError as e:
                self.on_error(e)
            except Exception as e:
                self.on_error(e)
            else:
                self.on_message(msg)

    def _disconnect(self):
        if self.type == "heartbeat":
            self.ws.send(json.dumps({"type": "heartbeat", "on": False}))
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            pass

    def start(self):
        def _go():
            self._connect()
            self._listen()
            self._disconnect()
        
        def _count_loop():
            while not self.stop:
                prev_count = self.msg_count
                time.sleep(1)
                print('%d MSG/s' % (self.msg_count - prev_count))

        self.stop = False
        self.thread = Thread(target=_go)
        self.count_thread = Thread(target=_count_loop)
        self.thread.start()
        self.count_thread.start()

    def close(self):
        self.stop = True
        self.thread.join()
        self.count_thread.join()

    def on_message(self, msg):
        if self.verbose:
            print(msg)
        if self.mongo_collection and msg['type'] != 'subscriptions':
            self.msg_count += 1
            self.mongo_collection.insert_one(msg)

    def on_error(self, e, data=None):
        self.error = e
        self.stop = True
        print('{} - data: {}'.format(e, data))
    
ws = WSClient()
ws.start()

pc = gdax.PublicClient()
order_books_collection = mongo_client.crypto_test.order_books
collect_ob = True

def get_ob():
    for product in products:
        ob = pc.get_product_order_book(product, level=3)
        order_books_collection.insert_one(ob)
        print('Synced OB for %s' % product)

def ob_loop():
    while collect_ob:
        if(int(time.time()) % 60 == 0):
            get_ob()

ob_thread = Thread(target=ob_loop)
ob_thread.start()

