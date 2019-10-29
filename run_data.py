from data_genrator import basket_orders
import json
from google.cloud import pubsub_v1
import os
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("-e", "--env", required=True,help="local or gcp env")

args = vars(ap.parse_args())

if args['env'] != 'gcp':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './sa.json'

topic_name="store_events"
project_id ="yonis-sandbox-20180926"




batch_settings = pubsub_v1.types.BatchSettings(
    max_bytes=512000,
    max_latency=5,  # One second
    max_messages=500
)

pubsub_client = pubsub_v1.PublisherClient(batch_settings)
topic_path = pubsub_client.topic_path(project_id, topic_name)

while True:
    bask = basket_orders()
    basket_rows = bask.basket_orders()
    for r in bask.basket:
        print r
        message_future = pubsub_client.publish(topic_path,data=r.encode('utf-8'))
    #msg_future = pubsub_v1.publisher.Client.publish("projects/yonis-sandbox-20180926/topics/store_events", data=r.encode('utf-8'))
    #message_future = pubsub_v1.publisher.Client.publish() pubsub_client.publish(topic_path, data=r.encode('utf-8'))

    def callback(message_future):
        if message_future.exception(timeout=3):
            print('Publishing message on {} threw an Exception {}.'.format(topic_name, message_future.exception()))
        else:
            print(message_future.result())