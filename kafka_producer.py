import configparser
from kafka import KafkaProducer
import time
import requests
import json

# Read param.cfg file and assign all necessary parameters dynamically
config = configparser.ConfigParser()
config.read('param.cfg')

meetup_resvp_stream_api_url = config.get('MEETUP', 'MEETUP_APU_URL') # Meetup Real-Time messages api url
KafkaTopic = config.get('KAFKA', 'KAFKA_TOPIC') # Topic Name
KafkaServer = config.get('KAFKA', 'KAFKA_SERVER') # Kafka server address

if __name__ == "__main__":
    print('Kafka Producer Application Started...')

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KafkaServer, value_serializer=
                                       lambda x: json.dumps(x).encode('utf-8'))
    print('Printing before while loop start...')

    while True:
        try:
            stream_get = requests.get(meetup_resvp_stream_api_url, stream=True)
            if stream_get.status_code == 200:
                for message in stream_get.iter_lines():
                    print('Message received: ')
                    print(message)
                    print(type(message))

                    message = json.loads(message)
                    print('Message to be sent: ')
                    print(message)
                    print(type(message))
                    kafka_producer_obj.send(KafkaTopic, message)
                    time.sleep(5)
        except Exception as ex:
            print('Connection to meetup stream api could not established due to ')
    print('Printing after while loop complete.')
