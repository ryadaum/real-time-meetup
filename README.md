## Real time meetup using PySpark

![project map](https://user-images.githubusercontent.com/83231879/116277460-28678680-a78e-11eb-90fd-b8f67c243108.PNG)


**This project is a real-time Meetup message processing. The application as shown above aims to:**

1. Get Meetup events messages from meetup.com RSVP stream API.
2. Using Apache Kafka as messaging system (publish-subscribe) layer.
3. Ingest data from Kafka to Spark Structured Streaming processing engine to: 
   1.Transform data to the desired schema.
   2.Process data and make simple aggregation operations in real-time.

4. Load the transformed row data into Nosql database (MongoDB).
5. Load the processed aggregated data into Mysql database.
6. Visualize processed aggregated data in a PowerBI dashboard.(later on)

### PySpark ETL Project Structure

The first thing we need to do is inite a Zookeeper that will orchestrate the Kafka cluster work by runnig `01-start-zookeeper.cmd`. Then 
We will start a Kafka cluster by running `02-start-kafka.cmd`. After that we will run `03-create-meetuprsvp-topic.cmd` to create a Kafka Topic that will receive Meetup messages.
To start receiving our messages from Meetup.com api, we need to run `kafka_producer.py`, and this Python module will connect to Meetup api url (https://stream.meetup.com/2/rsvps) and create a while loop that will receive messages in Json format.

_Message Sample_:
```json
{"venue":{"venue_name":"Upper Mangatawhiri Reservoir","lon":175.15259,"lat":-37.08655,"venue_id":26930421},"visibility":"public","response":"yes","guests":0,"member":{"member_id":263633098,"photo":"https:\/\/secure.meetupstatic.com\/photos\/member\/3\/a\/9\/3\/thumb_303194995.jpeg","member_name":"Pretesh"},"rsvp_id":1861834284,"mtime":1611262042371,"event":{"event_name":"Hunua ranges, Puka Puka track loop (Intermediate level)","event_id":"275509941","time":1611345600000,"event_url":"https:\/\/www.meetup.com\/Auckland-Outdoors\/events\/275509941\/"},"group":{"group_topics":[{"urlkey":"camping","topic_name":"Camping"},{"urlkey":"hiking","topic_name":"Hiking"},{"urlkey":"outdoors","topic_name":"Outdoors"},{"urlkey":"outdoor-fitness","topic_name":"Outdoor  Fitness"},{"urlkey":"outdoor-adventures","topic_name":"Outdoor Adventures"},{"urlkey":"back-country-hiking","topic_name":"Back country hiking"},{"urlkey":"backpacking","topic_name":"Backpacking"},{"urlkey":"backpacking-and-camping","topic_name":"Backpacking and Camping"},{"urlkey":"wilderness-hiking","topic_name":"Wilderness Hiking"}],"group_city":"Auckland","group_country":"nz","group_id":21832687,"group_name":"Auckland Outdoors","group_lon":174.76,"group_urlname":"Auckland-Outdoors","group_lat":-36.85}}
```
So, will start `etl.py` and move these nested json messages and convert them to row formatted datafram and load it to MongoDB database for future analyses.
> Note: You need to add appropriate versions of Kafka-Spark structured streaming connector and mongodb-Spark structured streaming connector drivers to your Spark configuration file. Or at spark-submit stage.

Row schema:

```
root 

 |-- venue_name: string (nullable = true) 

 |-- lon: string (nullable = true) 

 |-- lat: string (nullable = true) 

 |-- venue_id: string (nullable = true) 

 |-- visibility: string (nullable = true) 

 |-- response: string (nullable = true) 

 |-- guests: string (nullable = true) 

 |-- member_id: string (nullable = true) 

 |-- photo: string (nullable = true) 

 |-- member_name: string (nullable = true) 

 |-- rsvp_id: string (nullable = true) 

 |-- mtime: string (nullable = true) 

 |-- event_name: string (nullable = true) 

 |-- event_id: string (nullable = true) 

 |-- time: string (nullable = true) 

 |-- event_url: string (nullable = true) 

 |-- urlkey: string (nullable = true) 

 |-- topic_name: string (nullable = true) 

 |-- group_city: string (nullable = true) 

 |-- group_country: string (nullable = true) 

 |-- group_id: string (nullable = true) 

 |-- group_name: string (nullable = true) 

 |-- group_lon: string (nullable = true) 

 |-- group_urlname: string (nullable = true) 

 |-- group_lat: string (nullable = true) 

 |-- event_time: timestamp (nullable = true) 
 ```
 Then will process the data an aggregate the responses for each group and load the processed data into Mysql database to create a real-time dashboard(later on)
