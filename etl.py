import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


if __name__ == '__main__':

    # Read param.cfg file and assign all necessary parameters dynamically
    config = configparser.ConfigParser()
    config.read('param.cfg')

    master = config.get('SPARK', 'MASTER')
    mongoOutputUri = config.get('MONGODB', 'MONGO_OUTPUT')
    KafkaServer = config.get('KAFKA', 'KAFKA_SERVER')
    KafkaTopic = config.get('KAFKA', 'KAFKA_TOPIC')
    mysqlDbUrl = config.get('MYSQL', 'MYSQL_DB_URL')
    mysqlTbl = config.get('MYSQL', 'MYSQL_TBL')
    mysqlUsr = config.get('MYSQL', 'USERNAME')
    mysqlPws = config.get('MYSQL', 'PASSWORD')

    # Start a SparkSession
    spark = SparkSession \
        .builder \
        .appName('Meetup Stream Pipeline') \
        .master(master) \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()

    # Defining streaming message schema
    schema = StructType([
        StructField('venue', StructType([
            StructField('venue_name', StringType()),
            StructField('lon', StringType()),
            StructField('lat', StringType()),
            StructField('venue_id', StringType())
        ])),
        StructField('visibility', StringType()),
        StructField('response', StringType()),
        StructField('guests', StringType()),
        StructField('member', StructType([
            StructField('member_id', StringType()),
            StructField('photo', StringType()),
            StructField('member_name', StringType())
        ])),
        StructField('rsvp_id', StringType()),
        StructField('mtime', StringType()),
        StructField('event', StructType([
            StructField('event_name', StringType()),
            StructField('event_id', StringType()),
            StructField('time', StringType()),
            StructField('event_url', StringType())
        ])),
        StructField('group', StructType([
            StructField('group_topics', ArrayType(StructType([
                StructField('urlkey', StringType()),
                StructField('topic_name', StringType())
            ]))),

        StructField('group_city', StringType()),
        StructField('group_country', StringType()),
        StructField('group_id', StringType()),
        StructField('group_name', StringType()),
        StructField('group_lon', StringType()),
        StructField('group_urlname', StringType()),
        StructField('group_lat', StringType())
        ]))
    ])

    # Reading Kafka message
    kafka_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', KafkaServer) \
        .option('subscribe', KafkaTopic) \
        .option('startingOffsets', 'latest') \
        .load()

    # Get the (key, value) Kafka message, extract value and convert it to a dataframe with the defined schema
    value_df = kafka_df.select(from_json(col('value').cast('string'), schema).alias('value'),
                               col('timestamp').cast('TIMESTAMP').alias('event_time'))
    value_df = value_df.select('value.*', 'event_time')
    # value_df.printSchema()

    # Explode value_df and select the fields from it
    explode_df = value_df.selectExpr('venue.venue_name', 'venue.lon', 'venue.lat', 'venue.venue_id',
                                     'visibility', 'response', 'guests', 'member.member_id',
                                     'member.photo', 'member.member_name', 'rsvp_id', 'mtime',
                                     'event.event_name', 'event.event_id', 'event.time',
                                     'event.event_url',
                                     'explode(group.group_topics) as group_topics',
                                     'group.group_city', 'group.group_country', 'group.group_id',
                                     'group.group_name', 'group.group_lon', 'group.group_urlname',
                                     'group.group_lat', 'event_time')
    # Flatten group_topics
    flatten_df = explode_df \
        .withColumn('group_urlkey', expr('group_topics.urlkey')) \
        .withColumn('group_topic_name', expr('group_topics.topic_name')) \
        .drop("group_topics")

    # Find the response_count grouping by group_name, group_country, group_lat, group_lon, response
    response_count_df = flatten_df.groupBy('group_name', 'group_country', 'group_lat', 'group_lon',
                                           'response').agg(count(col('response')).alias('response_count'))

    # print('Schema of response_count_df:')
    # response_count_df.printSchema()

    # Write flatten_df into Mongodb using foreachBach() function

    # Create Mongodb writing query function
    def mongoSink(df, batch_id):
        df.write \
            .format('mongo') \
            .mode('append') \
            .option('spark.mongodb.output.uri', mongoOutputUri) \
            .save()

    write_df_mongodb = flatten_df.writeStream \
        .foreachBatch(mongoSink) \
        .outputMode('append') \
        .start()

    # Write response_count_df into Mysql using foreachBach() function

    # Create Mysql writing query function
    def mysqlSink(df, batch_id):
        df.write \
          .jdbc(url=mysqlDbUrl, table=mysqlTbl, properties={'user':mysqlUsr, 'password':mysqlPws})

    write_df_mysql = response_count_df.writeStream \
        .foreachBatch(mysqlSink) \
        .outputMode('append') \
        .trigger(processingTime='30 seconds') \
        .start()

    spark.streams.awaitAnyTermination()

    ## Display flatten_df on Console
    # write_df_console = flatten_df.writeStream \
    #     .format('console') \
    #     .outputMode('update') \
    #     .start()

    ## Display response_count_df on Console
    # write_response_console = response_count_df.writeStream \
    #     .format('console') \
    #     .outputMode('update') \
    #     .option('truncate', 'false') \
    #     .trigger(processingTime='30 seconds') \
    #     .start()

    # spark.streams.awaitAnyTermination()
