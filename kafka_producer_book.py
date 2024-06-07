import random
from kafka import KafkaProducer
import mysql.connector
import time

# connect to the MySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="reddit"
)

# create a cursor to execute SQL queries
cursor = mydb.cursor()

# initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# continuously publish new tweets and hashtags to Kafka
while True:
    try:
        # select a random number of tweets to retrieve
       
        
        #----------------------------------------------------------------------------------

        # select all tweets that were added since the last time the loop ran
        book_score = "SELECT score from mytable"
        cursor.execute(book_score)
        # print the SQL query being executed
        print("Executing SQL query for score...")
        # publish each new tweet to the Kafka topic
        for score in cursor:
            
            message = bytes(f"{score}", encoding='utf-8')
        
            # print the data being processed
            print("Publishing score:", score)
        
            # publish the message to the Kafka topic
            producer.send('score', value=message)
            #time.sleep(1)
        #----------------------------------------------------------------------------------

        # select all tweets that were added since the last time the loop ran
        book_author = "SELECT author from mytable"
        cursor.execute(book_author)
        # print the SQL query being executed
        print("Executing SQL query for score...")
        # publish each new tweet to the Kafka topic
        for author in cursor:
            
            message = bytes(f"{author}", encoding='utf-8')
        
            # print the data being processed
            print("Publishing author:", author)
        
            # publish the message to the Kafka topic
            producer.send('author', value=message)
            #time.sleep(1)
        #----------------------------------------------------------------------------------
         # select all tweets that were added since the last time the loop ran
        book_awards = "SELECT total_awards_received from mytable"
        cursor.execute(book_awards)
        # print the SQL query being executed
        print("Executing SQL query for score...")
        # publish each new tweet to the Kafka topic
        for awards in cursor:
            
            message = bytes(f"{awards}", encoding='utf-8')
        
            # print the data being processed
            print("Publishing awards:", awards)
        
            # publish the message to the Kafka topic
            producer.send('awards', value=message)
            #time.sleep(1)
        

       
    except Exception as e:
        print(e)
        # in case of any error, sleep for a shorter time and try again
        time.sleep(2)
