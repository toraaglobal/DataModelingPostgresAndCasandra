
## Data Modeling with Apache Cassandra

### Project Description
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.


#### Project Deliverable
Apache Cassandra is a free and open-source, distributed, wide column store, NoSQL database management system designed to handle large amounts of data across many commodity servers, providing high availability with no single point of failure. 
In order to create a cassandra data model for the startup companay, there is need to know how they intend to query the data.
A ist of query is provided by the startup company and the data mode will be design to answer those query effectively using apache cassandra.

#### Source Data
The source is in a fileformat and it is partition by date.
Here are examples of filepaths to two files in the dataset:

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv

```

## Data Pre-processing

Below  are the packages use for this project


```python
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
```

### Pre-Processing - Files


```python
def get_filepath(folderName:str):
    # checking your current working directory
    print(os.getcwd())
    # Get your current folder and subfolder event data
    filepath = os.getcwd() + '/' + folderName
    for root, dirs, files in os.walk(filepath):
        file_path_list = glob.glob(os.path.join(root,'*'))
        print(file_path_list)
    return file_path_list

file_path_list = get_filepath('event_data')
```

    /home/workspace
    ['/home/workspace/event_data/2018-11-30-events.csv', '/home/workspace/event_data/2018-11-23-events.csv', '/home/workspace/event_data/2018-11-22-events.csv', '/home/workspace/event_data/2018-11-29-events.csv', '/home/workspace/event_data/2018-11-11-events.csv', '/home/workspace/event_data/2018-11-14-events.csv', '/home/workspace/event_data/2018-11-20-events.csv', '/home/workspace/event_data/2018-11-15-events.csv', '/home/workspace/event_data/2018-11-05-events.csv', '/home/workspace/event_data/2018-11-28-events.csv', '/home/workspace/event_data/2018-11-25-events.csv', '/home/workspace/event_data/2018-11-16-events.csv', '/home/workspace/event_data/2018-11-18-events.csv', '/home/workspace/event_data/2018-11-24-events.csv', '/home/workspace/event_data/2018-11-04-events.csv', '/home/workspace/event_data/2018-11-19-events.csv', '/home/workspace/event_data/2018-11-26-events.csv', '/home/workspace/event_data/2018-11-12-events.csv', '/home/workspace/event_data/2018-11-27-events.csv', '/home/workspace/event_data/2018-11-06-events.csv', '/home/workspace/event_data/2018-11-09-events.csv', '/home/workspace/event_data/2018-11-03-events.csv', '/home/workspace/event_data/2018-11-21-events.csv', '/home/workspace/event_data/2018-11-07-events.csv', '/home/workspace/event_data/2018-11-01-events.csv', '/home/workspace/event_data/2018-11-13-events.csv', '/home/workspace/event_data/2018-11-17-events.csv', '/home/workspace/event_data/2018-11-08-events.csv', '/home/workspace/event_data/2018-11-10-events.csv', '/home/workspace/event_data/2018-11-02-events.csv']


### Processing - files 


```python
full_data_rows_list = [] # create an empty list
    
for f in file_path_list:
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
print(len(full_data_rows_list))
print(full_data_rows_list[:10])

```

    8056
    [['Stephen Lynch', 'Logged In', 'Jayden', 'M', '0', 'Bell', '182.85669', 'free', 'Dallas-Fort Worth-Arlington, TX', 'PUT', 'NextSong', '1.54099E+12', '829', "Jim Henson's Dead", '200', '1.54354E+12', '91'], ['Manowar', 'Logged In', 'Jacob', 'M', '0', 'Klein', '247.562', 'paid', 'Tampa-St. Petersburg-Clearwater, FL', 'PUT', 'NextSong', '1.54056E+12', '1049', 'Shell Shock', '200', '1.54354E+12', '73'], ['Morcheeba', 'Logged In', 'Jacob', 'M', '1', 'Klein', '257.41016', 'paid', 'Tampa-St. Petersburg-Clearwater, FL', 'PUT', 'NextSong', '1.54056E+12', '1049', 'Women Lose Weight (Feat: Slick Rick)', '200', '1.54354E+12', '73'], ['Maroon 5', 'Logged In', 'Jacob', 'M', '2', 'Klein', '231.23546', 'paid', 'Tampa-St. Petersburg-Clearwater, FL', 'PUT', 'NextSong', '1.54056E+12', '1049', "Won't Go Home Without You", '200', '1.54354E+12', '73'], ['Train', 'Logged In', 'Jacob', 'M', '3', 'Klein', '216.76363', 'paid', 'Tampa-St. Petersburg-Clearwater, FL', 'PUT', 'NextSong', '1.54056E+12', '1049', 'Hey_ Soul Sister', '200', '1.54354E+12', '73'], ['LMFAO', 'Logged In', 'Jacob', 'M', '4', 'Klein', '227.99628', 'paid', 'Tampa-St. Petersburg-Clearwater, FL', 'PUT', 'NextSong', '1.54056E+12', '1049', "I'm In Miami Bitch", '200', '1.54354E+12', '73'], ['DJ Dizzy', 'Logged In', 'Jacob', 'M', '5', 'Klein', '221.1522', 'paid', 'Tampa-St. Petersburg-Clearwater, FL', 'PUT', 'NextSong', '1.54056E+12', '1049', 'Sexy Bitch', '200', '1.54354E+12', '73'], ['Fish Go Deep & Tracey K', 'Logged In', 'Jacob', 'M', '6', 'Klein', '377.41669', 'paid', 'Tampa-St. Petersburg-Clearwater, FL', 'PUT', 'NextSong', '1.54056E+12', '1049', 'The Cure & The Cause (Dennis Ferrer Remix)', '200', '1.54354E+12', '73'], ['', 'Logged In', 'Alivia', 'F', '0', 'Terrell', '', 'free', 'Parkersburg-Vienna, WV', 'GET', 'Home', '1.54051E+12', '1070', '', '200', '1.54354E+12', '4'], ['M83', 'Logged In', 'Jacob', 'M', '7', 'Klein', '96.1824', 'paid', 'Tampa-St. Petersburg-Clearwater, FL', 'PUT', 'NextSong', '1.54056E+12', '1049', 'Staring At Me', '200', '1.54354E+12', '73']]



```python

csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)


with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

```


```python
# number of rows in the file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))
```

    6821


The event_datafile_new.csv contains the following columns: 
- artist 
- firstName of user
- gender of user
- item number in session
- last name of user
- length of the song
- level (paid or free song)
- location of the user
- sessionId
- song title
- userId

The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>

<img src="images/image_event_datafile_new.jpg">

### Connect to  cassandra Cluster


```python
from cassandra.cluster import Cluster
try:
    cluster = Cluster()
    session = cluster.connect()
except Exception as e:
    print(str(e))
```

### Create Keyspace


```python
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)
```

### Set Keyspace


```python
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)
```

### Model 1
Our first mode is to design a tabe that answer the query that returns artist, soung tite, song lenght within a session id and that are in an iteminSession.

Example illutration query below:


**Query that return a artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4**

#### Below is a table name `artist` model to answer the query above


```python
query = "CREATE TABLE IF NOT EXISTS artist"
query = query + "(sessionId int, itemInSession int, artist text,length text,song text,  PRIMARY KEY (sessionId, itemInSession)) "

try:
    session.execute(query)
    print("query: {}".format(query))
except Exception as e:
    print(e)
```

    query: CREATE TABLE IF NOT EXISTS artist(sessionId int, itemInSession int, artist text,length text,song text,  PRIMARY KEY (sessionId, itemInSession)) 



```python
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO artist (sessionId,itemInSession,artist, length ,song)"
        query = query + "VALUES  (%s, %s, %s, %s, %s)"
        session.execute(query, (int(line[8]),int(line[3]),line[0] ,line[5], line[9]))
```

### Model 1 validation

Below is a select query verified the data model and ensured there is data in the table named `artist`


```python
query = "select sessionId ,  itemInSession, artist,length,song from artist  WHERE sessionId=338 and itemInSession = 4 "
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row)
    print (row.artist, row.length, row.song)
```

    Row(sessionid=338, iteminsession=4, artist='Faithless', length='495.3073', song='Music Matters (Mark Knight Dub)')
    Faithless 495.3073 Music Matters (Mark Knight Dub)


### Model 2
**Query that return name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182.**

Example illutration query below:



#### below is the table created named `artistname`


```python
query = "CREATE TABLE IF NOT EXISTS artistname"
query = query + """
               (userId int,sessionId int,itemInSession int,artist text,song text,firstName text, lastName text,
                PRIMARY KEY ( (userId, sessionId),itemInSession))  WITH CLUSTERING ORDER BY (itemInSession ASC)
                """
try:
    session.execute(query)
    print(query)
except Exception as e:
    print(e)

 
```

    CREATE TABLE IF NOT EXISTS artistname
                   (userId int,sessionId int,itemInSession int,artist text,song text,firstName text, lastName text,
                    PRIMARY KEY ( (userId, sessionId),itemInSession))  WITH CLUSTERING ORDER BY (itemInSession ASC)
                    



```python
                  
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO artistname (userId, sessionId,  itemInSession ,artist  ,song  ,firstName , lastName)"
        query = query + "VALUES  (%s, %s, %s, %s, %s,%s,%s)"
        session.execute(query, (int(line[10]),int(line[8]), int(line[3]),line[0], line[9], line[1], line[4]))    
        
```

#### Model 2 Validation

Below is a select query verified the data model and ensured there is data in the table named `artistname`


```python
query = """ 
       select userId ,sessionId, itemInSession ,artist ,song ,firstName , lastName 
       from artistname  WHERE userId=10 and sessionId=182  
       """ 
try:
    rows = session.execute(query)
    print('Query: {}'.format(query))
except Exception as e:
    print(e)
    
for row in rows:
    print(row)
```

    Query:  
           select userId ,sessionId, itemInSession ,artist ,song ,firstName , lastName 
           from artistname  WHERE userId=10 and sessionId=182  
           
    Row(userid=10, sessionid=182, iteminsession=0, artist='Down To The Bone', song="Keep On Keepin' On", firstname='Sylvie', lastname='Cruz')
    Row(userid=10, sessionid=182, iteminsession=1, artist='Three Drives', song='Greece 2000', firstname='Sylvie', lastname='Cruz')
    Row(userid=10, sessionid=182, iteminsession=2, artist='Sebastien Tellier', song='Kilometer', firstname='Sylvie', lastname='Cruz')
    Row(userid=10, sessionid=182, iteminsession=3, artist='Lonnie Gordon', song='Catch You Baby (Steve Pitron & Max Sanna Radio Edit)', firstname='Sylvie', lastname='Cruz')


### Model 3
**Query that return user name (first and last) in my music app history who listened to the song 'All Hands Against His Own**

Example illutration query below:




```python
query = "CREATE TABLE IF NOT EXISTS username"
query = query + "(song text,firstName text, lastName text,  PRIMARY KEY (song ,firstName , lastName)) "

try:
    session.execute(query)
except Exception as e:
    print(e)


```


```python
file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = "INSERT INTO username (song,firstName , lastName)"
        query = query + "VALUES  ( %s,%s,%s)"
        session.execute(query, (line[9], line[1], line[4]))   
```

#### Model 3 Validation

Below is a select query verified the data model and ensured there is data in the table named `username`


```python
query = "select song ,firstName , lastName from username  WHERE song = 'All Hands Against His Own' "
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row)

```

    Row(song='All Hands Against His Own', firstname='Jacqueline', lastname='Lynch')
    Row(song='All Hands Against His Own', firstname='Sara', lastname='Johnson')
    Row(song='All Hands Against His Own', firstname='Tegan', lastname='Levine')



```python
query = "drop table artist"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
```


```python
query = "drop table artistname"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
```


```python
query = "drop table username"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
```

### Close the session and cluster connection¶


```python
session.shutdown()
cluster.shutdown()
```

### conclusion
The following tables are created in cassandra `artist`, `artistname` and `username` to answer all the required query of the startup company.


```python

```
