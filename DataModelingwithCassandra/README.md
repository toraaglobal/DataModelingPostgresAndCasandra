
## Data Modeling with Apache Cassandra


```python
__name__ = 'Tajudeen Abdulazeez'
__email__='tabdulazeez99@gmail.com'
__program__ = 'Udacity Data Engineer Nano degree project 2'
```

### Project Description
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.


#### Project Deliverable
Apache Cassandra database to answer some of there business question.

#### Source Data
The source is in a fileformat and it is partition by date.
Here are examples of filepaths to two files in the dataset:

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv

```

### Import Packages


```python
# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
```

### ETL Pipeline for Pre-Processing the Files


```python
# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    print(file_path_list)
```

    /home/workspace
    ['/home/workspace/event_data/2018-11-30-events.csv', '/home/workspace/event_data/2018-11-23-events.csv', '/home/workspace/event_data/2018-11-22-events.csv', '/home/workspace/event_data/2018-11-29-events.csv', '/home/workspace/event_data/2018-11-11-events.csv', '/home/workspace/event_data/2018-11-14-events.csv', '/home/workspace/event_data/2018-11-20-events.csv', '/home/workspace/event_data/2018-11-15-events.csv', '/home/workspace/event_data/2018-11-05-events.csv', '/home/workspace/event_data/2018-11-28-events.csv', '/home/workspace/event_data/2018-11-25-events.csv', '/home/workspace/event_data/2018-11-16-events.csv', '/home/workspace/event_data/2018-11-18-events.csv', '/home/workspace/event_data/2018-11-24-events.csv', '/home/workspace/event_data/2018-11-04-events.csv', '/home/workspace/event_data/2018-11-19-events.csv', '/home/workspace/event_data/2018-11-26-events.csv', '/home/workspace/event_data/2018-11-12-events.csv', '/home/workspace/event_data/2018-11-27-events.csv', '/home/workspace/event_data/2018-11-06-events.csv', '/home/workspace/event_data/2018-11-09-events.csv', '/home/workspace/event_data/2018-11-03-events.csv', '/home/workspace/event_data/2018-11-21-events.csv', '/home/workspace/event_data/2018-11-07-events.csv', '/home/workspace/event_data/2018-11-01-events.csv', '/home/workspace/event_data/2018-11-13-events.csv', '/home/workspace/event_data/2018-11-17-events.csv', '/home/workspace/event_data/2018-11-08-events.csv', '/home/workspace/event_data/2018-11-10-events.csv', '/home/workspace/event_data/2018-11-02-events.csv']


### Processing the files to create the data file csv that will be used for Apache Casssandra tables


```python
# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
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
# check the number of rows in your csv file
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

### Creating a Cluster


```python
# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()
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

### Query that return a artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4


```python
## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4

query = "CREATE TABLE IF NOT EXISTS artist"
query = query + "(sessionId int, artist text, itemInSession int,length text,song text,  PRIMARY KEY (sessionId, itemInSession)) "
try:
    session.execute(query)
except Exception as e:
    print(e)
```


```python
# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO artist (sessionId,artist, itemInSession,length ,song)"
        query = query + "VALUES  (%s, %s, %s, %s, %s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        #print(line[0])
        #print(line[3])
        #print(line[5])
        #print(line[8])
        #print(line[9])
        session.execute(query, (int(line[8]),line[0], int(line[3]),line[5], line[9]))
```

#### Do a SELECT to verify that the data have been inserted into each table


```python
## TO-DO: Add in the SELECT statement to verify the data was entered into the table
query = "select * from artist  WHERE sessionId=338 and itemInSession = 4 "
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


### Query that return name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182


```python
## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182

query = "CREATE TABLE IF NOT EXISTS artistname"
query = query + "(userId int,artist text, itemInSession int,sessionId int,song text,firstName text, lastName text,   PRIMARY KEY (userId,sessionId, itemInSession,firstName,lastName)) "
try:
    session.execute(query)
except Exception as e:
    print(e)

                   
# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO artistname (userId,artist , itemInSession ,sessionId ,song  ,firstName , lastName)"
        query = query + "VALUES  (%s, %s, %s, %s, %s,%s,%s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        #print(line[0])
        #print(line[3])
        #print(line[5])
        #print(line[8])
        #print(line[9])
        session.execute(query, (int(line[10]),line[0], int(line[3]),int(line[8]), line[9], line[1], line[4]))    
        
```


```python
## TO-DO: Add in the SELECT statement to verify the data was entered into the table
query = "select * from artistname  WHERE userId=10 and sessionId=182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row)
```

    Row(userid=10, sessionid=182, iteminsession=0, firstname='Sylvie', lastname='Cruz', artist='Down To The Bone', song="Keep On Keepin' On")
    Row(userid=10, sessionid=182, iteminsession=1, firstname='Sylvie', lastname='Cruz', artist='Three Drives', song='Greece 2000')
    Row(userid=10, sessionid=182, iteminsession=2, firstname='Sylvie', lastname='Cruz', artist='Sebastien Tellier', song='Kilometer')
    Row(userid=10, sessionid=182, iteminsession=3, firstname='Sylvie', lastname='Cruz', artist='Lonnie Gordon', song='Catch You Baby (Steve Pitron & Max Sanna Radio Edit)')


### Query that return user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'


```python
## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

query = "CREATE TABLE IF NOT EXISTS username"
query = query + "(song text,firstName text, lastName text,   PRIMARY KEY (song)) "
try:
    session.execute(query)
except Exception as e:
    print(e)

                   
# We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO username (song,firstName , lastName)"
        query = query + "VALUES  ( %s,%s,%s)"
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        #print(line[0])
        #print(line[3])
        #print(line[5])
        #print(line[8])
        #print(line[9])
        session.execute(query, (line[9], line[1], line[4]))    
        
```


```python
## TO-DO: Add in the SELECT statement to verify the data was entered into the table
query = "select * from username  WHERE song = 'All Hands Against His Own' "
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row)

```

    Row(song='All Hands Against His Own', firstname='Sara', lastname='Johnson')


### Drop the tables before closing out the sessions


```python
## TO-DO: Drop the table before closing out the sessions
query = "drop table artist"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
    
query = "drop table artistname"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
query = "drop table username"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)

```

    Error from server: code=2200 [Invalid query] message="unconfigured table artistname"


### Close the session and cluster connection¶


```python
session.shutdown()
cluster.shutdown()
```


```python

```
