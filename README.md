# Data Modeling with Postgres
### Project Description
A startup called Sparkily wants to analyze the data they have been collecting on songs and user activity on their music streaming app. The analytics team is particularly interested in understanding what songs user are listening to.Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Project Delivrables
A dimensional model using star schema will be design and implemented with postgresSQL.
The database model will be optimized  for query performance, Sparkify company can perform ad hoc query to answer different business question.
Develop ETL pipeline to load the data into the target database from the file systems and logs using `python` and `sql`
it has to be no-violatile, integrated and time variant.

### Source Data
Sparkily has two different source of data:
1. Song dataset :  Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```


1. log dataset : The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

### Schema for the Song Play Analysis
![Schema]('https://github.com/toraaglobal/DataModelingPostgresAndCasandra/blob/master/songplayschema.jpg')

### Example Query
```
SELECT t1.user_agent,t2.firstname, t2.last_name, t3.title, t3.duration,t4.name as artist_name
from songplays t1 join users t2 on t1.user_id=t2.user_id
join songs t3 on t3.song_id = t1.song_id
join artist t4 on t4.artist_id = t1.artist_id
```


# Data Modeling with Cassandra
### Project Description
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

### Project Deliverable
Apache Cassandra database to answer some of there business question.

### Source Data
The source is in a fileformat and it is partition by date.
Here are examples of filepaths to two files in the dataset:

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv

```

### Example query
The Cansandra should be able to answer some of the following query

1. Query : Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)  for userid = 10, sessionid = 182
1.  Query : Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

