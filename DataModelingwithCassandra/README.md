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
