# DROP TABLES

songplay_table_drop = "drop table if exists fact_songplays"
user_table_drop = "drop table if exists dim_users"
song_table_drop = "drop table if exists dim_songs"
artist_table_drop = "drop table if exists dim_artist"
time_table_drop = "drop table if exists dim_time"

# CREATE TABLES
user_table_create = (""" CREATE TABLE dim_users (
     user_id int primary key, 
     first_name varchar, 
     last_name varchar, 
     gender varchar, 
     level varchar not null)
""")

song_table_create = (""" CREATE TABLE dim_songs (
     song_id varchar primary key, 
     title varchar, 
     artist_id varchar, 
     year int, 
     duration float)
""")

artist_table_create = (""" CREATE TABLE dim_artist (
     artist_id varchar primary key, 
     name varchar, 
     location varchar, 
     latitude varchar, 
     longitude varchar)
""")

time_table_create = (""" CREATE TABLE dim_time (
     start_time varchar primary key, 
     hour float, 
     day int, 
     week int, 
     month int, 
     year int, 
     weekday int)
""")
songplay_table_create = (""" CREATE TABLE fact_songplays (
     songplays_id SERIAL primary key,
     start_time varchar REFERENCES dim_time, 
     user_id int REFERENCES dim_users, 
     level varchar, 
     song_id varchar REFERENCES dim_songs, 
     artist_id varchar REFERENCES dim_artist, 
     session_id int, 
     location varchar, 
     user_agent varchar)
""")

# INSERT RECORDS

songplay_table_insert = (""" INSERT INTO fact_songplays (start_time , user_id , level , song_id , artist_id , session_id , location , user_agent)
     VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""
INSERT INTO dim_users (user_id , first_name, last_name , gender, level) 
VALUES   (%s,%s,%s,%s,%s)
ON CONFLICT (user_id) 
DO UPDATE SET level  = excluded.level ;
""")

song_table_insert = ("""
INSERT INTO dim_songs (song_id , title, artist_id , year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) 
DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO dim_artist (artist_id , name , location , latitude , longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) 
DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO dim_time (start_time , hour , day, week , month , year , weekday )
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) 
DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT t1.song_id,t1.artist_id from dim_songs t1  join dim_artist t2 on t2.artist_id=t1.artist_id
where t1.title LIKE %s and t2.name LIKE %s and t1.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]