# DROP TABLES

songplay_table_drop = "drop table if exists fact_songplays"
user_table_drop = "drop table if exists dim_users"
song_table_drop = "drop table if exists dim_songs"
artist_table_drop = "drop table if exists dim_artist"
time_table_drop = "drop table if exists dim_time"

# CREATE TABLES

songplay_table_create = (""" CREATE TABLE fact_songplays (start_time varchar, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar)
""")

user_table_create = (""" CREATE TABLE dim_users (user_id int, first_name varchar, last_name varchar, gender varchar, level varchar)
""")

song_table_create = (""" CREATE TABLE dim_songs (song_id varchar, title varchar, artist_id varchar, year int, duration float)
""")

artist_table_create = (""" CREATE TABLE dim_artist (artist_id varchar, name varchar, location varchar, latitude varchar, longitude varchar)
""")

time_table_create = (""" CREATE TABLE dim_time (start_time varchar, hour float, day int, week int, month int, year int, weekday int)
""")

# INSERT RECORDS

songplay_table_insert = (""" INSERT INTO fact_songplays (start_time , user_id , level , song_id , artist_id , session_id , location , user_agent)
     VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""
INSERT INTO dim_users (user_id , first_name, last_name , gender, level)  VALUES   (%s,%s,%s,%s,%s)
""")

song_table_insert = ("""
INSERT INTO dim_songs (song_id , title, artist_id , year, duration)
VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
INSERT INTO dim_artist (artist_id , name , location , latitude , longitude)
VALUES (%s, %s, %s, %s, %s)
""")


time_table_insert = ("""
INSERT INTO dim_time (start_time , hour , day, week , month , year , weekday )
VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
SELECT t1.song_id,t1.artist_id from dim_songs t1  join dim_artist t2 on t2.artist_id=t1.artist_id
where t1.title = %s and t2.name= %s and t1.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]