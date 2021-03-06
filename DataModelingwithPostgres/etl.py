import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """process song file

    Arguments:
        cur {cursor} -- database cursor
        filepath {path} -- filepath to the songs file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    tmp = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = list(tmp.values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    tmp2 = df[['artist_id','artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = list(tmp2.values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """process log file

    Arguments:
        cur {database cursor} -- database cursor
        filepath {filepath to the log} -- log file path to process
    """
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']
    df['timestamp'] = pd.to_datetime(df['ts'], unit='ms').dt.time

    # convert timestamp column to datetime
    t = df[['ts']]
    t['tsdatetime'] = pd.to_datetime(df['ts'], unit='ms')
    t['timestamp'] = pd.to_datetime(df['ts'],  unit='ms').dt.time
    t['hour'] = pd.to_datetime(df['ts'],  unit='ms').dt.hour
    t['day'] = pd.to_datetime(df['ts'],  unit='ms').dt.day
    t['weekofyear'] = pd.to_datetime(df['ts'],  unit='ms').dt.weekofyear
    t['month'] = pd.to_datetime(df['ts'],  unit='ms').dt.month
    t['year'] = pd.to_datetime(df['ts'],  unit='ms').dt.year
    t['weekday'] = pd.to_datetime(df['ts'],  unit='ms').dt.weekday
    
    # insert time data records
    tmp = t[['timestamp', 'hour', 'day', 'weekofyear', 'month', 'year','weekday']]
    time_data = tmp.values
    column_labels =  ['timestamp', 'hour', 'day', 'weekofyear', 'month', 'year','weekday']
    time_df = t[column_labels]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName', 'lastName', 'gender', 'level' ]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        print(results)
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row['timestamp'], row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'],row['userAgent']]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """process file data to database

    Arguments:
        cur {database cursor} -- cursor to the target database
        conn {connection to the target database} -- connection to the target database
        filepath {filepath} -- the location of the data to process and insert into database
        func {function} -- function 
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=aotasa99")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()