import sys
import psycopg2
import argparse
import pandas as pd

from postgres_etl_template.scripts import create_tables
from postgres_etl_template.src import utils_misc, utils_transform_data
from postgres_etl_template.src.sql_queries import song_table_insert, \
    artist_table_insert, \
    time_table_insert, \
    user_table_insert, \
    song_select, \
    songplay_table_insert


def process_song_file(cur, filepath):
    """Process and load into the database the song data"""
    # open and transform song file
    df_raw_songs = pd.read_json(filepath, lines=True)
    df_songs, df_artists = utils_transform_data.clean_and_transform_data_songs(df_raw_songs)

    # insert song record
    cur.execute(song_table_insert, df_songs.values[0].tolist())
    cur.execute(artist_table_insert, df_artists.values[0].tolist())


def process_log_file(cur, filepath):
    """Process and load into the database the log data"""
    # open log file
    df_raw_log = pd.read_json(filepath, lines=True)
    df_clean_log, df_users, df_datetime = utils_transform_data.clean_and_transform_data_log(df_raw_log)

    # insert time records
    for i, row in df_datetime.iterrows():
        cur.execute(time_table_insert, list(row))

    # insert user records
    for i, row in df_users.iterrows():
        cur.execute(user_table_insert, list(row))

    # insert songplay records
    for i, row in df_clean_log.iterrows():
        # get songid and artistid from song and artist table
        cur.execute(song_select, (row.song, row.artist, row.length))
        res = cur.fetchall()
        song_id, artist_id = res[0] if res else (None, None)

        # insert songplay record
        songplay_data = (
            str(pd.to_datetime(row.ts, unit='ms')),
            row.userId,
            row.level,
            song_id,
            artist_id,
            row.sessionId,
            row.location,
            row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Process and insert in the database each source of data"""
    # get all files matching extension from directory
    all_files = utils_misc.get_files(filepath)
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def parse_input(args):
    parser = argparse.ArgumentParser(description="Process and load raw data into a database")
    parser.add_argument("path_data_songs", help="Path where are stored the raw data of songs")
    parser.add_argument("path_data_logs", help="Path where are stored the raw data of logs")
    parser.add_argument("--reset-tables", help="Drop and create from scratch all database tables", action="store_true")
    return parser.parse_args(args)


def main(args=None):
    # read input from interface
    if args is None:
        args = sys.argv[1:]
    args = parse_input(args)

    if args.reset_tables:
        create_tables.main()

    # transform and load
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    process_data(cur, conn, filepath=args.path_data_songs, func=process_song_file)
    process_data(cur, conn, filepath=args.path_data_logs, func=process_log_file)
    conn.close()


if __name__ == "__main__":
    main()
