import pandas as pd


def decode_timestamp(df_timestamp):
    """Get a dataframe with decoded time information (hour,day,week,..) from raw timestamp
    Args:
        df_timestamp(pd.DataFrame): dataframe with timestamp only

    Returns:
        pd.DataFrame: dataframe with columns=["startime","hour","week","month","year","weekday"]
    """
    # convert to datetime
    df_datetime = pd.to_datetime(df_timestamp["ts"], unit='ms').to_frame(name="startime")
    # add explicit time information
    df_datetime['hour'] = df_datetime["startime"].dt.hour
    df_datetime['day'] = df_datetime["startime"].dt.day
    df_datetime['week'] = df_datetime["startime"].dt.week
    df_datetime['month'] = df_datetime["startime"].dt.month
    df_datetime['year'] = df_datetime["startime"].dt.year
    df_datetime['weekday'] = df_datetime["startime"].dt.weekday
    # convert timestamp object into string, ready to be insert into the database
    df_datetime['startime'] = df_datetime['startime'].astype(str)
    return df_datetime


def clean_and_transform_data_songs(df_raw_songs):
    """Clean raw songs data and extract songs and artists dataframes
    Args:
        df_raw_songs(pd.DataFrame): raw songs data

    Returns:
        tuple=[pd.DataFrame,pd.DataFrame,pd.DataFrame]
    """
    # fix empty data
    df_raw_songs.loc[df_raw_songs["artist_location"] == "", "artist_location"] = None

    # columns of interest in the raw datasets
    COLUMNS_SONGS = ["song_id", "title", "artist_id", "year", "duration"]
    COLUMNS_ARTISTS = ["artist_id", "artist_name", "artist_location",
                       "artist_latitude", "artist_longitude"]

    # get subsets of interest: df_songs=f(song_id) and df_artists=f(artist_id)
    df_songs = df_raw_songs.loc[:, COLUMNS_SONGS]
    df_artists = df_raw_songs.loc[:, COLUMNS_ARTISTS]
    return df_songs, df_artists


def clean_and_transform_data_log(df_raw_log):
    """Clean raw log data and extract users and datetime dataframes
    Args:
        df_raw_log(pd.DataFrame): raw log data

    Returns:
        tuple=[pd.DataFrame,pd.DataFrame,pd.DataFrame]
    """
    # select only actions when users went to next song - drawback: have to store and return a copy
    df_clean_log = df_raw_log.loc[df_raw_log["page"] == "NextSong", :].reset_index(drop=True)
    # fix empty data
    df_clean_log.loc[df_clean_log.loc[:, "userId"].astype(str) == "", "userId"] = None

    # columns of interest in the raw datasets
    COLUMNS_TIME = ["ts"]
    COLUMNS_USERS = ["userId", "firstName", "lastName", "gender", "level"]

    # get subsets of interest: df_time=f(ts) and df_users=f(user_id)
    df_users = df_clean_log.loc[:, COLUMNS_USERS]
    df_time = df_clean_log.loc[:, COLUMNS_TIME]
    df_datetime = decode_timestamp(df_time)

    return df_clean_log, df_users, df_datetime
