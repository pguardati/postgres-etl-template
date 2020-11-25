import os
import unittest
import pandas as pd

from postgres_etl_template.constants import DIR_DATA_TEST
from postgres_etl_template.src import utils_misc
from postgres_etl_template.src import utils_transform_data


class TestTransform(unittest.TestCase):
    """Test that the transformations computed prior the sql insertion give deterministic results"""
    def setUp(self):
        self.df_raw_songs = utils_misc.get_df_from_dir(os.path.join(DIR_DATA_TEST, "song_data"))
        self.df_raw_log = utils_misc.get_df_from_dir(os.path.join(DIR_DATA_TEST, "log_data"))

    def test_song_subset(self):
        df_songs, df_artists = utils_transform_data.clean_and_transform_data_songs(self.df_raw_songs)
        self.assertEqual(df_songs.values[0].tolist(),
                         ['SOZCTXZ12AB0182364', 'Setanta matins', 'AR5KOSW1187FB35FF4', 0, 269.58322])
        self.assertEqual(df_artists.values[0].tolist(),
                         ['AR5KOSW1187FB35FF4', 'Elena', 'Dubai UAE', 49.80388, 15.47491])

    def test_data_subset(self):
        df_clean_low, df_users, df_datetime = utils_transform_data.clean_and_transform_data_log(self.df_raw_log)
        self.assertEqual(df_users.values[0].tolist(),
                         [39.0, 'Walter', 'Frye', 'M', 'free'])
        self.assertEqual(['2018-11-01 20:57:10.796', 20, 1, 44, 11, 2018, 3],
                         df_datetime.values[0].tolist())

    def test_merge_subsets(self):
        # extract song and artist
        df_songs, df_artists = utils_transform_data.clean_and_transform_data_songs(self.df_raw_songs)
        df_clean_low, df_users, df_datetime = utils_transform_data.clean_and_transform_data_log(self.df_raw_log)
        # merge song and artist
        df_songs_and_artist = pd.merge(df_songs, df_artists, on="artist_id")
        # merge log and song_artist where (song, length and artist) are the same
        df_songplays = pd.merge(df_songs_and_artist, df_clean_low,
                                left_on=["title", "duration", "artist_name"],
                                right_on=["song", "length", "artist"])
        df_songplays = df_songplays[["ts", "userId", "level", "song_id", "artist_id", "sessionId",
                                     "artist_location", "userAgent"]]
        self.assertEqual(
            df_songplays.values[0].tolist(),
            [1541105830796,
             39,
             'free',
             'SOZCTXZ12AB0182364',
             'AR5KOSW1187FB35FF4',
             38,
             'Dubai UAE',
             '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, '
             'like Gecko) Chrome/36.0.1985.143 Safari/537.36"'])
