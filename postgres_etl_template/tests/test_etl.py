import unittest
import os
import psycopg2
import pandas as pd

from postgres_etl_template.constants import DIR_DATA_TEST
from postgres_etl_template.src.sql_queries import songplays_check
from postgres_etl_template.scripts import etl


class TestETL(unittest.TestCase):
    def test_songplays_table(self):
        """Test that after the etl pipeline,
        there is only 1 element remaining in the songplays table, with not null artistid
        """
        # run etl pipeline
        etl.main([
            os.path.join(DIR_DATA_TEST, "song_data"),
            os.path.join(DIR_DATA_TEST, "log_data"),
            "--reset-tables"
        ])

        # check that there is only one element
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres")
        cur = conn.cursor()

        # get elements with not null artist id
        cur.execute(songplays_check)
        res = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(res, columns=[columns])

        conn.close()
        self.assertEqual(df.loc[:, ["artist_id"]].values[0], "AR5KOSW1187FB35FF4")


if __name__ == "__main__":
    unittest.main()
