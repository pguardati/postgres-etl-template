import unittest

from sparkify_postgres_db.src.etl import parse_input


class TestInterface(unittest.TestCase):
    """Check possible configurations of the etl inputs"""

    def test_configuration_to_insert_from_scratch(self):
        args = parse_input(["data/song_data", "data/log_data", "--reset-tables"])
        self.assertEqual(args.path_data_songs, "data/song_data")
        self.assertEqual(args.path_data_logs, "data/log_data")
        self.assertTrue(args.reset_tables)

    def test_configuration_to_update_current_tables(self):
        args = parse_input(["data/song_data", "data/log_data"])
        self.assertEqual(args.path_data_songs, "data/song_data")
        self.assertEqual(args.path_data_logs, "data/log_data")
        self.assertFalse(args.reset_tables)
