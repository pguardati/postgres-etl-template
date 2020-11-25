import pandas as pd
import psycopg2

from postgres_etl_template.src.sql_queries import tables
from postgres_etl_template.src.utils_misc import get_top_elements_from_table


def check_database_content():
    """Check top 5 elements for each table in the database"""
    # open connection
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    # print all expected tables
    for table in tables:
        df = get_top_elements_from_table(cur, table)
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print("\n--Table : {}\n".format(table), df)

    # close connection
    conn.close()


if __name__ == "__main__":
    check_database_content()
