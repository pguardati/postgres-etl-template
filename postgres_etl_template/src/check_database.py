import pandas as pd
import psycopg2

from postgres_etl_template.src.sql_queries import tables


def get_top_elements_from_table(cur, table):
    """Get first 5 elements from a table of a database
    Args:
        cur(cursor): cursor of psycopg2
        table(str): name of the table to fetch

    Returns:
        pd.DataFrame
    """
    # get query result
    cur.execute("SELECT * FROM {} LIMIT 5;".format(table))
    res = cur.fetchall()
    # build dataframe using query records
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(res, columns=[columns])
    return df


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
