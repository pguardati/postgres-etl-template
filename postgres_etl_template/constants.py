import os

PROJECT_NAME = 'postgres_etl_template'
REPOSITORY_PATH = os.path.realpath(__file__)[:os.path.realpath(__file__).find(PROJECT_NAME)]
PROJECT_PATH = os.path.join(REPOSITORY_PATH, PROJECT_NAME)
DIR_DATA = os.path.join(PROJECT_PATH, 'data')
DIR_DATA_TEST = os.path.join(PROJECT_PATH, 'tests', 'test_data')

print("project_path:{}".format(PROJECT_PATH))
print("data_path:{}".format(DIR_DATA))
