import unittest
import getpass
from itertools import zip_longest
from regex_project import SnowflakeOperations, SnowflakeConnector

#This test generates a table in Snowflake. Every time the test runs, "test_table" is deleted if it already exists and then is regenerated. 
#Make sure to delete "test_table" afterwards.

class TestSnowflakeOperations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        #Called once before all tests
        user = input("Enter your Snowflake username: ")
        password = getpass.getpass("Enter your Snowflake password (hidden): ")
        cls.connector = SnowflakeConnector({
            'user': user,
            'password': password,
            'account': 'immuta.us-east-1',
            'warehouse': 'dev_wh',
            'database': 'sprintern',
            'schema': 'example'
        })
        cls.connector.connect()

    @classmethod
    def tearDownClass(cls):
        cls.connector.close()

    def setUp(self):
        self.table_name = "test_table"
        self.column_names = ["column1", "column2"]
        self.snowflake_ops = SnowflakeOperations(self.connector, self.table_name, self.column_names)

        #Delete the table if it already exists
        cursor = self.connector.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        cursor.close()

    def test_create_table(self):
        self.snowflake_ops.create_table()

        #Assertions
        cursor = self.connector.connection.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{self.table_name}'")
        result = cursor.fetchone()
        self.assertIsNotNone(result, "Table was not created")

        cursor.close()

    def test_insert_data_in_batches(self):
        self.snowflake_ops.create_table()  
        data = [["value1_row1", "value2_row1"], ["value1_row2", "value2_row2"]]
        self.snowflake_ops.insert_data_in_batches(data, 100)

        #Assertions
        cursor = self.connector.connection.cursor()
        cursor.execute(f"SELECT * FROM {self.table_name}")
        fetched_data = cursor.fetchall()
        self.assertEqual(len(fetched_data), len(data), "Incorrect number of rows inserted")

        cursor.close()

if __name__ == '__main__':
    unittest.main()

