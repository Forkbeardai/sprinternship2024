import exrex
import argparse
import snowflake.connector
import getpass
from itertools import zip_longest
import random


#Class for managing Snowflake connection
class SnowflakeConnector:
    def __init__(self, snowflake_config):
        self.snowflake_config = snowflake_config
        self.connection = None

    def connect(self):
        #Establish a connection to Snowflake using credentials
        self.connection = snowflake.connector.connect(
            user  = self.snowflake_config['user'],
            password = self.snowflake_config['password'],
            account = self.snowflake_config['account'],
            warehouse = self.snowflake_config['warehouse'],
            database = self.snowflake_config['database'],
            schema = self.snowflake_config['schema']
        )

    def close(self):
        #Close  Snowflake connection
        if self.connection:
            self.connection.close()




#Class for generating data based on regex patterns
class DataGenerator:
    def __init__(self, regexes, count, invalid_regexes, invalid_data_percentages):
        self.regexes = regexes
        self.count = count
        self.invalid_regexes = invalid_regexes
        self.invalid_data_percentages = invalid_data_percentages

    def generate_data(self):
        data = []  

        for regex, invalid_regex, invalid_percentage in zip(self.regexes, self.invalid_regexes, self.invalid_data_percentages):
            valid_count = int(int(self.count) * (1 - float(invalid_percentage) / 100))
            invalid_count = int(int(self.count) * float(invalid_percentage) / 100) #How many invalid data we should generate

            print(f"num of valid count is {valid_count}, num of invalid count is {invalid_count}")
            #Generate valid data
            valid_data = [exrex.getone(regex) for _ in range(valid_count)]

            if invalid_count > 0:
                #Generate invalid data if requested
                invalid_data = [exrex.getone(invalid_regex) for _ in range(invalid_count)]
                valid_data.extend(invalid_data)

            #Shuffle the data
            random.shuffle(valid_data)

            data.append(valid_data)
        return data




#Class for Snowflake database operations
class SnowflakeOperations:
    def __init__(self, snowflake_connector, table_name, column_names):
        self.snowflake_connector = snowflake_connector
        self.table_name = table_name
        self.column_names = column_names

    def create_table(self):
        #Create a table in Snowflake with specified columns
        connection = self.snowflake_connector.connection
        cursor = connection.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} (id INTEGER AUTOINCREMENT PRIMARY KEY)")
        for col_name in self.column_names:
            cursor.execute(f"ALTER TABLE {self.table_name} ADD {col_name} STRING")
        connection.commit()
        cursor.close()

    def insert_data_in_batches(self, data, batch_size=100):
        connection = self.snowflake_connector.connection
        cursor = connection.cursor()

        #Transpose data to get rows
        rows = list(zip_longest(*data, fillvalue=None)) #create a list of tuple, [(col1_data1, col2_data1), (cold1_data2, col2_data2)...]
        total_rows = len(rows)
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = rows[start:end]

            #Preparing the SQL statement for batch insertion
            columns = ', '.join(self.column_names)  #Columns are joined into a comma-separated string. ex:assuming column_names = ['column1', 'column2', 'column3'], column = 'column1, column2, column3'
            placeholders = ', '.join(['%s' for _ in self.column_names])
            values_placeholder = ', '.join([f"({placeholders})" for _ in batch])
            sql = f"INSERT INTO {self.table_name} ({columns}) VALUES {values_placeholder}"
            flattened_values = [item for sublist in batch for item in sublist]

            #Execute SQL statement
            cursor.execute(sql, flattened_values)

        connection.commit()
        cursor.close()

class CommandLineParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description = 'Generate data based on a given regular expression.')
        self.parser.add_argument('-u', '--user', type = str, required = True, help = 'Snowflake username')
        self.parser.add_argument('-p', '--password', type = str, default = 'default', help = 'Snowflake password (optional)')
        self.parser.add_argument('-a', '--account', type = str, required = True, help = 'Snowflake account')
        self.parser.add_argument('-w', '--warehouse', type = str, required = True, help = 'Snowflake warehouse')
        self.parser.add_argument('-d', '--database', type = str, required = True, help = 'Snowflake database')
        self.parser.add_argument('-s', '--schema', type = str, required = True, help = 'Snowflake schema')
        self.parser.add_argument('-t', '--table_name', type = str, required = True, help = 'Snowflake table name')
        self.parser.add_argument('-c', '--count', type = str, help = 'Amount of data to be generated for each regex (must be positive ot 0)')
        self.parser.add_argument('-r', '--regex', type = str, action = 'append', help = 'Regex')
        self.parser.add_argument('-n', '--column_name', type = str, action = 'append', help = 'Column name for corresponding regex')
        self.parser.add_argument('-i', '--invalid_regexes', type = str, action = 'append', help = 'Invalid regex for corresponding regex')
        self.parser.add_argument('-pct', '--invalid_data_percentages', type = str, action = 'append', help = 'Invalid data percentage for corresponding regex (must be between 0 and 100).')


    def parse_args(self):
        args = self.parser.parse_args()
        if args.password == 'default':
            args.password = getpass.getpass('Enter your Snowflake password (hidden): ')
        if int(args.count) < 0:
            raise Exception("Count cannot be negative.")
        for element in args.invalid_data_percentages:
            if int(element) < 0 or int(element) > 100:
                raise Exception("Percentage must be between 0 and 100.")
        return args


def main():
    parser = CommandLineParser()
    args = parser.parse_args() #Parse the command user inputted, now can access the specific data by caling args.xxx

    snowflake_config = {
        'user': args.user,
        'password': args.password,
        'account': args.account,
        'warehouse': args.warehouse,
        'database': args.database,
        'schema': args.schema,
        'table_name': args.table_name
    }

    try:
        snowflake_connector = SnowflakeConnector(snowflake_config)
        snowflake_connector.connect() #Connect to snowflake

        data_generator = DataGenerator(args.regex, args.count, args.invalid_regexes, args.invalid_data_percentages)
        generated_data = data_generator.generate_data()

        snowflake_operations = SnowflakeOperations(snowflake_connector, args.table_name, args.column_name)
        snowflake_operations.create_table()

        #Using batch insert
        snowflake_operations.insert_data_in_batches(generated_data, batch_size=100)  #You can adjust the batch size

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        snowflake_connector.close()

if __name__ == "__main__":
    main()
