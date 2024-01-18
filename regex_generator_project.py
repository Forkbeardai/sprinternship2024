import exrex
import argparse
import snowflake.connector
import getpass
import random

class SnowflakeConnector:
    def __init__(self, snowflake_config):
        self.snowflake_config = snowflake_config
        self.connection = None

    def connect(self):
        self.connection = snowflake.connector.connect(
            user = self.snowflake_config['user'],
            password = self.snowflake_config['password'],
            account = self.snowflake_config['account'],
            warehouse = self.snowflake_config['warehouse'],
            database = self.snowflake_config['database'],
            schema = self.snowflake_config['schema']
        )

    def close(self):
        if self.connection:
            self.connection.close()

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
            invalid_count = int(int(self.count) * float(invalid_percentage) / 100)

            # Generate valid data
            valid_data = [exrex.getone(regex) for _ in range(valid_count)]

            if invalid_count > 0:
                # Generate invalid data if requested
                invalid_data = [exrex.getone(invalid_regex) for _ in range(invalid_count)]
                valid_data.extend(invalid_data)

              # Shuffle the data
            random.shuffle(valid_data)

            data.append(valid_data)

        return data

class SnowflakeOperations:
    def __init__(self, snowflake_connector, table_name, column_names):
        self.snowflake_connector = snowflake_connector
        self.table_name = table_name
        self.column_names = column_names

    def create_table(self):
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

        #Transpose data to get the rows
        rows = list(zip(*data))

        total_rows = len(rows)
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch = rows[start:end]

            #Write SQL Statement
            columns = ', '.join(self.column_names)
            #Generate placeholders
            placeholders_list = []

            for _ in self.column_names:
                placeholders_list.append('%s')
            placeholders = ', '.join(placeholders_list)
            values_placeholder_list = []

            for _ in batch:
                row_placeholder = f"({placeholders})"
                values_placeholder_list.append(row_placeholder)
            values_placeholder = ', '.join(values_placeholder_list)

            sql = f"INSERT INTO {self.table_name} ({columns}) VALUES {values_placeholder}"
            flattened_values = [item for sublist in batch for item in sublist]
            
            #Execute SQL Statement
            cursor.execute(sql, flattened_values)

        connection.commit()
        cursor.close()

class CommandLineParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description = 'Generate data based on a given regular expression.')
        self.parser.add_argument('-u', '--user', type = str, required = True, help = 'Snowflake username')
        self.parser.add_argument('-p', '--password', type = str, default = 'default', help = 'Snowflake password (will be prompted to enter password (hidden) if not entered as an argument)')
        self.parser.add_argument('-a', '--account', type = str, required = True, help = 'Snowflake account')
        self.parser.add_argument('-w', '--warehouse', type = str, required = True, help = 'Snowflake warehouse')
        self.parser.add_argument('-d', '--database', type = str, required = True, help = 'Snowflake database')
        self.parser.add_argument('-s', '--schema', type = str, required = True, help = 'Snowflake schema')
        self.parser.add_argument('-t', '--table_name', type = str, required = True, help = 'Snowflake table name')
        self.parser.add_argument('-c', '--count', type = str, help = 'Amount of data to be generated for each regex')
        self.parser.add_argument('-r', '--regex', type = str, action = 'append', help = 'Regex')
        self.parser.add_argument('-n', '--column_name', type = str, action = 'append', help = 'Column name for corresponding regex')
        self.parser.add_argument('-n', '--column_name', type=str, action='append', help='Column name for corresponding regex')
        self.parser.add_argument('-i', '--invalid_regexes', type=str, action='append', help='Invalid regex for corresponding regex')
        self.parser.add_argument('-pct', '--invalid_data_percentages', type=str, action='append', help='Invalid data percentage for corresponding regex')


    def parse_args(self):
        args = self.parser.parse_args()
        if args.password == 'default':
            args.password = getpass.getpass('Enter your Snowflake password (hidden): ')
        return args


def main():
    parser = CommandLineParser()
    args = parser.parse_args()

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
        snowflake_connector.connect()

        data_generator = DataGenerator(args.regex, args.count, args.invalid_regexes, args.invalid_data_percentages)
        generated_data = data_generator.generate_data()

        snowflake_operations = SnowflakeOperations(snowflake_connector, args.table_name, args.column_name)
        snowflake_operations.create_table()

        # Using batch insert
        snowflake_operations.insert_data_in_batches(generated_data, batch_size=100)  # You can adjust the batch size

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        snowflake_connector.close()

if __name__ == "__main__":
    main()