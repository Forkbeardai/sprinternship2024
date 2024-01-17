import exrex
import argparse
import snowflake.connector
import getpass

class SnowflakeConnector:
    def __init__(self, snowflake_config):
        self.snowflake_config = snowflake_config
        self.connection = None

    def connect(self):
        self.connection = snowflake.connector.connect(
            user=self.snowflake_config['user'],
            password=self.snowflake_config['password'],
            account=self.snowflake_config['account'],
            warehouse=self.snowflake_config['warehouse'],
            database=self.snowflake_config['database'],
            schema=self.snowflake_config['schema']
        )

    def close(self):
        if self.connection:
            self.connection.close()

class DataGenerator:
    def __init__(self, regexes, count):
        self.regexes = regexes
        self.count = count

    def generate_data(self):
        data = []
        for regex in self.regexes:
            generated_data = [exrex.getone(regex) for _ in range(int(self.count))]
            data.append(generated_data)

        
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

    def insert_data(self, data):
        connection = self.snowflake_connector.connection
        cursor = connection.cursor()
        for i, row_data in enumerate(zip(*data)):
            columns = ', '.join(self.column_names)
            placeholders = ', '.join(['%s' for _ in self.column_names])
            cursor.execute(f"INSERT INTO {self.table_name} (id, {columns}) VALUES (%s, {placeholders})", (i + 1, *row_data))
        connection.commit()
        cursor.close()


class CommandLineParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description='Generate data based on a given regular expression.')
        self.parser.add_argument('-u', '--user', type=str, required=True, help ='Snowflake username')
        self.parser.add_argument('-p', '--password', type=str, default='default_password', help='Snowflake password (optional)')
        self.parser.add_argument('-a', '--account', type=str, required=True, help='Snowflake account')
        self.parser.add_argument('-w', '--warehouse', type=str, required=True, help='Snowflake warehouse')
        self.parser.add_argument('-d', '--database', type=str, required=True, help='Snowflake database')
        self.parser.add_argument('-s', '--schema', type=str, required=True, help='Snowflake schema')
        self.parser.add_argument('-t', '--table_name', type=str, required=True, help='Snowflake table name')
        self.parser.add_argument('-c', '--count', type=str, help='Amount of data to be generated for each regex')
        self.parser.add_argument('-r', '--regex', type=str, action='append', help='Regex')
        self.parser.add_argument('-n', '--column_name', type=str, action='append', help='Column name for corresponding regex')
        
    def parse_args(self):
        args = self.parser.parse_args()

        # Check if the provided password matches the default value
        if args.password == 'default_password':
            # Use a secure method to get the user's password
            args.password = getpass.getpass('Enter your Snowflake password(hidden): ')

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

        data_generator = DataGenerator(args.regex, args.count)
        generated_data = data_generator.generate_data()

        snowflake_operations = SnowflakeOperations(snowflake_connector, args.table_name, args.column_name)
        snowflake_operations.create_table()
        snowflake_operations.insert_data(generated_data)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        snowflake_connector.close()

if __name__ == "__main__":
    main()
















