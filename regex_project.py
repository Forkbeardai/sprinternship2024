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

class RegexGenerator:
    def __init__(self, regex_count_name, snowflake_connector):
        self.regex_count_name = regex_count_name
        self.snowflake_connector = snowflake_connector

    def generate_data(self):
        connection = self.snowflake_connector.connection
        snowflake_config = self.snowflake_connector.snowflake_config

        cursor = connection.cursor()
        
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {snowflake_config['table_name']} (id INTEGER AUTOINCREMENT PRIMARY KEY)")
        connection.commit()

        for triplet in self.regex_count_name:
            regex, count, *rest = triplet.split(':')
            col_name = rest[0] if rest else None

            # Alter the table to add a new column
            cursor.execute(f"ALTER TABLE {snowflake_config['table_name']} ADD {col_name} STRING")
            connection.commit()

            for i in range(int(count)):
                generated_data = exrex.getone(regex)

                # Check if the row with the specified id exists
                cursor.execute(f"SELECT 1 FROM {snowflake_config['table_name']} WHERE id = %s", (i + 1,))
                existing_row = cursor.fetchone()

                if existing_row:
                    # If the row exists, update the existing row
                    cursor.execute(f"UPDATE {snowflake_config['table_name']} SET {col_name} = %s WHERE id = %s", (generated_data, i + 1))
                else:
                    # If the row doesn't exist, insert a new row
                    cursor.execute(f"INSERT INTO {snowflake_config['table_name']} (id, {col_name}) VALUES (%s, %s)", (i + 1, generated_data))

                connection.commit()

        cursor.close()

class CommandLineParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description='Generate data based on a given regular expression.')
        self.parser.add_argument('--user', type=str, help='Snowflake username (required)')
        self.parser.add_argument('--password', type=str, default='default_password', help='Snowflake password (optional)') #password is optional, if not entered, later wll be asked by the scanner
        self.parser.add_argument('--account', type=str, help='Snowflake account (required)')
        self.parser.add_argument('--warehouse', type=str, help='Snowflake warehouse (required)')
        self.parser.add_argument('--database', type=str, help='Snowflake database (required)')
        self.parser.add_argument('--schema', type=str, help='Snowflake schema (required)')
        self.parser.add_argument('--table_name', type=str, help='Snowflake table name (required)')
        self.parser.add_argument('--regex_count_name', nargs='+', type=str,
                                 help='List of regex:count:name, separated by space and each surrounded by quotes')

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

    snowflake_connector = SnowflakeConnector(snowflake_config)
    snowflake_connector.connect()

    regex_count_name = args.regex_count_name

    generator = RegexGenerator(regex_count_name, snowflake_connector)
    generator.generate_data()

    snowflake_connector.close()

main()
