import exrex
import argparse
import snowflake.connector

class RegexGenerator:
    def __init__(self, regex_count_name, snowflake_config):
        self.regex_count_name = regex_count_name
        self.snowflake_config = snowflake_config

    def generate_data(self):
        connection = snowflake.connector.connect(
            user = self.snowflake_config['user'],
            password = self.snowflake_config['password'],
            account = self.snowflake_config['account'],
            warehouse = self.snowflake_config['warehouse'],
            database = self.snowflake_config['database'],
            schema = self.snowflake_config['schema']
        )

        cursor = connection.cursor()

        cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.snowflake_config['table_name']} (id INTEGER AUTOINCREMENT PRIMARY KEY, name STRING, data STRING)")
        connection.commit()

        for triplet in self.regex_count_name:
            regex, count, *rest = triplet.split(':')
            name = rest[0] if rest else None #name is optional
            for i in range(int(count)):
                generated_data = exrex.getone(regex)
                cursor.execute(f"INSERT INTO {self.snowflake_config['table_name']} (name, data) VALUES (%s, %s)", (name, generated_data))
                connection.commit()

        cursor.close()
        connection.close()

class CommandLineParser:
    # delete defaults later
    def __init__(self):
        self.parser = argparse.ArgumentParser(description = 'Generate data based on a given regular expression.')
        self.parser.add_argument('--user', type = str, help = 'Snowflake username (required)')
        self.parser.add_argument('--password', type = str, help = 'Snowflake password (required)')
        self.parser.add_argument('--account', type = str, help = 'Snowflake account (required)')
        self.parser.add_argument('--warehouse', type = str, help = 'Snowflake warehouse (required)')
        self.parser.add_argument('--database', type = str, help = 'Snowflake database (required)')
        self.parser.add_argument('--schema', type = str, help = 'Snowflake schema (required)')
        self.parser.add_argument('--table_name', type = str, help = 'Snowflake table name (required)')
        self.parser.add_argument('--regex_count_name', nargs = '+', type = str, help = 'List of regex:count:name, separated by space and each surrounded by quotes')
    
    def parse_args(self):
        return self.parser.parse_args()

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

    regex_count_name = args.regex_count_name

    generator = RegexGenerator(regex_count_name, snowflake_config)
    generator.generate_data()

main()