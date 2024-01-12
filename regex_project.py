import exrex
import argparse
import snowflake.connector

class RegexGenerator:
    def __init__(self, regex_count_name, snowflake_config):
        self.regex_count_name = regex_count_name
        self.snowflake_config = snowflake_config
    

    def generate_data(self):

        #data ={} #keys: column name, value: generated value for that column

        connection = snowflake.connector.connect(
            user = self.snowflake_config['user'],
            password = self.snowflake_config['password'],
            account = self.snowflake_config['account'],
            warehouse = self.snowflake_config['warehouse'],
            database = self.snowflake_config['database'],
            schema = self.snowflake_config['schema']
        )
        
        cursor = connection.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.snowflake_config['table_name']} (id INTEGER AUTOINCREMENT PRIMARY KEY)")
        connection.commit()

        for triplet in self.regex_count_name:
            regex, count, *rest = triplet.split(':')
            col_name = rest[0] if rest else None  # name is optional

            # Alter the table to add a new column
            cursor.execute(f"ALTER TABLE {self.snowflake_config['table_name']} ADD {col_name} STRING")
            connection.commit()

            for i in range(int(count)):
                generated_data = exrex.getone(regex)

                # Check if the row with the specified id exists
                cursor.execute(f"SELECT 1 FROM {self.snowflake_config['table_name']} WHERE id = %s", (i + 1,)) #return the constant 1 for every row of the table. i + 1 because range starts from 0
                                                                                                               #determine if current row exist
                existing_row = cursor.fetchone() #returns a single record or None if no more rows are available

                if existing_row:
                    # If the row exists, update the existing row
                    cursor.execute(f"UPDATE {self.snowflake_config['table_name']} SET {col_name} = %s WHERE id = %s", (generated_data, i + 1))
                else:
                    # If the row doesn't exist, insert a new row
                    cursor.execute(f"INSERT INTO {self.snowflake_config['table_name']} (id, {col_name}) VALUES (%s, %s)", (i + 1, generated_data))

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
    args = parser.parse_args() #parse these arguments into an object. Ex: Now args.user outputs my_user

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
