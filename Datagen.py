import exrex
import argparse
import snowflake.connector
#hajfghaghas
class RegexGenerator:
    def __init__(self, regex, count=1):
        self.regex = regex
        self.count = count

    def generate_data(self):
        for i in range(self.count):
            generated_data = exrex.getone(self.regex)
            print(generated_data)

class CommandLineParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description = 'Generate data based on a given regular expression.')
        self.parser.add_argument('regex', type = str, help = 'RegEx')
        self.parser.add_argument('--count', type = int, default = 1, help = 'Number of data items to generate (default is 1)')
        self.parser.add_argument('--snowflake-user', type = str, default = 'default', help = 'Snowflake username (not required)')
        self.parser.add_argument('--snowflake-password', type = str, default = 'default', help = 'Snowflake password (not required)')
        self.parser.add_argument('--snowflake-account', type = str, default = 'default', help = 'Snowflake account (not required)')
        self.parser.add_argument('--snowflake-warehouse', type = str, default = 'default', help = 'Snowflake warehouse (not required)')
        self.parser.add_argument('--snowflake-database', type = str, default = 'default', help = 'Snowflake database (not required)')
        self.parser.add_argument('--snowflake-schema', type = str, default = 'default', help = 'Snowflake schema (not required)')
    
    def parse_args(self):
        return self.parser.parse_args()

##class SnowflakeConnector:

def main():
    parser = CommandLineParser()
    args = parser.parse_args()

    generator = RegexGenerator(args.regex, args.count)
    generator.generate_data()

main()