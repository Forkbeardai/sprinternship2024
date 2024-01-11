import exrex
import argparse
import snowflake.connector

class RegexGenerator:
    def __init__(self, regex_count_name):
        self.regex_count_name = regex_count_name

    def generate_data(self):
        for triplet in self.regex_count_name:
            regex, count, *rest = triplet.split(':')
            name = rest[0] if rest else None
            for i in range(int(count)):
                generated_data = exrex.getone(regex)
                print(generated_data)
    
##class SnowflakeConnector:


class CommandLineParser:
    # delete defaults later
    def __init__(self):
        self.parser = argparse.ArgumentParser(description = 'Generate data based on a given regular expression.')
        self.parser.add_argument('--user', type = str, default = 'default', help = 'Snowflake username (required)')
        self.parser.add_argument('--password', type = str, default = 'default', help = 'Snowflake password (required)')
        self.parser.add_argument('--account', type = str, default = 'default', help = 'Snowflake account (required)')
        self.parser.add_argument('--warehouse', type = str, default = 'default', help = 'Snowflake warehouse (required)')
        self.parser.add_argument('--database', type = str, default = 'default', help = 'Snowflake database (required)')
        self.parser.add_argument('--schema', type = str, default = 'default', help = 'Snowflake schema (required)')
        self.parser.add_argument('--table_name', type = str, default = 'Sample Regex Table', help = 'Snowflake schema (not required)')
        self.parser.add_argument('--regex_count_name', nargs = '+', type = str, help = 'List of regex:count:name, separated by space and each surrounded by quotes')
    
    def parse_args(self):
        return self.parser.parse_args()


def main():
    parser = CommandLineParser()
    args = parser.parse_args()

    regex_count_name = args.regex_count_name

    generator = RegexGenerator(regex_count_name)
    generator.generate_data()

main()