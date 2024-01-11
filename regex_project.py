import exrex
import argparse
import snowflake.connector

class RegexGenerator:
    def __init__(self, regex_count):
        self.regex_count = regex_count

    def generate_data(self):
        for regex, count in self.regex_count_pairs:
            print(f"Generating {count} data items for regex: {regex}")
            for i in range(count):
                generated_data = exrex.getone(regex)
                print(generated_data)

class CommandLineParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description = 'Generate data based on a given regular expression.')
        self.parser.add_argument('regex_count', nargs = '+', type = lambda s: s.split(':'), help = 'List of regex:count pairs, each separated by space')
        self.parser.add_argument('--snowflake-user', type = str, default = 'default', help = 'Snowflake username (not required)')
        self.parser.add_argument('--snowflake-password', type = str, default = 'default', help = 'Snowflake password (not required)')
        self.parser.add_argument('--snowflake-account', type = str, default = 'default', help = 'Snowflake account (not required)')
        self.parser.add_argument('--snowflake-warehouse', type = str, default = 'default', help = 'Snowflake warehouse (not required)')
        self.parser.add_argument('--snowflake-database', type = str, default = 'default', help = 'Snowflake database (not required)')
        self.parser.add_argument('--snowflake-schema', type = str, default = 'default', help = 'Snowflake schema (not required)')
    
    def parse_args(self):
        return self.parser.parse_args()

def main():
    parser = CommandLineParser()
    args = parser.parse_args()

    regex_count_pairs = [(regex, int(count)) for regex, count in args.regex_count_pairs]

    generator = RegexGenerator(regex_count_pairs)
    generator.generate_data()

main()