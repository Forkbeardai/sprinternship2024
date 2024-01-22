import unittest
import getpass
from unittest.mock import patch
from regex_project import CommandLineParser

class TestCommandLineParser(unittest.TestCase):
    
    def test_parse_args(self):
        #Prepare test arguments
        test_args = ['-u', 'test_user', '-p', 'test_password', '-a', 'test_account', '-w', 'test_warehouse', '-d', 'test_database',
                     '-s', 'test_schema', '-t', 'test_table', '-c', '10', '-r', 'regex1', '-n', 'column1',
                     '-i', 'invalid_regex1', '-pct', '20']
        #Run the parser
        parser = CommandLineParser()
        parsed_args = parser.parser.parse_args(test_args)
        #Assertions 
        self.assertEqual(parsed_args.user, 'test_user')
        self.assertEqual(parsed_args.password, 'test_password')
        self.assertEqual(parsed_args.account, 'test_account')
        self.assertEqual(parsed_args.warehouse, 'test_warehouse')
        self.assertEqual(parsed_args.database, 'test_database')
        self.assertEqual(parsed_args.schema, 'test_schema')
        self.assertEqual(parsed_args.table_name, 'test_table')
        self.assertEqual(parsed_args.count, '10')
        self.assertEqual(parsed_args.regex, ['regex1'])
        self.assertEqual(parsed_args.column_name, ['column1'])
        self.assertEqual(parsed_args.invalid_regexes, ['invalid_regex1'])
        self.assertEqual(parsed_args.invalid_data_percentages, ['20'])

if __name__ == '__main__':
    unittest.main()