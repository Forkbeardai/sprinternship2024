import unittest
from regex_project import DataGenerator
import re

class TestDataGenerator(unittest.TestCase):
        # Perform tests, like checking the length of generated data   
        
        def setUp(self):
        # Common setup for all tests can go here
            self.regexes = ['\d{3}', '[a-zA-Z]+']
            self.invalid_regexes = ['\d{4}', '[a-zA-Z]{5}']
            self.invalid_data_percentages = [10, 20]
            self.count = 100
            self.data_gen = DataGenerator(self.regexes, self.count, self.invalid_regexes, self.invalid_data_percentages)

        def test_generate_data_valid_count(self):
            data = self.data_gen.generate_data()
            valid_count = int(self.count * (1 - sum(self.invalid_data_percentages) / 200))
            for datum in data:
                self.assertEqual(len(datum), self.count)


if __name__ == '__main__':
    unittest.main()
