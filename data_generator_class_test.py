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

        def test_generate_data_validity(self):
            data = self.data_gen.generate_data()
            valid_data_regex = re.compile(self.regexes[0])
            for datum in data[0][:int(self.count * (1 - self.invalid_data_percentages[0] / 100))]:
                self.assertTrue(valid_data_regex.fullmatch(datum))

        def test_invalid_percentage_handling(self):
            regexes = ['\d{3}']
            
            # Test with a percentage greater than 100
            invalid_regexes_over = ['\D+']
            invalid_data_percentages_over = [110]
            with self.assertRaises(ValueError):
                DataGenerator(regexes, 10, invalid_regexes_over, invalid_data_percentages_over).generate_data()

            # Test with a percentage less than 0
            invalid_regexes_under = ['\D+']
            invalid_data_percentages_under = [-10]
            with self.assertRaises(ValueError):
                DataGenerator(regexes, 10, invalid_regexes_under, invalid_data_percentages_under).generate_data()


if __name__ == '__main__':
    unittest.main()
