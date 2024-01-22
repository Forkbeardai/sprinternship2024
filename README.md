# Snowflake Regex Data Generator

## Overview
The Snowflake Regex Generator is a robust Python tool for generating and inserting mock data into Snowflake databases based on predefined regular expressions. It supports custom data patterns, batch insertions, and handles both valid and invalid data generation for comprehensive testing and development.

## Features 
1. Regex-based data creation for precise pattern matching.
2. Batch data insertion for efficiency.
3. Generates a mix of valid and invalid data for thorough testing.
4. Flexible schema support for various table designs.

## Requirements 
1. Python 3.x
2. Snowflake account with necessary permissions
3. Python libraries: **`snowflake-connector-python`, `exrex`,`argparse`, `getpass`**

## Installation
`pip3 install snowflake-connector-python exrex argparse getpass`

## Configuration
### Configure the script with your Snowflake credentials and target table details. The following parameters are required:
1. Snowflake username, password, account name, warehouse, database, and schema.
2. Target table name in snowflake.
3. Regular expressions for data generation.
4. Column names corresponding to each regular expression.

## Usage
### Run the script from the command line with the necessary arguments:
`python3 regex_project.py -u <user> -p <password> -a <account> -w <warehouse> -d <database> -s <schema> -t <table_name> -c <count> -r <regex> -n <column_name>`
### Extended Usage 
To incorporate invalid data generation, use the additional `-i <invalid_regexes>` for invalid regex patterns and `-pct <invalid_data_percentages>` for specifying the percentage of invalid data.

### Arguments
1. **-u, --user**: Snowflake username.
2. **-p, --password**: Snowflake password. (If you don't enter a password argument, you will be prompted to enter it hidden)
3. **-a, --account**: Snowflake account.
4. **-w, --warehouse**: Snowflake warehouse.
5. **-d, --database**: Snowflake database.
6. **-s, --schema**: Snowflake schema.
7. **-t, --table_name**: Name of the Snowflake table.
8. **-c, --count**: Number of data rows to generate for each regex pattern.
9. **-r, --regex**: Regular expression pattern for data generation (multiple allowed).
10. **-n, --column_name**: Column name in the Snowflake table for the corresponding regex(multiple allowed) 
11. **-i, --invalid_regexes**: Invalid regex for corresponding regex(multiple allowed) 
12. **-pct, --invalid_data_percentages**: Invalid data percentage for corresponding regex(multiple allowed) 

### Example
`python3 regex_project.py -u username -p password -a immuta.us-east-1 -w dev_wh -d sprintern -s example -t my_table -c 100 -r "[A-Z]{5}" -n letters -r "/d{3}" -n digits`

### Example with invalid data
`python3 regex_project.py -u username -p password -a immuta.us-east-1 -w dev_wh -d sprintern -s example -t new_table -c 1000 -r "/d{3}[ ]?/d{3}[ ]?/d{4}" -n phone_number -i "[a-zA-Z]{10}" -pct 20`

## Contact
<br> Jamie Zheng github: jamielinzheng email: Igotanicejam@gmail.com </br>
<br> Aditri Gadigi github: aditrigadigi email: adi3.gadigi@gmail.com </br>
<br> Emily Qiyu An github: emilyqiyuan email: emilyqiyuan@gmail.com </br>
<br> Yahleel Raya github: YahleelRaya email: yahleelxo100@gmail.com </br>

