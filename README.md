# Snowflake Regex Data Generator

## Overview
The Snowflake Regex Generator is a Python program designed to generate useful data based on regular expressions and insert the generated date directly into the Snowflake database table. It is ideal for testing, development purposes, or any scenario where mock data is required in a Snowflake environment.

## Features 
1. Generates data based on regular expressions.
2. Inserts data in batches to Snowflake database tables
3. Customizable for different data patterns and table schemas

## Requirements 
1. Python 3.x
2. Snowflake account with necessary permissions
3. Python libraries: **`snowflake-connector-python`, `exrex`,`argparse`, `getpass`**

## Installation
**pip3 install snowflake-connector-python exrex argparse**

## Configuration
### Configure the script with your Snowflake credentials and target table details. The following parameters are required:
1. Snowflake username, password, account name, warehouse, database, and schema.
2. Target table name in snowflake.
3. Regular expressions for data generation.
4. Column names corresponding to each regular expression.

## Usage
### Run the script from the command line with the necessary arguments:
python3 regex_project.py -u <user> -p <password> -a <account> -w <warehouse> -d <database> -s <schema> -t <table_name> -c <count> -r <regex> -n <column_name>
### Arguments
**-u, --user**: Snowflake username.
**-p, --password**: Snowflake password.
**-a, --account**: Snowflake account.
**-w, --warehouse**: Snowflake warehouse.
**-d, --database**: Snowflake database.
**-s, --schema**: Snowflake schema.
**-t, --table_name**: Name of the Snowflake table.
**-c, --count**: Number of data rows to generate for each regex pattern.
**-r, --regex**: Regular expression pattern for data generation (multiple allowed).
**-n, --column_name**: Column name in the Snowflake table for the corresponding regex(multiple allowed) 
